import gleam/deque
import gleam/dynamic
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/option
import gleam/order
import gleam/otp/actor
import gleam/otp/factory_supervisor as factory
import gleam/otp/static_supervisor as static
import gleam/otp/supervision
import gleam/pair
import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import hive/event.{
  type EventMessage, WorkerAssigned, WorkerCreated, WorkerStopped,
  WorkerUnassigned,
}
import hive/queue

type WithTimer(a) {
  WithTimer(data: a, timer: option.Option(process.Timer))
}

type QueueMessage(message) {
  QueueMessage(
    message: message,
    timeout_after: option.Option(timestamp.Timestamp),
    timer: option.Option(process.Timer),
  )
}

type DispatchMessage(message) {
  NewMessage(message: message)
  FromQueue(message: QueueMessage(message))
}

//-----------------------------------------------------------------------------------------------//
//                                          Dispatcher                                           //
//-----------------------------------------------------------------------------------------------//

type Worker(message) {
  Worker(
    timeout_timer: option.Option(process.Timer),
    age: timestamp.Timestamp,
    subject: process.Subject(WorkerInternalMessage(message)),
  )
}

/// (internal) The state of the dispatcher. it keeps track of the free workers and total of workers 
/// in the pool.
type DispatcherState(message) {
  DispatcherState(
    subject: process.Subject(DispatcherMessage(message)),
    free_workers: deque.Deque(Worker(message)),
    busy_workers: List(
      WithTimer(process.Subject(WorkerInternalMessage(message))),
    ),
    message_queue: deque.Deque(QueueMessage(message)),
    size: Int,
  )
}

/// The errors that can occur when scheduling a message to be processed by the pool.
pub type SchedulingError {
  /// The pool has reached its maximum capacity and cannot accept any more messages.
  CapacityExceeded
  /// The worker failed to start. This can happen if the worker's initialiser function returns an 
  /// error or if it fails to initialise within the given timeout. 
  FailedToStartWorker(cause: actor.StartError)
}

/// (internal) The message that can be sent to the dispatcher.
pub opaque type DispatcherMessage(message) {
  /// Schedule a message to be processed by the pool. The response will be sent to the given 
  /// receiver.
  DispatcherSchedule(
    receiver: option.Option(process.Subject(Result(Nil, SchedulingError))),
    message: DispatchMessage(message),
  )
  /// Notify the dispatcher that a worker is ready to process another message.
  DispatcherWorkerReady(
    worker: #(
      process.Subject(WorkerInternalMessage(message)),
      timestamp.Timestamp,
    ),
  )
  /// Notify the dispatcher that a worker has stopped (normally or abnormally).
  DispatcherWorkerStopped(
    worker: process.Subject(WorkerInternalMessage(message)),
  )
  /// Notify the dispatcher that a worker has been idle for too long and should be stopped.
  DispatcherFreeWorker(worker: process.Subject(WorkerInternalMessage(message)))
  /// Notify the dispatcher that a worker has failed to process a message within the given timeout 
  /// and should be stopped.
  TimeoutWorker(worker: process.Subject(WorkerInternalMessage(message)))
  /// Notify the dispatcher that a message from the queue should be discarded 
  TimeoutMessage
  /// Notify the dispatcher that a worker has been unassigned or stopped and a new message can be 
  /// processed
  DispatchQueue
}

/// (internal) The actor for the dispatcher
fn dispatcher(
  builder: Builder(state, message),
  factory_name: process.Name(
    factory.Message(
      process.Subject(DispatcherMessage(message)),
      process.Subject(WorkerInternalMessage(message)),
    ),
  ),
) -> Result(actor.Started(Nil), actor.StartError) {
  let emit_event = fn(event: EventMessage) {
    option.map(builder.event_receiver, process.send(_, event))
  }

  actor.new_with_initialiser(100, fn(subject) {
    DispatcherState(
      subject: subject,
      free_workers: deque.new(),
      busy_workers: [],
      message_queue: deque.new(),
      size: 0,
    )
    |> actor.initialised()
    |> Ok
  })
  |> fn(dispatcher) {
    builder.name
    |> option.map(actor.named(dispatcher, _))
    |> option.unwrap(dispatcher)
  }
  |> actor.on_message(fn(state, message: DispatcherMessage(message)) {
    // Helper function to assign work to a worker and notify the receiver and event receiver.
    let assign_work = fn(
      state: DispatcherState(message),
      worker: process.Subject(WorkerInternalMessage(message)),
      message: message,
      receiver: option.Option(process.Subject(Result(Nil, SchedulingError))),
    ) {
      // Free worker found
      process.send(worker, UserMessage(message))
      option.map(receiver, process.send(_, Ok(Nil)))
      emit_event(WorkerAssigned(
        state.size,
        deque.length(state.free_workers),
        builder.size,
      ))

      let timer =
        builder.processing_timeout
        |> option.map(process.send_after(
          state.subject,
          _,
          TimeoutWorker(worker),
        ))

      DispatcherState(..state, busy_workers: [
        WithTimer(data: worker, timer: timer),
        ..state.busy_workers
      ])
    }

    let unassign_worker = fn(
      workers: List(WithTimer(process.Subject(WorkerInternalMessage(message)))),
      worker: process.Subject(WorkerInternalMessage(message)),
    ) {
      list.filter(workers, fn(item) {
        case item.data == worker {
          False -> True
          True -> {
            option.map(item.timer, process.cancel_timer)
            False
          }
        }
      })
    }

    case message {
      DispatcherSchedule(receiver:, message:) -> {
        // Pop based on the strategy.
        let pop_res = case builder.strategy {
          FirstInFirstOut -> deque.pop_front(state.free_workers)
          LastInFirstOut -> deque.pop_back(state.free_workers)
          OldestInFirstOut -> {
            let workers =
              deque.to_list(state.free_workers)
              |> list.sort(fn(a, b) { timestamp.compare(a.age, b.age) })
              |> deque.from_list

            deque.pop_front(workers)
          }
        }

        case pop_res {
          Ok(#(worker, workers)) -> {
            // Free worker found
            option.map(worker.timeout_timer, process.cancel_timer)

            DispatcherState(..state, free_workers: workers)
            |> assign_work(
              worker.subject,
              case message {
                NewMessage(message:) -> message
                FromQueue(message:) -> message.message
              },
              receiver,
            )
            |> actor.continue()
          }
          Error(_) -> {
            // No free worker found, check if we can create a new one
            case int.compare(state.size, builder.size) {
              order.Gt | order.Eq -> {
                case builder.queue {
                  option.Some(queue_builder) -> {
                    case
                      int.compare(
                        deque.length(state.message_queue),
                        queue_builder.size,
                      )
                    {
                      order.Gt | order.Eq -> {
                        // Pool has reached max capacity, cannot accept new messages
                        option.map(receiver, process.send(
                          _,
                          Error(CapacityExceeded),
                        ))
                        actor.continue(state)
                      }
                      order.Lt -> {
                        let timeout = case message {
                          NewMessage(_) -> queue_builder.timeout
                          FromQueue(message:) ->
                            option.map(message.timeout_after, fn(timeout_after) {
                              timestamp.system_time()
                              |> timestamp.difference(timeout_after)
                              |> duration.to_milliseconds
                            })
                        }
                        let timer =
                          option.map(timeout, fn(timeout) {
                            let timer =
                              state.subject
                              |> process.send_after(timeout, TimeoutMessage)

                            #(
                              timer,
                              timestamp.system_time()
                                |> timestamp.add(duration.milliseconds(timeout)),
                            )
                          })

                        option.map(receiver, process.send(_, Ok(Nil)))

                        actor.continue(
                          DispatcherState(
                            ..state,
                            message_queue: state.message_queue
                              |> deque.push_back(QueueMessage(
                                message: case message {
                                  NewMessage(message:) -> message
                                  FromQueue(message:) -> message.message
                                },
                                timeout_after: option.map(timer, pair.second),
                                timer: option.map(timer, pair.first),
                              )),
                          ),
                        )
                      }
                    }
                  }
                  option.None -> {
                    // Pool has reached max capacity, cannot accept new messages
                    option.map(receiver, process.send(
                      _,
                      Error(CapacityExceeded),
                    ))
                    actor.continue(state)
                  }
                }
              }
              _ -> {
                // Start a new worker.
                let response =
                  factory.start_child(
                    factory.get_by_name(factory_name),
                    state.subject,
                  )

                case response {
                  Ok(worker) -> {
                    let state =
                      DispatcherState(
                        ..state,
                        free_workers: state.free_workers,
                        size: state.size + 1,
                      )
                    emit_event(WorkerCreated(
                      state.size,
                      deque.length(state.free_workers) + 1,
                      builder.size,
                    ))

                    state
                    |> assign_work(
                      worker.data,
                      case message {
                        NewMessage(message:) -> message
                        FromQueue(message:) -> message.message
                      },
                      receiver,
                    )
                    |> actor.continue()
                  }
                  Error(error) -> {
                    option.map(receiver, process.send(
                      _,
                      Error(FailedToStartWorker(error)),
                    ))
                    actor.continue(state)
                  }
                }
              }
            }
          }
        }
      }
      DispatcherWorkerReady(worker:) -> {
        // Worker has finished processing a message and is now free to process another one.
        let timer =
          option.map(builder.close_after, fn(timeout) {
            process.send_after(
              state.subject,
              timeout,
              DispatcherFreeWorker(worker.0),
            )
          })
        let busy_workers = unassign_worker(state.busy_workers, worker.0)
        emit_event(WorkerUnassigned(
          state.size,
          deque.length(state.free_workers) + 1,
          builder.size,
        ))

        option.map(builder.queue, fn(_) {
          process.send(state.subject, DispatchQueue)
        })

        actor.continue(
          DispatcherState(
            ..state,
            free_workers: deque.push_front(
              state.free_workers,
              Worker(timeout_timer: timer, subject: worker.0, age: worker.1),
            ),
            busy_workers: busy_workers,
          ),
        )
      }
      DispatcherWorkerStopped(worker) -> {
        // A worker has stopped. Remove it from the pool and notify the event receiver.
        emit_event(WorkerStopped(
          state.size - 1,
          deque.length(state.free_workers),
          builder.size,
        ))

        let busy_workers = unassign_worker(state.busy_workers, worker)

        option.map(builder.queue, fn(_) {
          process.send(state.subject, DispatchQueue)
        })

        actor.continue(
          DispatcherState(
            ..state,
            busy_workers: busy_workers,
            size: state.size - 1,
          ),
        )
      }
      DispatcherFreeWorker(worker:) -> {
        // A worker has been idle for too long and should be stopped.
        let workers =
          deque.to_list(state.free_workers)
          |> list.filter(fn(w) { w.subject != worker })
          |> deque.from_list

        process.send(worker, StopWorker)

        actor.continue(DispatcherState(..state, free_workers: workers))
      }
      TimeoutWorker(worker:) -> {
        let busy_workers =
          list.filter(state.busy_workers, fn(item) { item.data != worker })

        process.send(state.subject, DispatcherWorkerStopped(worker))

        actor.continue(DispatcherState(..state, busy_workers: busy_workers))
      }
      TimeoutMessage -> {
        let now = timestamp.system_time()
        let queue =
          state.message_queue
          |> deque.to_list()
          |> list.filter(fn(item) {
            option.map(item.timeout_after, fn(timeout_after) {
              case timestamp.compare(now, timeout_after) {
                order.Gt | order.Eq -> {
                  option.map(item.timer, process.cancel_timer)
                  False
                }
                order.Lt -> True
              }
            })
            |> option.unwrap(True)
          })
          |> deque.from_list()

        actor.continue(DispatcherState(..state, message_queue: queue))
      }
      DispatchQueue -> {
        case builder.queue {
          option.Some(queue_builder) -> {
            let res = case queue_builder.strategy {
              queue.LastInFirstOut -> deque.pop_back(state.message_queue)
              queue.FirstInFirstOut -> deque.pop_front(state.message_queue)
            }

            case res {
              Ok(#(message, queue)) -> {
                option.map(message.timer, process.cancel_timer)

                process.send(
                  state.subject,
                  DispatcherSchedule(option.None, FromQueue(message)),
                )

                actor.continue(DispatcherState(..state, message_queue: queue))
              }
              Error(_) -> actor.continue(state)
            }
          }
          option.None -> actor.continue(state)
        }
      }
    }
  })
  |> actor.start()
}

//-----------------------------------------------------------------------------------------------//
//                                            Worker                                             //
//-----------------------------------------------------------------------------------------------//

/// (internal) The message that can be sent to a worker.
type WorkerInternalMessage(message) {
  UserMessage(message: message)
  StopWorker
}

/// (internal) The result of a worker after processing a message. Same as `actor.Next` but for hive.
pub opaque type Next(state) {
  /// Continue processing with the updated state
  Continue(state: state)
  /// Continue processing without releasing the worker back to the dispatcher. This is useful when 
  /// the worker is waiting for a message to finish the processing.
  KeepBusy(state: state)
  /// Stop processing and terminate the worker normally
  Stop(process.ExitReason)
}

/// Continue processing with the updated state.
pub fn continue(state) -> Next(state) {
  Continue(state: state)
}

/// Continue processing without releasing the worker back to the dispatcher. This is useful when
/// the worker is waiting for a message to finish the processing.
pub fn keep_busy(state) -> Next(state) {
  KeepBusy(state: state)
}

/// Stop processing and terminate the worker normally
pub fn stop() -> Next(state) {
  Stop(process.Normal)
}

/// Stop processing and terminate the worker abnormally with the given reason
pub fn stop_abnormal(reason: String) -> Next(state) {
  Stop(process.Abnormal(dynamic.string(reason)))
}

/// (internal) The state of a worker. It keep tracks of the worker's parent (the dispatcher), 
/// itself the state exposed to the user. 
type WorkerState(state, message) {
  WorkerState(
    parent: process.Subject(DispatcherMessage(message)),
    subject: process.Subject(WorkerInternalMessage(message)),
    age: timestamp.Timestamp,
    state: state,
  )
}

/// (internal) The actor for a worker. It is responsible for processing messages and notifying the
/// dispatcher when it is ready to process another message or when it has stopped.
fn worker(
  builder: Builder(state, message),
  dispatcher: process.Subject(DispatcherMessage(message)),
) -> Result(
  actor.Started(process.Subject(WorkerInternalMessage(message))),
  actor.StartError,
) {
  actor.new_with_initialiser(builder.timeout, fn(subject) {
    let user_subject = process.new_subject()
    use state <- result.try(builder.initialiser(user_subject))

    let age = timestamp.system_time()

    WorkerState(parent: dispatcher, subject: subject, age: age, state: state)
    |> actor.initialised()
    |> actor.selecting(
      process.new_selector()
      |> process.select(subject)
      |> process.select_map(user_subject, UserMessage),
    )
    |> actor.returning(subject)
    |> Ok
  })
  |> actor.on_message(fn(state, message) {
    case message {
      UserMessage(message:) -> {
        let response = builder.on_message(state.state, message)

        case response {
          Continue(state: inner) -> {
            process.send(
              state.parent,
              DispatcherWorkerReady(#(state.subject, state.age)),
            )
            actor.continue(WorkerState(..state, state: inner))
          }
          KeepBusy(state: inner) -> {
            actor.continue(WorkerState(..state, state: inner))
          }
          Stop(process.Normal) -> {
            process.send(state.parent, DispatcherWorkerStopped(state.subject))
            actor.stop()
          }
          Stop(process.Abnormal(reason)) -> {
            process.send(state.parent, DispatcherWorkerStopped(state.subject))
            actor.stop_abnormal(
              decode.run(reason, decode.string)
              |> result.unwrap("Failed to decode reason"),
            )
          }
          Stop(process.Killed) -> {
            process.send(state.parent, DispatcherWorkerStopped(state.subject))
            actor.stop_abnormal("Worker was killed")
          }
        }
      }
      StopWorker -> {
        process.send(state.parent, DispatcherWorkerStopped(state.subject))
        actor.stop()
      }
    }
  })
  |> actor.start()
}

//-----------------------------------------------------------------------------------------------//
//                                            Builder                                            //
//-----------------------------------------------------------------------------------------------//

pub type Strategy {
  /// The first worker that was added to the free workers queue will be the first to be assigned a 
  /// message.
  FirstInFirstOut
  /// The last worker that was added to the free workers queue will be the first to be assigned a
  /// message.
  LastInFirstOut
  /// The worker that has been alive for the longest will be the first to be assigned a message.
  OldestInFirstOut
}

/// The builder for a pool containing its configuration.
pub opaque type Builder(state, message) {
  Builder(
    size: Int,
    timeout: Int,
    strategy: Strategy,
    processing_timeout: option.Option(Int),
    name: option.Option(process.Name(DispatcherMessage(message))),
    initialiser: fn(process.Subject(message)) -> Result(state, String),
    on_message: fn(state, message) -> Next(state),
    close_after: option.Option(Int),
    event_receiver: option.Option(process.Subject(EventMessage)),
    queue: option.Option(queue.Builder),
  )
}

/// Create a new builder with the given initialiser function and timeout for the initialiser. The 
/// default size will be 1 and strategy is FIFO.
/// 
/// If the worker does not initialise within the given timeout, the message will be rejected and 
/// an error will be returned from the `send` function.
pub fn new_with_initialiser(
  timeout: Int,
  initialiser: fn(process.Subject(message)) -> Result(state, String),
) -> Builder(state, message) {
  Builder(
    size: 1,
    timeout: timeout,
    strategy: FirstInFirstOut,
    processing_timeout: option.None,
    name: option.None,
    initialiser:,
    on_message: fn(state, _: message) { continue(state) },
    close_after: option.None,
    event_receiver: option.None,
    queue: option.None,
  )
}

/// Create a new builder with the given initial state. The default size will be 1 and strategy is FIFO.
pub fn new(initial: state) -> Builder(state, message) {
  new_with_initialiser(100, fn(_) { initial |> Ok })
}

/// Set the maximum number of workers in the pool. 
pub fn with_size(
  builder: Builder(state, message),
  size: Int,
) -> Builder(state, message) {
  Builder(..builder, size: size)
}

/// Set the strategy for scheduling messages to workers in the pool.
pub fn with_strategy(
  builder: Builder(state, message),
  strategy: Strategy,
) -> Builder(state, message) {
  Builder(..builder, strategy:)
}

/// Set the name of the pool. This is required to send messages to the pool when started under a 
/// supervisor.
pub fn with_name(
  builder: Builder(state, message),
  name: process.Name(DispatcherMessage(message)),
) -> Builder(state, message) {
  Builder(..builder, name: option.Some(name))
}

/// Set the maximum amount of time (in ms) a worker can be idle before it is stopped. If not set, 
/// workers will never be stopped automatically.
pub fn with_close_after(
  builder: Builder(state, message),
  close_after: Int,
) -> Builder(state, message) {
  Builder(..builder, close_after: option.Some(close_after))
}

/// Set the maximum amount of time (in ms) a worker can take to process a message before it is
/// considered to have failed. If not set, workers will never be considered to have failed due to
/// taking too long to process a message.
pub fn with_processing_timeout(
  builder: Builder(state, message),
  processing_timeout: Int,
) -> Builder(state, message) {
  Builder(..builder, processing_timeout: option.Some(processing_timeout))
}

/// Set the handler for processing messages.
pub fn on_message(
  builder: Builder(state, message),
  on_message: fn(state, message) -> Next(state),
) -> Builder(state, message) {
  Builder(..builder, on_message:)
}

/// Set the subject that should receive events emitted by the pool. This is primarily for testing 
/// purposes.
pub fn with_event_receiver(
  builder: Builder(state, message),
  event_receiver: process.Subject(EventMessage),
) -> Builder(state, message) {
  Builder(..builder, event_receiver: option.Some(event_receiver))
}

pub fn with_queue(
  builder: Builder(state, message),
  queue_builder: queue.Builder,
) -> Builder(state, message) {
  Builder(..builder, queue: option.Some(queue_builder))
}

/// Send a message to any available worker in the pool. If all workers are busy and the pool has 
/// reached its maximum capacity or if the worker fails to start, an error will be returned.
/// 
/// The timeout represents the maximum amount of time (in ms) to wait for a worker to start. If 
/// the worker does not start within the given timeout, an error will be returned.
pub fn send(
  subject: process.Subject(DispatcherMessage(message)),
  message: message,
  timeout: Int,
) -> Result(Nil, SchedulingError) {
  let receiver = process.new_subject()
  process.send(
    subject,
    DispatcherSchedule(option.Some(receiver), NewMessage(message)),
  )

  case process.receive(receiver, timeout) {
    Ok(inner) -> inner
    Error(_) -> Error(FailedToStartWorker(cause: actor.InitTimeout))
  }
}

/// Create the child specification for the pool to be started under a supervisor.
pub fn supervised(
  builder: Builder(state, message),
) -> supervision.ChildSpecification(static.Supervisor) {
  let factory_name = process.new_name("hive_factory")

  static.new(static.OneForOne)
  |> static.add(supervision.worker(fn() { dispatcher(builder, factory_name) }))
  |> static.add(
    factory.worker_child(worker(builder, _))
    |> factory.restart_strategy(supervision.Temporary)
    |> factory.named(factory_name)
    |> factory.supervised(),
  )
  |> static.supervised()
}
