import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/static_supervisor as supervisor
import gleeunit
import gleeunit/should
import hive
import hive/event
import hive/queue
import logging

pub fn main() -> Nil {
  logging.configure()
  gleeunit.main()
}

pub type Message {
  Sleep(Int)
  Ping(str: String, reply_to: process.Subject(String))
  StopNormal
  StopAbnormal
}

fn handle_message(state, message: Message) {
  case message {
    Sleep(duration) -> {
      process.sleep(duration)
      hive.continue(state)
    }
    Ping(str, reply_to) -> {
      process.send(reply_to, str)
      hive.continue(state)
    }
    StopNormal -> hive.stop()
    StopAbnormal -> hive.stop_abnormal("abnormal stop requested")
  }
}

/// Tests the behaviour of a pool with a size of 0, which should reject all messages as there are no workers to handle them.
pub fn full_pool_test() {
  let pool_name = process.new_name("pool")

  let pool_spec =
    hive.new(Nil)
    |> hive.with_size(0)
    |> hive.with_name(pool_name)
    |> hive.on_message(fn(state, _) { hive.continue(state) })
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  hive.send(pool, Nil, 100)
  |> should.be_error()
  |> should.equal(hive.CapacityExceeded)
}

/// Tests the behaviour of a pool with a size of 1,  which should accept the first message and reject the next one while the first is still being processed.
pub fn schedule_and_fill_test() {
  let pool_name = process.new_name("pool")

  let subject = process.new_subject()

  let pool_spec =
    hive.new(Nil)
    |> hive.with_size(1)
    |> hive.with_name(pool_name)
    |> hive.on_message(handle_message)
    |> hive.with_event_receiver(subject)
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  // Accept the first message
  hive.send(pool, Sleep(100), 100)
  |> should.be_ok()

  // Worker should be created
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerCreated(count: 1, free: 1, capacity: 1))

  // Worker should be assigned
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  // Reject the second message as the pool is full
  hive.send(pool, Sleep(100), 100)
  |> should.be_error()
  |> should.equal(hive.CapacityExceeded)

  // Worker should be unassigned within 200ms 
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerUnassigned(count: 1, free: 1, capacity: 1))

  // Accept the third message as the worker is now free
  hive.send(pool, Sleep(100), 100)
  |> should.be_ok()

  // Worker should be assigned again
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))
}

/// Tests the behaviour of a pool with a size of 1, which should accept the first message and automatically clean the worker after a certain amount of time.
pub fn schedule_and_free_test() {
  let pool_name = process.new_name("pool")

  let subject = process.new_subject()

  let pool_spec =
    hive.new(Nil)
    |> hive.with_size(1)
    |> hive.with_name(pool_name)
    |> hive.on_message(handle_message)
    |> hive.with_event_receiver(subject)
    |> hive.with_close_after(100)
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  // Accept the first message
  hive.send(pool, Sleep(100), 100)
  |> should.be_ok()

  // Worker should be created
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerCreated(count: 1, free: 1, capacity: 1))

  // Worker should be assigned
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  // Worker should be unassigned within 200ms 
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerUnassigned(count: 1, free: 1, capacity: 1))

  // Worker should be unassigned within 200ms 
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerStopped(count: 0, free: 0, capacity: 1))

  // Accept the third message as the worker is now free
  hive.send(pool, Sleep(100), 100)
  |> should.be_ok()

  // Worker should be created
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerCreated(count: 1, free: 1, capacity: 1))

  // Worker should be assigned again
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))
}

/// Tests the behaviour of a pool when a worker emits a stop command
pub fn stop_worker_test() {
  let pool_name = process.new_name("pool")

  let subject = process.new_subject()

  let pool_spec =
    hive.new(Nil)
    |> hive.with_size(1)
    |> hive.with_name(pool_name)
    |> hive.on_message(handle_message)
    |> hive.with_event_receiver(subject)
    |> hive.with_close_after(100)
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  // Accept the first message
  hive.send(pool, StopNormal, 100)
  |> should.be_ok()

  // Worker should be created
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerCreated(count: 1, free: 1, capacity: 1))

  // Worker should be assigned
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  // Worker should be stopped
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerStopped(count: 0, free: 0, capacity: 1))
}

/// Tests the behaviour of a pool when a worker emits an abnormal stop command
pub fn crash_worker_test() {
  let pool_name = process.new_name("pool")

  let subject = process.new_subject()

  let pool_spec =
    hive.new(Nil)
    |> hive.with_size(1)
    |> hive.with_name(pool_name)
    |> hive.on_message(handle_message)
    |> hive.with_event_receiver(subject)
    |> hive.with_close_after(100)
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  // Accept the first message
  hive.send(pool, StopAbnormal, 100)
  |> should.be_ok()

  // Worker should be created
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerCreated(count: 1, free: 1, capacity: 1))

  // Worker should be assigned
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  // Worker should be stopped 
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerStopped(count: 0, free: 0, capacity: 1))

  // Verify the pool is still functional and can accept new messages
  hive.send(pool, Sleep(100), 100)
  |> should.be_ok()

  // Worker should be created
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerCreated(count: 1, free: 1, capacity: 1))

  // Worker should be assigned again
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  // Worker should be unassigned within 200ms
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerUnassigned(count: 1, free: 1, capacity: 1))
}

/// Tests the behaviour of a pool when a worker does not initialise within the specified timeout
pub fn timeout_worker_init_test() {
  let pool_name = process.new_name("pool")

  let subject = process.new_subject()

  let pool_spec =
    hive.new_with_initialiser(1, fn(_) {
      process.sleep(100)
      Ok(Nil)
    })
    |> hive.with_size(1)
    |> hive.with_name(pool_name)
    |> hive.on_message(handle_message)
    |> hive.with_event_receiver(subject)
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  // Accept the first message
  hive.send(pool, Sleep(100), 100)
  |> should.be_error()
  |> should.equal(hive.FailedToStartWorker(cause: actor.InitTimeout))
}

/// Tests the behaviour of a pool when a worker fails to initailise
pub fn fail_worker_init_test() {
  let pool_name = process.new_name("pool")

  let subject = process.new_subject()

  let pool_spec =
    hive.new_with_initialiser(100, fn(_) { Error("init failed") })
    |> hive.with_size(1)
    |> hive.with_name(pool_name)
    |> hive.on_message(handle_message)
    |> hive.with_event_receiver(subject)
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  // Accept the first message
  hive.send(pool, Sleep(100), 100)
  |> should.be_error()
  |> should.equal(
    hive.FailedToStartWorker(cause: actor.InitFailed("init failed")),
  )
}

pub fn worker_processing_timeout_test() {
  let pool_name = process.new_name("pool")

  let subject = process.new_subject()

  let pool_spec =
    hive.new(Nil)
    |> hive.with_size(1)
    |> hive.with_name(pool_name)
    |> hive.on_message(handle_message)
    |> hive.with_event_receiver(subject)
    |> hive.with_processing_timeout(100)
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  // • Sleep time > processing timeout, so the worker should be stopped after 100ms
  {
    hive.send(pool, Sleep(500), 100)
    |> should.be_ok()

    // Worker should be created
    process.receive(subject, 100)
    |> should.be_ok()
    |> should.equal(event.WorkerCreated(count: 1, free: 1, capacity: 1))

    // Worker should be assigned
    process.receive(subject, 100)
    |> should.be_ok()
    |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

    // Worker should be stopped (150ms to account for scheduling delays)
    process.receive(subject, 150)
    |> should.be_ok()
    |> should.equal(event.WorkerStopped(count: 0, free: 0, capacity: 1))
  }

  // • Sleep time < processing timeout, so the worker should complete successfully
  {
    hive.send(pool, Sleep(1), 100)
    |> should.be_ok()

    // Worker should be created
    process.receive(subject, 100)
    |> should.be_ok()
    |> should.equal(event.WorkerCreated(count: 1, free: 1, capacity: 1))

    // Worker should be assigned
    process.receive(subject, 100)
    |> should.be_ok()
    |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

    // Worker should be unassigned and not stopped
    process.receive(subject, 100)
    |> should.be_ok()
    |> should.equal(event.WorkerUnassigned(count: 1, free: 1, capacity: 1))
  }
}

pub fn fifo_queue_test() {
  let pool_name = process.new_name("pool")

  let subject = process.new_subject()
  let ping_subject = process.new_subject()

  let pool_spec =
    hive.new(Nil)
    |> hive.with_size(1)
    |> hive.with_name(pool_name)
    |> hive.on_message(handle_message)
    |> hive.with_event_receiver(subject)
    |> hive.with_queue(
      queue.new()
      |> queue.with_size(2)
      |> queue.with_strategy(queue.FirstInFirstOut),
    )
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  // Accept the first message
  hive.send(pool, Sleep(100), 100)
  |> should.be_ok()

  // Worker should be created
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerCreated(count: 1, free: 1, capacity: 1))

  // Worker should be assigned
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  // 2nd message should be queued
  hive.send(pool, Ping("A", ping_subject), 100)
  |> should.be_ok()

  // 3rd message should be queued
  hive.send(pool, Ping("B", ping_subject), 100)
  |> should.be_ok()

  // 4th message should be rejected
  hive.send(pool, Ping("C", ping_subject), 100)
  |> should.be_error()
  |> should.equal(hive.CapacityExceeded)

  // Worker should be unassigned (done processing 1st message)
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerUnassigned(count: 1, free: 1, capacity: 1))

  // Worker should be assigned (processing 2nd message)
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  process.receive(ping_subject, 200)
  |> should.be_ok()
  |> should.equal("A")

  // Worker should be unassigned (done processing 2nd message)
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerUnassigned(count: 1, free: 1, capacity: 1))

  // Worker should be assigned (processing 3rd message)
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  process.receive(ping_subject, 200)
  |> should.be_ok()
  |> should.equal("B")

  // Worker should be unassigned (done processing 3rd message)
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerUnassigned(count: 1, free: 1, capacity: 1))
}

pub fn lifo_queue_test() {
  let pool_name = process.new_name("pool")

  let subject = process.new_subject()
  let ping_subject = process.new_subject()

  let pool_spec =
    hive.new(Nil)
    |> hive.with_size(1)
    |> hive.with_name(pool_name)
    |> hive.on_message(handle_message)
    |> hive.with_event_receiver(subject)
    |> hive.with_queue(
      queue.new()
      |> queue.with_size(2)
      |> queue.with_strategy(queue.LastInFirstOut),
    )
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  // Accept the first message
  hive.send(pool, Sleep(100), 100)
  |> should.be_ok()

  // Worker should be created
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerCreated(count: 1, free: 1, capacity: 1))

  // Worker should be assigned
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  // 2nd message should be queued
  hive.send(pool, Ping("A", ping_subject), 100)
  |> should.be_ok()

  // 3rd message should be queued
  hive.send(pool, Ping("B", ping_subject), 100)
  |> should.be_ok()

  // 4th message should be rejected
  hive.send(pool, Ping("C", ping_subject), 100)
  |> should.be_error()
  |> should.equal(hive.CapacityExceeded)

  // Worker should be unassigned (done processing 1st message)
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerUnassigned(count: 1, free: 1, capacity: 1))

  // Worker should be assigned (processing 2nd message)
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  process.receive(ping_subject, 200)
  |> should.be_ok()
  |> should.equal("B")

  // Worker should be unassigned (done processing 2nd message)
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerUnassigned(count: 1, free: 1, capacity: 1))

  // Worker should be assigned (processing 3rd message)
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerAssigned(count: 1, free: 0, capacity: 1))

  process.receive(ping_subject, 200)
  |> should.be_ok()
  |> should.equal("A")

  // Worker should be unassigned (done processing 3rd message)
  process.receive(subject, 200)
  |> should.be_ok()
  |> should.equal(event.WorkerUnassigned(count: 1, free: 1, capacity: 1))
}
