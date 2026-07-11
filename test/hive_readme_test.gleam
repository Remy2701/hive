import gleam/erlang/process
import gleam/otp/static_supervisor as supervisor
import hive

type State {
  State(count: Int)
}

type Message {
  AddOne
}

pub fn readme_test() {
  let pool_name = process.new_name("pool")

  let pool_spec =
    hive.new(State(count: 1))
    |> hive.with_size(2)
    |> hive.with_name(pool_name)
    |> hive.on_message(fn(state, message) {
      // Simulate some work
      process.sleep(100)
      case message {
        AddOne -> hive.continue(State(count: state.count + 1))
      }
    })
    |> hive.supervised()

  let assert Ok(_) =
    supervisor.new(supervisor.OneForOne)
    |> supervisor.add(pool_spec)
    |> supervisor.start

  let pool = process.named_subject(pool_name)

  let assert Ok(_) = hive.send(pool, AddOne, 100)
  let assert Ok(_) = hive.send(pool, AddOne, 100)

  // The pool has a size of 2 and is currently processing 2 messages, so the next message will be rejected  
  let assert Error(hive.CapacityExceeded) = hive.send(pool, AddOne, 100)

  // Wait for the worker to complete
  process.sleep(200)

  // The pool should now have a free worker
  let assert Ok(_) = hive.send(pool, AddOne, 100)
}
