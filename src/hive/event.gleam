//-----------------------------------------------------------------------------------------------//
//                                         Event Message                                         //
//-----------------------------------------------------------------------------------------------//

/// Events that can be emitted by the pool. They can be used to monitor a pool's activity but are 
/// primarily for testing purposes.
pub type EventMessage {
  WorkerCreated(count: Int, free: Int, capacity: Int)
  WorkerStopped(count: Int, free: Int, capacity: Int)
  WorkerAssigned(count: Int, free: Int, capacity: Int)
  WorkerUnassigned(count: Int, free: Int, capacity: Int)
}
