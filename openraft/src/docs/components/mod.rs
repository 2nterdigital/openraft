//! Components and sub systems of the openraft project.
//!
//! - [`Engine and Runtime`](engine_runtime)
//! - [`StateMachine`](state_machine)
//! - [`Election observer`](election_observer)

/// Optional facts emitted directly at native election boundaries.
pub mod election_observer {
    #![doc = include_str!("election-observer.md")]
}

pub mod engine_runtime {
    #![doc = include_str!("engine-runtime.md")]
}

pub mod state_machine {
    #![doc = include_str!("state-machine.md")]
}
