//! The report vocabulary kernels share. The statuses, questions, and
//! answers live in `proto` — uniform across kernels so HITL behaves
//! the same whatever loop is running; the report *shapes* an agent
//! writes are each kernel's own.

pub use proto::progress::{Answer, ProgressStatus, Question};
