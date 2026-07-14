//! The progress report — the only structured channel from agent to
//! engine. The shape lives in `proto`; kernels ingest an agent's
//! report through [`ProgressReport::from_agent_json`], the strict
//! parse that feeds the repair re-prompt on failure.

pub use proto::progress::{ProgressReport, ProgressStatus, Question};
