//! Request and response bodies for the daemon's REST commands.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub use proto::budget::BudgetExtension;
use proto::report::{Answer, Question};
use proto::run::RunState;

/// Submit a flow for execution as a new run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SubmitRunRequest {
    /// The flow file, verbatim TOML. Sent as text so the daemon is the
    /// single validator; clients may pre-validate — Rust clients with
    /// the shared `proto::flow` parser, others against the published
    /// JSON Schema — but never re-encode.
    pub flow: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SubmitRunResponse {
    pub run_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ListRunsResponse {
    pub runs: Vec<RunListEntry>,
}

/// One entry of the run list. Usually a readable run's summary; a run
/// whose durable record the daemon cannot read is still listed — as
/// [`UnreadableRun`] — because omitting it would make absence
/// ambiguous between "gone" and "broken", exactly the distinction an
/// operator needs when a disk went bad. Untagged on the wire: both
/// variants are flat objects told apart by their `state` field, so
/// clients read one uniform shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum RunListEntry {
    Run(RunSummary),
    Unreadable(UnreadableRun),
}

/// A run that exists but cannot be read: its registry metadata is
/// intact — identity, kernel, agent, creation time — but projecting
/// its event log failed, so it has no state to report. Not a
/// [`proto::run::RunState`]: that enum is the run's lifecycle, and a
/// run is never *in* state "unreadable" — the daemon's read of it
/// failed. Its own status endpoint answers with the failure itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct UnreadableRun {
    pub run_id: String,
    /// Always `"unreadable"` — the discriminant that tells this entry
    /// from a run summary in the untagged list.
    pub state: UnreadableState,
    /// Human-readable description of what failed; never parse this.
    /// Names files within the run's directory, never a daemon-side
    /// path — the host's filesystem layout is the server log's
    /// business, not the wire's.
    pub reason: String,
    pub kernel: String,
    pub agent: String,
    /// RFC 3339 UTC timestamp.
    pub created_at: String,
}

/// The one value [`UnreadableRun::state`] ever holds. A type, not a
/// string, so the wire literal is pinned where the shape is defined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum UnreadableState {
    Unreadable,
}

/// One line of `hako list`: enough to see what a run is and where it
/// stands, nothing more.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunSummary {
    pub run_id: String,
    /// Flattened: `"state": "paused", "reason": "budget"` sit directly
    /// on the summary object.
    #[serde(flatten)]
    pub state: RunState,
    pub kernel: String,
    pub agent: String,
    /// RFC 3339 UTC timestamp.
    pub created_at: String,
    /// RFC 3339 UTC timestamp of the last event.
    pub updated_at: String,
}

/// The full picture of one run, also returned by every command that
/// changes a run so clients see the effect without a second request.
/// Extends the list line structurally — on the wire it reads as one
/// flat object, a summary with depth.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunStatusResponse {
    #[serde(flatten)]
    pub run: RunSummary,
    pub iterations_completed: u32,
    /// The summary from the agent's most recent report.
    pub last_summary: Option<String>,
    /// Open questions when paused `awaiting_human`; empty otherwise.
    #[serde(default)]
    pub pending_questions: Vec<Question>,
}

/// Answer one or more of a paused run's questions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AnswerRequest {
    pub answers: Vec<Answer>,
}

/// Resume a paused run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ResumeRequest {
    /// Guidance injected into the next iteration's prompt preamble.
    pub note: Option<String>,
    /// New caps for a run paused on `budget`; absent fields keep their
    /// current cap.
    pub extend: Option<BudgetExtension>,
}

/// Every non-2xx response carries this body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ApiError {
    pub code: ErrorCode,
    /// Human-readable; never parse this.
    pub message: String,
}

/// Every machine-readable failure code the daemon answers with — on
/// the wire as snake_case strings, e.g. `run_not_found`. Closed on the
/// producing end: the daemon's error mapping emits a named variant, so
/// a new failure mode adds one here, never a bare string. Open on the
/// consuming end: a client older than its daemon reads a code it does
/// not know as [`ErrorCode::Unknown`] and still gets the message,
/// instead of failing the whole response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Unauthorized,
    InvalidRequest,
    InvalidFlow,
    InvalidAgent,
    /// A secret the flow or its agent needs is not provisioned on the
    /// daemon. Distinct from [`ErrorCode::InvalidAgent`] because the
    /// fix is on the daemon host, not in the flow file: the message
    /// names what to provision.
    MissingSecret,
    RunNotFound,
    RunNotRunning,
    NotAwaitingInput,
    UnknownQuestion,
    NotPaused,
    /// The run predates a daemon restart: the material to relaunch it
    /// died with the old process, so it can never be resumed — and
    /// answers, which exist to feed a resume, are refused with it.
    NotResumable,
    InternalError,
    /// Never produced by the daemon — the deserialize-side net that
    /// catches codes newer than this build of the contract. The price
    /// is that the published OpenAPI enum lists it alongside the real
    /// codes.
    #[serde(other)]
    Unknown,
}

#[cfg(test)]
mod tests {
    use proto::run::PauseReason;
    use serde::de::DeserializeOwned;
    use serde_json::json;

    use super::*;

    fn round_trips<T>(value: &T)
    where
        T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let wire = serde_json::to_string(value).unwrap();
        assert_eq!(&serde_json::from_str::<T>(&wire).unwrap(), value);
    }

    #[test]
    fn a_paused_summary_reads_flat() {
        let summary = RunSummary {
            run_id: "r1".into(),
            state: RunState::Paused {
                reason: PauseReason::Budget,
            },
            kernel: "pipeline".into(),
            agent: "claude".into(),
            created_at: "2026-07-13T08:00:00Z".into(),
            updated_at: "2026-07-13T09:30:00Z".into(),
        };
        assert_eq!(
            serde_json::to_value(&summary).unwrap(),
            json!({
                "run_id": "r1",
                "state": "paused",
                "reason": "budget",
                "kernel": "pipeline",
                "agent": "claude",
                "created_at": "2026-07-13T08:00:00Z",
                "updated_at": "2026-07-13T09:30:00Z"
            })
        );
    }

    /// The degraded entry mirrors a summary's shape — `reason` sits
    /// beside `state` exactly as a pause reason does — and the
    /// entries are untagged on the wire: `"state"` alone tells a
    /// healthy run from an unreadable one, on both ends.
    #[test]
    fn a_list_mixing_healthy_and_unreadable_runs_reads_flat_and_round_trips() {
        let list = ListRunsResponse {
            runs: vec![
                RunListEntry::Run(RunSummary {
                    run_id: "r1".into(),
                    state: RunState::Paused {
                        reason: PauseReason::Budget,
                    },
                    kernel: "pipeline".into(),
                    agent: "claude".into(),
                    created_at: "2026-07-13T08:00:00Z".into(),
                    updated_at: "2026-07-13T09:30:00Z".into(),
                }),
                RunListEntry::Unreadable(UnreadableRun {
                    run_id: "r2".into(),
                    state: UnreadableState::Unreadable,
                    reason: "corrupt run record events.jsonl: line 3".into(),
                    kernel: "pipeline".into(),
                    agent: "claude".into(),
                    created_at: "2026-07-13T07:00:00Z".into(),
                }),
            ],
        };
        assert_eq!(
            serde_json::to_value(&list.runs[1]).unwrap(),
            json!({
                "run_id": "r2",
                "state": "unreadable",
                "reason": "corrupt run record events.jsonl: line 3",
                "kernel": "pipeline",
                "agent": "claude",
                "created_at": "2026-07-13T07:00:00Z"
            })
        );
        round_trips(&list);
    }

    /// What keeps the untagged split safe is that the two shapes stay
    /// disjoint — no object can match both. Two guards, each pinned
    /// against its nearest collision: a paused summary also carries a
    /// `reason` beside its `state`, but `"paused"` is no
    /// [`UnreadableState`]; a drifted unreadable entry may grow
    /// fields a summary has, but `"unreadable"` is no [`RunState`].
    /// An object matching neither shape is a parse error, not a guess.
    #[test]
    fn list_entry_shapes_stay_disjoint_so_state_alone_discriminates() {
        let paused_with_reason = json!({
            "run_id": "r1",
            "state": "paused",
            "reason": "budget",
            "kernel": "pipeline",
            "agent": "claude",
            "created_at": "2026-07-13T08:00:00Z",
            "updated_at": "2026-07-13T09:30:00Z"
        });
        assert!(matches!(
            serde_json::from_value::<RunListEntry>(paused_with_reason).unwrap(),
            RunListEntry::Run(_)
        ));

        let unreadable_with_drifted_field = json!({
            "run_id": "r2",
            "state": "unreadable",
            "reason": "corrupt run record events.jsonl: line 3",
            "kernel": "pipeline",
            "agent": "claude",
            "created_at": "2026-07-13T07:00:00Z",
            "updated_at": "2026-07-13T08:00:00Z"
        });
        assert!(matches!(
            serde_json::from_value::<RunListEntry>(unreadable_with_drifted_field).unwrap(),
            RunListEntry::Unreadable(_)
        ));

        let matches_neither = json!({
            "run_id": "r3",
            "state": "running",
            "kernel": "pipeline",
            "agent": "claude",
            "created_at": "2026-07-13T08:00:00Z"
        });
        serde_json::from_value::<RunListEntry>(matches_neither).unwrap_err();
    }

    #[test]
    fn commands_round_trip() {
        let submit = SubmitRunRequest {
            flow: "[loop]\nkernel = \"pipeline\"\n".into(),
        };
        let answer = AnswerRequest {
            answers: vec![Answer {
                question_id: "q1".into(),
                answer: "plain files".into(),
            }],
        };
        let resume = ResumeRequest {
            note: Some("skip the flaky suite".into()),
            extend: Some(BudgetExtension {
                max_iterations: Some(30),
                max_wall_clock_seconds: None,
                max_tokens: None,
            }),
        };
        round_trips(&submit);
        round_trips(&answer);
        round_trips(&resume);
    }

    /// The wire strings are the published contract; the enum only
    /// names them. The `match` is the pin: a new variant fails to
    /// compile here until its wire string is written down, and a
    /// rename that would silently break clients fails the assertion.
    #[test]
    fn error_codes_keep_their_wire_strings() {
        let wire = |code| match code {
            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::InvalidRequest => "invalid_request",
            ErrorCode::InvalidFlow => "invalid_flow",
            ErrorCode::InvalidAgent => "invalid_agent",
            ErrorCode::MissingSecret => "missing_secret",
            ErrorCode::RunNotFound => "run_not_found",
            ErrorCode::RunNotRunning => "run_not_running",
            ErrorCode::NotAwaitingInput => "not_awaiting_input",
            ErrorCode::UnknownQuestion => "unknown_question",
            ErrorCode::NotPaused => "not_paused",
            ErrorCode::NotResumable => "not_resumable",
            ErrorCode::InternalError => "internal_error",
            ErrorCode::Unknown => "unknown",
        };
        for code in [
            ErrorCode::Unauthorized,
            ErrorCode::InvalidRequest,
            ErrorCode::InvalidFlow,
            ErrorCode::InvalidAgent,
            ErrorCode::MissingSecret,
            ErrorCode::RunNotFound,
            ErrorCode::RunNotRunning,
            ErrorCode::NotAwaitingInput,
            ErrorCode::UnknownQuestion,
            ErrorCode::NotPaused,
            ErrorCode::NotResumable,
            ErrorCode::InternalError,
            ErrorCode::Unknown,
        ] {
            assert_eq!(serde_json::to_value(code).unwrap(), json!(wire(code)));
        }
        round_trips(&ApiError {
            code: ErrorCode::RunNotFound,
            message: "no such run".into(),
        });
    }

    /// A daemon newer than this client may answer with a code this
    /// build does not name. Failing the whole response over that one
    /// unrecognized code would hide the message explaining it.
    #[test]
    fn a_code_from_a_newer_daemon_still_reads_as_an_api_error() {
        let error: ApiError = serde_json::from_value(json!({
            "code": "newer_daemon_code",
            "message": "upgrade the client"
        }))
        .unwrap();
        assert_eq!(error.code, ErrorCode::Unknown);
        assert_eq!(error.message, "upgrade the client");
    }

    /// The nested `RunSummary` must be invisible on the wire — a
    /// status is a flat object, a summary with depth.
    #[test]
    fn a_status_reads_flat_like_a_summary() {
        let status = RunStatusResponse {
            run: RunSummary {
                run_id: "r9".into(),
                state: RunState::Running,
                kernel: "pipeline".into(),
                agent: "codex".into(),
                created_at: "2026-07-13T08:00:00Z".into(),
                updated_at: "2026-07-13T09:30:00Z".into(),
            },
            iterations_completed: 4,
            last_summary: Some("parser ported, printer next".into()),
            pending_questions: vec![],
        };
        assert_eq!(
            serde_json::to_value(&status).unwrap(),
            json!({
                "run_id": "r9",
                "state": "running",
                "kernel": "pipeline",
                "agent": "codex",
                "created_at": "2026-07-13T08:00:00Z",
                "updated_at": "2026-07-13T09:30:00Z",
                "iterations_completed": 4,
                "last_summary": "parser ported, printer next",
                "pending_questions": []
            })
        );
        round_trips(&status);
    }
}
