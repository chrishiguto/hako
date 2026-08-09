//! Proof the contract holds: every seam is satisfiable by the
//! testkit's in-process fakes, the traits are dyn-compatible behind
//! `Arc`, and a kernel can drive a full iteration through
//! `KernelContext` alone — no VMs, no LLMs, no network.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use engine::testkit::{
    self, MapSecrets, RecordingNotifier, RecordingSink, ScriptedAgent, ScriptedSandbox, Transcript,
};
use engine::{
    ExecEvent, ExitStatus, Kernel, KernelContext, KernelError, Notification, OutputStream,
    PauseReason, ReportStatus, RunEvent, RunId, RunOutcome, RunState, SandboxSpec, SecretName,
    TokenUsage, Workspace,
};
use futures_util::StreamExt;

/// One hand-written iteration exercising every collaborator: resolve a
/// secret, boot a sandbox, run the agent, collect its report, tear the
/// sandbox down, pause for the human.
struct OneIterationKernel;

#[async_trait]
impl Kernel for OneIterationKernel {
    async fn run(&self, ctx: KernelContext) -> Result<RunOutcome, KernelError> {
        ctx.sandbox.preflight().await?;
        let token = ctx.secrets.resolve(&SecretName::new("GH_TOKEN")).await?;

        ctx.events
            .emit(RunEvent::RunStarted {
                kernel: "one-iteration".into(),
                agent: ctx.agent.name().into(),
            })
            .await?;
        ctx.events
            .emit(RunEvent::IterationStarted { iteration: 1 })
            .await?;

        let spec = SandboxSpec {
            workspace: ctx.workspace.mount(),
            env: BTreeMap::from([("GH_TOKEN".to_string(), token)]),
        };
        let vm = ctx.sandbox.create(&spec).await?;

        let prompt = "iteration 1";
        ctx.sandbox
            .put_file(
                &vm,
                &spec.workspace.guest.join("prompt.md"),
                prompt.as_bytes(),
            )
            .await?;

        let mut output = ctx
            .sandbox
            .exec_stream(&vm, &ctx.agent.invocation(prompt))
            .await?;
        let mut stdout = String::new();
        let mut exit = None;
        while let Some(event) = output.next().await {
            // Exec chunks are raw bytes; decoding them for the event
            // log happens here, in kernel code, and nowhere else.
            match event? {
                ExecEvent::Stdout(bytes) => {
                    let chunk = String::from_utf8_lossy(&bytes).into_owned();
                    stdout.push_str(&chunk);
                    ctx.events
                        .emit(RunEvent::AgentOutput {
                            iteration: 1,
                            stream: OutputStream::Stdout,
                            chunk,
                        })
                        .await?;
                }
                ExecEvent::Stderr(bytes) => {
                    ctx.events
                        .emit(RunEvent::AgentOutput {
                            iteration: 1,
                            stream: OutputStream::Stderr,
                            chunk: String::from_utf8_lossy(&bytes).into_owned(),
                        })
                        .await?;
                }
                ExecEvent::Exited(status) => exit = Some(status),
            }
        }
        if !exit.is_some_and(|status| status.success()) {
            return Ok(RunOutcome::Failed);
        }

        if let Some(usage) = ctx.agent.token_usage(&stdout) {
            ctx.events
                .emit(RunEvent::TokensUsed {
                    iteration: 1,
                    usage,
                })
                .await?;
        }

        // The report shape is this kernel's own — kernels parse their
        // reports themselves; only the status vocabulary is shared.
        let raw = ctx
            .sandbox
            .get_file(&vm, &ctx.workspace.guest_report_path())
            .await?;
        let report: serde_json::Value =
            serde_json::from_slice(&raw).expect("the test seeds a valid report");
        let status: ReportStatus = serde_json::from_value(report["status"].clone())
            .expect("the test's report carries a status");

        ctx.sandbox.destroy(vm).await?;

        if status != ReportStatus::NeedsInput {
            ctx.events
                .emit(RunEvent::StateChanged {
                    state: RunState::Done,
                })
                .await?;
            return Ok(RunOutcome::Done);
        }
        ctx.events
            .emit(RunEvent::StateChanged {
                state: RunState::Paused {
                    reason: PauseReason::AwaitingHuman,
                },
            })
            .await?;
        ctx.notifier
            .notify(&Notification {
                run_id: ctx.run_id.clone(),
                reason: PauseReason::AwaitingHuman,
                summary: report["summary"]
                    .as_str()
                    .expect("the test's report carries a summary")
                    .to_owned(),
            })
            .await?;
        Ok(RunOutcome::Paused(PauseReason::AwaitingHuman))
    }
}

#[tokio::test]
async fn a_fully_faked_kernel_drives_one_iteration_end_to_end() {
    let transcript: Transcript = vec![
        Ok(ExecEvent::Stdout(b"working on it\n".to_vec())),
        Ok(ExecEvent::Stdout(b"tokens used: 142\n".to_vec())),
        Ok(ExecEvent::Exited(ExitStatus { code: Some(0) })),
    ];
    let sandbox = Arc::new(ScriptedSandbox::scripted(vec![transcript]));
    let workspace = Workspace::at("/var/lib/hako/runs/r1/workspace");
    // The report file the agent "wrote", seeded up front because the
    // scripted exec transcript never touches the file store.
    sandbox.seed_file(
        workspace.guest_report_path(),
        br#"{
            "status": "needs_input",
            "summary": "need a decision before the schema can land",
            "remaining": ["wire the store"],
            "blockers": [],
            "questions": [{"id": "q1", "text": "sqlite or plain files?", "options": []}]
        }"#
        .to_vec(),
    );
    let sink = Arc::new(RecordingSink::default());
    let notifier = Arc::new(RecordingNotifier::default());

    let ctx = testkit::context()
        .workspace(workspace)
        .sandbox(sandbox.clone())
        .agent(Arc::new(
            ScriptedAgent::new()
                .requiring("GH_TOKEN")
                .reporting(TokenUsage {
                    input: 142,
                    output: 17,
                }),
        ))
        .events(sink.clone())
        .notifier(notifier.clone())
        .secrets(Arc::new(MapSecrets::new([("GH_TOKEN", "ghp_fake")])))
        .build();

    // Held as `dyn Kernel`: the registry that maps flow names to
    // kernels needs the seam to be dyn-compatible.
    let kernel: Arc<dyn Kernel> = Arc::new(OneIterationKernel);
    let outcome = kernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::AwaitingHuman));

    // Kinds come off the wire tag, so they cannot drift from the serde
    // mapping `proto` owns.
    let kinds: Vec<String> = sink
        .events()
        .iter()
        .map(|e| {
            serde_json::to_value(e).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        kinds,
        vec![
            "run_started",
            "iteration_started",
            "agent_output",
            "agent_output",
            "tokens_used",
            "state_changed",
        ]
    );

    let notifications = notifier.notifications();
    assert_eq!(notifications.len(), 1);
    assert_eq!(notifications[0].reason, PauseReason::AwaitingHuman);
    assert_eq!(notifications[0].run_id, RunId::new("r1"));

    // Every sandbox the kernel booted was also torn down.
    assert_eq!(sandbox.created(), sandbox.destroyed());
}
