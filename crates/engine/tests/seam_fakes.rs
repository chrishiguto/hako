//! Proof the contract holds: every seam is implementable by an
//! in-process fake, the traits are dyn-compatible behind `Arc`, and a
//! kernel can drive a full iteration through `KernelContext` alone —
//! no VMs, no LLMs, no network.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use engine::{
    AgentAdapter, Budgets, EventSink, EventSinkError, ExecEvent, ExecSpec, ExecStream, ExitStatus,
    Kernel, KernelContext, KernelError, Notification, Notifier, NotifierError, OutputStream,
    PauseReason, PromptsConfig, ReportStatus, RunEvent, RunId, RunOutcome, RunState, Sandbox,
    SandboxError, SandboxHandle, SandboxSpec, SecretName, SecretValue, SecretsError,
    SecretsProvider, TokenUsage, VerifyConfig, Workspace,
};
use futures_util::{StreamExt, stream};

/// Boots nothing: hands out handles, remembers files put into it, and
/// replays a scripted agent transcript on exec.
#[derive(Default)]
struct FakeSandbox {
    created: AtomicU32,
    destroyed: AtomicU32,
    files: Mutex<BTreeMap<PathBuf, Vec<u8>>>,
}

#[async_trait]
impl Sandbox for FakeSandbox {
    async fn create(&self, _spec: &SandboxSpec) -> Result<SandboxHandle, SandboxError> {
        let n = self.created.fetch_add(1, Ordering::SeqCst);
        Ok(SandboxHandle::new(format!("fake-vm-{n}")))
    }

    async fn exec_stream(
        &self,
        _sandbox: &SandboxHandle,
        _command: &ExecSpec,
    ) -> Result<ExecStream, SandboxError> {
        let transcript = vec![
            Ok(ExecEvent::Stdout(b"working on it\n".to_vec())),
            Ok(ExecEvent::Stdout(b"tokens used: 142\n".to_vec())),
            Ok(ExecEvent::Exited(ExitStatus { code: Some(0) })),
        ];
        Ok(stream::iter(transcript).boxed())
    }

    async fn put_file(
        &self,
        _sandbox: &SandboxHandle,
        path: &Path,
        contents: &[u8],
    ) -> Result<(), SandboxError> {
        self.files
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), contents.to_vec());
        Ok(())
    }

    async fn get_file(
        &self,
        _sandbox: &SandboxHandle,
        path: &Path,
    ) -> Result<Vec<u8>, SandboxError> {
        self.files
            .lock()
            .unwrap()
            .get(path)
            .cloned()
            .ok_or_else(|| SandboxError(format!("no such file: {}", path.display())))
    }

    async fn remove_file(&self, _sandbox: &SandboxHandle, path: &Path) -> Result<(), SandboxError> {
        self.files.lock().unwrap().remove(path);
        Ok(())
    }

    async fn destroy(&self, _sandbox: SandboxHandle) -> Result<(), SandboxError> {
        self.destroyed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn preflight(&self) -> Result<(), SandboxError> {
        Ok(())
    }
}

/// A scripted agent: fixed invocation shape, fixed token report.
struct ScriptedAgent;

impl AgentAdapter for ScriptedAgent {
    fn name(&self) -> &str {
        "scripted"
    }

    fn required_secrets(&self) -> Vec<SecretName> {
        vec![SecretName::new("GH_TOKEN")]
    }

    fn invocation(&self, prompt: &str) -> ExecSpec {
        ExecSpec {
            argv: vec!["scripted-agent".into(), "--prompt".into(), prompt.into()],
            cwd: None,
        }
    }

    fn token_usage(&self, stdout: &str) -> Option<TokenUsage> {
        stdout.contains("tokens used").then_some(TokenUsage {
            input: 142,
            output: 17,
        })
    }
}

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<RunEvent>>,
}

#[async_trait]
impl EventSink for RecordingSink {
    async fn emit(&self, event: RunEvent) -> Result<(), EventSinkError> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

#[derive(Default)]
struct RecordingNotifier {
    notifications: Mutex<Vec<Notification>>,
}

#[async_trait]
impl Notifier for RecordingNotifier {
    async fn notify(&self, notification: &Notification) -> Result<(), NotifierError> {
        self.notifications
            .lock()
            .unwrap()
            .push(notification.clone());
        Ok(())
    }
}

struct MapSecrets(BTreeMap<SecretName, SecretValue>);

#[async_trait]
impl SecretsProvider for MapSecrets {
    async fn resolve(&self, name: &SecretName) -> Result<SecretValue, SecretsError> {
        self.0
            .get(name)
            .cloned()
            .ok_or_else(|| SecretsError::NotFound(name.clone()))
    }
}

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
    let sandbox = Arc::new(FakeSandbox::default());
    let workspace = Workspace::at("/var/lib/hako/runs/r1/workspace");
    // The report file the agent "wrote", seeded up front because the
    // scripted exec transcript never touches the file store.
    sandbox.files.lock().unwrap().insert(
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
    let secrets = MapSecrets(BTreeMap::from([(
        SecretName::new("GH_TOKEN"),
        SecretValue::new("ghp_fake"),
    )]));

    let ctx = KernelContext {
        run_id: RunId::new("r1"),
        budgets: Budgets::default(),
        verify: VerifyConfig::default(),
        prompts: PromptsConfig::default(),
        workspace,
        sandbox: sandbox.clone(),
        agent: Arc::new(ScriptedAgent),
        events: sink.clone(),
        notifier: notifier.clone(),
        secrets: Arc::new(secrets),
    };

    // Held as `dyn Kernel`: the registry that maps flow names to
    // kernels needs the seam to be dyn-compatible.
    let kernel: Arc<dyn Kernel> = Arc::new(OneIterationKernel);
    let outcome = kernel.run(ctx).await.unwrap();

    assert_eq!(outcome, RunOutcome::Paused(PauseReason::AwaitingHuman));

    let events = sink.events.lock().unwrap();
    // Kinds come off the wire tag, so they cannot drift from the serde
    // mapping `proto` owns.
    let kinds: Vec<String> = events
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

    let notifications = notifier.notifications.lock().unwrap();
    assert_eq!(notifications.len(), 1);
    assert_eq!(notifications[0].reason, PauseReason::AwaitingHuman);
    assert_eq!(notifications[0].run_id, RunId::new("r1"));

    // Every sandbox the kernel booted was also torn down.
    assert_eq!(
        sandbox.created.load(Ordering::SeqCst),
        sandbox.destroyed.load(Ordering::SeqCst)
    );
}
