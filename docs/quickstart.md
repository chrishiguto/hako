# Quickstart: first Verified Done run

This guide takes a real flow through `hako run` to Verified Done: a real
daemon, smolvm microVMs, Claude Code, a clone-mode Workspace, Verify Checks,
an unrefuted Skeptic Iteration, and a checkpoint commit on the run branch.

The production-shaped path is a daemon on a Linux host reachable only over
your tailnet. The CLI is a thin Client: it submits the flow and follows the
Event Log, while every Run executes on the daemon host. The local path at the
end is the development variant.

## 1. Prepare the daemon host

The host needs Linux, hardware virtualization, Docker or Podman for baking an
image, and the Rust toolchain pinned by this repository. Install smolvm 1.6.3
using its upstream instructions, then verify the load-bearing prerequisites:

```sh
test -r /dev/kvm && test -w /dev/kvm
smolvm --version
```

The second command must print `smolvm 1.6.3`. `hakod` performs the same pin
check before binding its listener and refuses any other version.

Build the two hako binaries from a clean clone:

```sh
just check
just test
cargo build --release --workspace --bins --locked
sudo install -m 755 target/release/hako target/release/hakod /usr/local/bin/
```

Give the daemon its own Unix identity and KVM access. The separate identity is
what lets its Secret Store remain unreadable to interactive users on the host.

```sh
sudo useradd --system --user-group --create-home --home-dir /var/lib/hako hako
sudo usermod --append --groups kvm hako
sudo install -d -m 700 -o hako -g hako \
  /var/lib/hako/runs /var/lib/hako/secrets /var/lib/hako/images
sudo -u hako smolvm --version
```

If the `hako` user already exists, keep it and only create the directories.

## 2. Bake an agent image

Toolchains belong in the image because each Stage receives a fresh Sandbox.
The image must contain Git, the selected Agent CLI, `gh` if delivery prompts
use it, and every tool invoked by the repository's Verify Checks.

This tested Alpine example supplies Claude Code and the tools needed by the
smoke. Extend its `apk add` list with project toolchains before running real
projects.

```dockerfile
FROM alpine:3.22

ARG AGENT_UID
ARG CLAUDE_VERSION=2.1.231

RUN apk add --no-cache \
    bash \
    ca-certificates \
    curl \
    git \
    github-cli \
    libgcc \
    libstdc++ \
    ripgrep

WORKDIR /tmp/claude-install
RUN curl -fsSL https://claude.ai/install.sh | bash -s "${CLAUDE_VERSION}" \
    && cp -L /root/.local/bin/claude /usr/local/bin/claude \
    && claude --version

RUN adduser -D -u "${AGENT_UID}" agent
USER agent
WORKDIR /workspace
```

Build the image with the daemon user's numeric UID. Workspace volume mounts
preserve numeric ownership; matching the UID lets the unprivileged Agent write
the daemon-owned clone. Claude Code rejects `--dangerously-skip-permissions`
when invoked as root, so `USER agent` is required rather than cosmetic.

```sh
mkdir -p hako-agent-image
$EDITOR hako-agent-image/Dockerfile
docker build \
  --build-arg "AGENT_UID=$(id -u hako)" \
  --tag hako-agent:smoke \
  hako-agent-image
docker save hako-agent:smoke | \
  sudo -u hako tee /var/lib/hako/images/hako-agent.tar >/dev/null
```

Probe the exact archive before giving it to the daemon:

```sh
sudo -u hako install -d -m 700 /var/lib/hako/image-probe
trap 'sudo -u hako smolvm machine delete --name hako-image-probe --force' EXIT
sudo -u hako smolvm machine create \
  --name hako-image-probe \
  --image /var/lib/hako/images/hako-agent.tar \
  --net \
  --volume /var/lib/hako/image-probe:/workspace
sudo -u hako smolvm machine start --name hako-image-probe
sudo -u hako smolvm machine exec \
  --name hako-image-probe \
  --stream \
  --workdir /workspace \
  -- sh -lc '
    test "$(id -u)" != 0
    claude --version
    git --version
    gh --version
    touch /workspace/guest-can-write
    test "$(curl -sS -o /dev/null -w "%{http_code}" https://api.anthropic.com/)" != 000
  '
sudo -u hako test -f /var/lib/hako/image-probe/guest-can-write
sudo -u hako smolvm machine delete --name hako-image-probe --force
trap - EXIT
```

The trap deletes the named probe when any command fails. The e2e test also
checks that hako's own ephemeral machines do not leak.

## 3. Provision secrets

Generate a long-lived Claude subscription token with `claude setup-token`, or
use an Anthropic API key. Enter the value through hidden terminal input; never
put it in a flow, command argument, repository, or this document.

```sh
read -rsp 'Claude OAuth token: ' HAKO_CLAUDE_SECRET
echo
printf '%s' "$HAKO_CLAUDE_SECRET" | sudo install \
  -m 600 -o hako -g hako /dev/stdin \
  /var/lib/hako/secrets/CLAUDE_CODE_OAUTH_TOKEN
unset HAKO_CLAUDE_SECRET
```

For an API key, write `ANTHROPIC_API_KEY` instead. The flow does not need to
list either name: the Claude Agent Adapter declares the alternatives and the
daemon resolves one before accepting the Run.

Create a daemon bearer token and keep the environment file root-only:

```sh
sudo install -d -m 700 -o root -g root /etc/hako
HAKO_REMOTE_TOKEN=$(openssl rand -hex 32)
TAILSCALE_IPV4=$(tailscale ip -4 | head -n 1)
sudo install -m 600 -o root -g root /dev/null /etc/hako/hakod.env
sudo tee /etc/hako/hakod.env >/dev/null <<EOF
HAKO_ADDR=${TAILSCALE_IPV4}:7878
HAKO_TOKEN=${HAKO_REMOTE_TOKEN}
HAKO_RUNS_DIR=/var/lib/hako/runs
HAKO_SECRETS_DIR=/var/lib/hako/secrets
HAKO_VM_IMAGE=/var/lib/hako/images/hako-agent.tar
HAKO_VM_NET=1
EOF
unset TAILSCALE_IPV4
```

Transfer `HAKO_REMOTE_TOKEN` to the Client through your password manager, then
unset it on the host. Configure tailnet grants or ACLs so only intended Client
identities can reach TCP port 7878. Binding the tailnet address keeps the
daemon off public and LAN interfaces; the bearer token remains required on
every endpoint.

## 4. Run the remote daemon

Install this unit as `/etc/systemd/system/hakod.service`:

```ini
[Unit]
Description=hako agent-loop daemon
After=network-online.target tailscaled.service
Wants=network-online.target

[Service]
Type=simple
User=hako
Group=hako
SupplementaryGroups=kvm
EnvironmentFile=/etc/hako/hakod.env
ExecStart=/usr/local/bin/hakod
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Start it and confirm that preflight reaches the listener:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now hakod
sudo systemctl status hakod
```

## 5. Submit the smoke from the thin Client

Create the toy source repository on the daemon host. Its `PROMPT.md` is the
objective; flows deliberately have no `goal` key.

```sh
sudo -u hako install -d /var/lib/hako/smoke-source
sudo install -m 644 -o hako -g hako \
  crates/server/tests/fixtures/e2e/PROMPT.md \
  /var/lib/hako/smoke-source/PROMPT.md
sudo -u hako git -C /var/lib/hako/smoke-source init --initial-branch=main
sudo -u hako git -C /var/lib/hako/smoke-source add PROMPT.md
sudo -u hako git -C /var/lib/hako/smoke-source \
  -c user.name=hako-smoke \
  -c user.email=hako-smoke@localhost \
  commit -m 'seed smoke objective'
```

On the Client, install only the `hako` binary and create `smoke.toml`:

```toml
[loop]
kernel = "pipeline"

[prompts]
plan = "PROMPT.md"

[agent]
engine = "claude"

[budget]
max_iterations = 2
max_hours = 1
iteration_timeout = "15m"

[verify]
checks = [
  "printf 'hako end-to-end smoke passed\\n' | cmp -s - SMOKE_RESULT.txt",
  "git diff --check",
]
on_fail = { retries = 0, then = "fail" }

[workspace]
repo = "/var/lib/hako/smoke-source"
```

Point the thin Client at the daemon's Tailscale address, submit, and attach:

```sh
export HAKO_ADDR=http://DAEMON_TAILSCALE_IP:7878
read -rsp 'hako bearer token: ' HAKO_TOKEN
echo
export HAKO_TOKEN
HAKO_RUN_ID=$(hako run smoke.toml)
printf 'run %s submitted\n' "$HAKO_RUN_ID"
hako attach "$HAKO_RUN_ID"
```

The final event must be `state_changed` with `state: "done"`. The preceding
history must show passing `verify_check_finished` events, a
`workspace_checkpointed` event, and `skeptic_verdict` with `refuted: false`.

Inspect the durable result on the daemon host:

```sh
HAKO_RUN_ID=PASTE_THE_SUBMITTED_RUN_ID
sudo -u hako git -C "/var/lib/hako/runs/${HAKO_RUN_ID}/workspace" \
  branch --show-current
sudo -u hako git -C "/var/lib/hako/runs/${HAKO_RUN_ID}/workspace" \
  log --oneline --decorate
sudo -u hako cat "/var/lib/hako/runs/${HAKO_RUN_ID}/workspace/SMOKE_RESULT.txt"
```

The branch is `hako/<run-id>`, its history contains an iteration checkpoint,
and the original `/var/lib/hako/smoke-source` remains unchanged.

## Local development variant

The ignored e2e test automates the same tracer on one Linux development host.
It starts real `hakod` and `hako` binaries, creates a clone-mode source,
follows the Event Log through Verified Done, and checks checkpoints, source
isolation, secret redaction, and microVM teardown.

Export the explicit inputs and run it through the repository's command
surface:

```sh
export HAKO_E2E_IMAGE=/absolute/path/to/hako-agent.tar
export HAKO_E2E_SECRETS_DIR=/absolute/path/to/owner-only/secret-store
just e2e
```

`just test` compiles but skips the real-infrastructure test, so ordinary
development and CI require neither KVM nor paid credentials.
