# Deploy Slackbot

Watches for deployments and reports them to Slack — a durable port of
[DBOS's CI/CD Slackbot example](https://docs.dbos.dev/python/examples/deploy-tracker-slackbot).

## What it demonstrates

- **Exactly-once posting.** Every deployment gets a `notify_deployment` workflow keyed by a
  **deterministic id** derived from the deployment's own id (`DeploySlackbot.Workflows.workflow_id/1`,
  `"deploy-<deployment_id>"`). `Dbos.start/3` upserts `workflow_status` `ON CONFLICT
  (workflow_uuid) DO UPDATE`, and never restarts a row already `SUCCESS`/`ERROR` — so a second
  `Dbos.start` call for the same deployment id, whatever triggers it, collapses onto the one
  workflow already there. There is only ever one workflow, and therefore only ever one pair of
  Slack posts, per deployment — a crash between detecting the deployment and finishing its
  notification does not produce a second message when the workflow recovers.
- **Deduplication of repeated detections** is the same mechanism, not a second one: the
  scheduled poller re-lists every deployment the source currently knows about on every tick,
  with no cursor to track — starting a workflow for a deployment already seen is a no-op.
- Slack itself sits behind a behaviour (`DeploySlackbot.Slack`), so the sample runs with no
  Slack workspace.

## Polling vs. a webhook

This sample **polls** a source for new deployments (`poll_deployments`, a `Dbos` scheduled
workflow, `@every 30s`), rather than receiving a webhook. Reasons:

- No HTTP server, router, or public endpoint to stand up for a sample app to be useful —
  `mix deps.get && iex -S mix` is the whole story.
- The exactly-once property this sample exists to demonstrate is identical either way: it comes
  from the deterministic workflow id, not from how the deployment was detected.

**What a webhook would change**: a `Plug`/`Bandit` endpoint would receive each deployment event
directly instead of listing them, so detection latency drops from "up to one poll interval" to
"immediate," and the CI/CD system's own retry behavior (most webhook senders retry on a
non-2xx response) becomes the second source of duplicate deliveries alongside repeated
polling — both handled by the same deterministic-id workflow, so the receiving code barely
changes. The main new surface is the webhook endpoint itself: verifying the request's
signature, returning 200 promptly (durable work happens in the workflow, not the HTTP handler),
and turning the payload into the same deployment shape `poll_deployments` already produces.

## The deployment source and Slack client, and which ones run by default

| Behaviour | Implementation | Used when |
|---|---|---|
| `DeploySlackbot.DeploymentSource` | `DeploySlackbot.DeploymentSource.InMemory` | **Always, in this sample** — a fake CI/CD system (an `Agent`) seeded by hand. A real integration (a CI provider's deployments API, or a webhook receiver) would implement the same two callbacks. |
| `DeploySlackbot.Slack` | `DeploySlackbot.Slack.Logging` | **Default** — records posts and prints them; no Slack workspace needed. |
| `DeploySlackbot.Slack` | `DeploySlackbot.Slack.WebApi` | Only when you configure it and `SLACK_BOT_TOKEN` is set — posts for real via `chat.postMessage`. |

Which module and client `DeploySlackbot.Workflows` uses is read from `Application.get_env/3`
inside each step's body (never inside the workflow body itself — see `docs/determinism.md`),
defaulting to the logging fake:

```elixir
config :deploy_slackbot,
  slack_module: DeploySlackbot.Slack.WebApi,
  slack_client: DeploySlackbot.Slack.WebApi.token_from_env!(),
  slack_channel: "#deploys"
```

`DeploySlackbot.Slack.WebApi.token_from_env!/0` raises immediately if `SLACK_BOT_TOKEN` isn't
set, rather than failing later inside a workflow step.

## Running it

Needs a local Postgres reachable as the current OS user, no password:

```sh
createdb deploy_slackbot_dev
mix deps.get
iex -S mix
```

```elixir
deployment = %{id: "d-42", app: "billing-api", version: "v3.1.0", environment: "production"}
DeploySlackbot.DeploymentSource.InMemory.push_deployment(
  DeploySlackbot.DeploymentSource.InMemory,
  deployment
)
```

Within 30 seconds `poll_deployments` fires and you'll see two lines printed:

```
[slack:#deploys] Deploying billing-api v3.1.0 to production...
[slack:#deploys] Deployed billing-api v3.1.0 to production.
```

Or trigger it immediately instead of waiting for the schedule:

```elixir
{:ok, handle} = Dbos.start("poll_deployments", [System.os_time(:millisecond), nil])
Dbos.await(handle)
```

## What to kill mid-run to see recovery work

Push a deployment, trigger `poll_deployments`, and kill the node right after the first
("Deploying...") line prints but before the second — the workflow has checkpointed its first
`Dbos.step`, not its second. Restart with `iex -S mix`: `Dbos.Recovery` redispatches the
`PENDING` `notify_deployment` workflow, replays the first post from its checkpoint (it does
**not** print again), and only the second post actually runs.

## Tests

```sh
createdb deploy_slackbot_test   # once
mix test
```

`test/deploy_slackbot/notify_test.exs` covers both properties directly:

- Calling `Dbos.start("notify_deployment", ...)` twice for the same deployment id (simulating a
  repeated detection) asserts exactly one `{"Deploying...", "Deployed..."}` pair was posted.
- Starting a workflow, waiting until its first post is checkpointed, killing its process, and
  recovering asserts the completion post still only happens once — the checkpointed first post
  is never repeated.
