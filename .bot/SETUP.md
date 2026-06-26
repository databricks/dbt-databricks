# Engineer-bot + reviewer-bot — operator setup (dbt-databricks)

How to activate the four bot workflows added by this PR. Until every step is
done they stay **inert** (workflows present, but each run no-ops / can't auth).

The bots run the generic **`databricks-bot-engine`** (in
`databricks/databricks-bot-engine`, **private for now**). All four workflows are
**self-contained**: they `pip install` the engine (pinned `ENGINE_REF`) and run
it inline. dbt-databricks is **public**, so it can't reuse the engine's internal
composite actions / reusable workflows — the steps are inlined here.

## 1. Two GitHub Apps (the bot identities)

Create two org GitHub Apps and **install both on `databricks/dbt-databricks`**.
They only mint short-lived tokens (`actions/create-github-app-token`); no webhooks.

| App | Repository permissions | Used for |
|---|---|---|
| **engineer-bot** | Contents: **R/W**, Pull requests: **R/W**, Issues: **R/W**, Metadata: Read | push the fix branch, open the fix PR, comment outcome on the issue |
| **reviewer-bot** | Pull requests: **R/W**, Contents: **R/W**, Metadata: Read | post inline findings + thread replies, and **resolve threads** — `resolveReviewThread` is gated behind **Contents:write**, NOT Pull-requests:write (GitHub gotcha; needed by the reviewer *follow-up*) |

The bot login is assumed to be `peco-engineer-bot[bot]` (used for git identity +
loop-prevention). Adjust the workflows if your App's bot login differs.

## 2. Repository secrets (Settings → Secrets and variables → Actions)

| Secret | Value |
|---|---|
| `ENGINEER_BOT_APP_ID` / `ENGINEER_BOT_PRIVATE_KEY` | the engineer-bot App's id + private key |
| `REVIEW_BOT_APP_ID` / `REVIEW_BOT_PRIVATE_KEY` | the reviewer-bot App's id + private key |
| `BOT_ENGINE_PAT` | a **fine-grained PAT**, **Contents: Read-only**, scoped to **just `databricks/databricks-bot-engine`** (private for now). Set expiry + rotation. **Drop it once that repo is public** (the install goes anonymous). |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | the model-serving workspace. The endpoint is built as `https://$DATABRICKS_HOST/serving-endpoints/databricks-claude-opus-4-8/invocations` and `DATABRICKS_TOKEN` authenticates it (Bearer). **`DATABRICKS_HOST` must be a *bare* hostname** (e.g. `adb-<id>.<n>.azuredatabricks.net`) — **no `https://`, no trailing slash** — or you get a malformed `https://https://…` and every run fails at model-call time. |

No new model secret is introduced beyond `DATABRICKS_HOST` + `DATABRICKS_TOKEN`.

## 3. Two labels (`engineer-bot` and `review-bot`)

Create **both**, keep them **maintainer-only** (applying a label needs triage+).
Nothing the bots do happens on an unlabeled issue/PR.

- **`engineer-bot`** — on an **issue** → triggers the bug-fix bot (`engineer-bot.yml`
  opens a fix PR); on a **PR** → opts it into the engineer **follow-up**.
- **`review-bot`** — on a **PR** → the reviewer reviews it. `engineer-bot.yml`
  auto-applies `review-bot` to its fix PRs so the loop closes.

## 4. Runner + mirror access (dbt-databricks-specific)

These workflows run on the **`databricks-protected-runner-group`** runner group
(`runs-on: { group: databricks-protected-runner-group, labels: [linux-ubuntu-latest] }`)
— the same protected runner the internal bot flows use, NOT a GitHub-hosted
runner (an LLM run over untrusted issue/PR content holds the model + App tokens;
keep it off shared runners). Because that group is **egress-blocked from public
PyPI/npm**, the workflows route `pip` + `npm` through the org's internal **JFrog
mirror** via the local `./.github/actions/setup-jfrog` action (keyless OIDC).

**Two org-infra prerequisites a repo admin must arrange** (dbt-databricks is a
*public* repo, so neither is automatic):
1. **Runner group access** — grant `databricks-protected-runner-group` to
   `databricks/dbt-databricks`. Without it the jobs queue forever.
2. **JFrog OIDC trust** — the `github-actions` OIDC provider on
   `databricks.jfrog.io` must accept tokens from this repo; otherwise
   `setup-jfrog` fails with "Could not extract JFrog access token".

If either can't be arranged, the alternative is a self-hosted runner with direct
PyPI/npm egress (drop the `setup-jfrog` steps) — but that's a larger change.

## 5. Engine version

`ENGINE_REF` is pinned (lockstep) across all four workflows to
`8abd625accfbcf89ab444ffc18a7fbee68084ed9`. **This must be ≥ the SHA that
includes `databricks-bot-engine` PR #71** (configurable convention files via the
`REPO_RULES_FILES` env var) — all four bot workflows set
`REPO_RULES_FILES: AGENTS.md` in their run-step `env:`, and on an older engine
that var is ignored (the bots would look for a non-existent `CLAUDE.md` and
inject no conventions). Bump `ENGINE_REF` (all four, in lockstep) to adopt engine
changes; never use `@main`.
