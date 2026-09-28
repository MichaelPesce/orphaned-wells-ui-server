# Terraform CI and backend deployment

`ENABLE_TERRAFORM_CI` is off unless its repository-variable value is exactly
`true`. With the flag off, the existing GKE workflows continue to use
`K8S_DEPLOY_TARGETS`. With it on, Terraform uses WIF and every updated GKE
workflow reads `kubernetes_deploy_targets` from the existing remote workspace.
`DEPLOYMENT_SERVICE_KEY_JSON` remains the deployment credential in phase one.

For the ordered first rollout and live test commands, use the
[rollout checklist](ROLLOUT.md). Manual operations are documented in the
[Terraform guide](../terraform/README.md#terraform-commands) and remain supported.

## Workflow sequence

- `terraform-checks.yml` checks PR diffs against `main`. Terraform changes run
  formatting and backend-free validation. Kubernetes changes render and check
  the workload manifest, including its GKE resources, without cloud credentials.
- `terraform-plan.yml` is a reusable workflow called at `@main`. Trusted
  same-repository PRs receive a speculative cloud plan in the workflow summary.
  Fork PRs and other untrusted PRs receive static checks only. To cloud-plan a
  fork contribution, a maintainer first reviews it and opens a PR from a trusted
  repository branch. Never run unreviewed PR code using `pull_request_target`.
- `deploy-k8s-staging.yml` coordinates every `main` push and manual dispatch.
  It builds the commit's image and calls `terraform-apply.yml`. The latter checks
  the last successful apply against the tracked Terraform inputs and the remote
  state object's generation. When they match, no plan or apply is needed.
- When reconciliation is needed, CI saves a fresh plan for that merged commit
  and publishes its readable summary. The `terraform-apply` Environment holds
  the apply job for approval. Apply downloads and verifies that exact saved plan,
  authenticates afresh, and refuses inputs superseded by a newer `main` commit.
- Only successful reconciliation releases staging deployment. Deployments read
  live targets and preserve the existing immutable image-tag behavior.

The apply job and every updated deployment share a concurrency group per
workspace with cancellation disabled. The staging orchestrator has a separate
group, avoiding nested-workflow deadlocks. GitHub may replace a pending run with
a newer one; comparing against the last applied inputs, rather than only the
latest push diff, means a later backend-only commit still reconciles any pending
infrastructure. Failed or rejected applies never release staging deployment.

Collaborator workflows retain their existing branch-promotion behavior. Their
shared deploy workflow checks the current `main` infrastructure revision before
mutation. If it is pending or failed, deployment stops with a retry instruction;
it does not silently fall back to the secret. Retry after the gated apply succeeds.
No application version is automatically promoted to collaborator environments
just because shared infrastructure changed.

## One-time setup

1. Confirm `backend.tf` points to the existing state and confirm the workspace
   (`ogrre` in the current operator documentation). Complete existing-resource
   imports first. CI never creates a replacement workspace or migrates state.
2. Reproduce production inputs from tracked `variables.tf` defaults and modules.
   Required local `terraform.tfvars` overrides must be incorporated into reviewed
   non-secret configuration before enabling CI. CI does not load a developer's
   local overrides. Keep desired shared infrastructure configuration reviewed
   and committed to `main`, whether an operator or CI performs the apply.
3. Review and run the bootstrap script below using a human administrator. It
   creates WIF and CI accounts, grants permissions, and configures a **dedicated**
   private plan bucket. It does not enable workflows or apply application
   Terraform. The plan bucket's lifecycle deletes `plans/` objects after seven
   days; `status/` readiness records do not expire.

   ```bash
   export PROJECT_ID=tidy-outlet-412020
   export TF_WORKSPACE=ogrre
   export TF_CI_BUCKET=tidy-outlet-412020-ogrre-terraform-ci
   export DEPLOY_SERVICE_ACCOUNT=ogrre-deployment-ci@tidy-outlet-412020.iam.gserviceaccount.com
   bash deployment/ci/bootstrap_terraform_ci.sh
   ```

   Bootstrap requires IAM policy administration, service-account and WIF
   administration, service activation, and bucket administration. These are
   operator permissions, not additional permissions for the CI plan account.

4. Create the GitHub Environment **`terraform-apply`** before enabling CI:
   - Add the people/team authorized to approve infrastructure changes as required reviewers.
   - Restrict deployment branches to the **branch** `main`; do not allow a tag named `main`.
   - Disable administrator bypass. Enable prevention of self-review when the reviewer team permits it.
   - Protect `main` with reviewed changes, especially for `.github/` and `deployment/`.

   Merely referencing the Environment in YAML creates it without approval rules.
   CI checks for required reviewers before planning and again before apply
   authentication, and fails if they are absent. Check the branch and bypass
   settings explicitly; the Environment is the human approval boundary.

5. Add the repository variables printed by bootstrap:

   | Variable | Meaning |
   | --- | --- |
   | `TF_WIF_PROVIDER` | Full Google WIF provider resource name |
   | `TF_PLAN_SERVICE_ACCOUNT` | `github-terraform-plan` email |
   | `TF_APPLY_SERVICE_ACCOUNT` | `github-terraform-ci` email |
   | `TF_WORKSPACE` | Existing remote workspace |
   | `TF_CI_BUCKET` | Private CI bucket name, without `gs://` |
   | `ENABLE_TERRAFORM_CI` | Leave unset or `false` until rollout is ready |

   These identifiers are variables, not keys or secrets. For example:

   ```bash
   gh variable set TF_WORKSPACE --repo CATALOG-Historic-Records/orphaned-wells-ui-server --body ogrre
   ```

## Identities and artifact permissions

The WIF provider checks immutable repository/owner IDs and `job_workflow_ref`.
Only the trusted reusable PR-plan workflow from `main` can obtain the PR plan
identity, and that workflow checks the PR origin and author association before
starting its credentialed job. A PR's editable caller cannot replace those checks.
Only the main reconciliation workflow's environment-gated job can impersonate
the apply account.

- **Plan account:** Compute, GKE, DNS, project-service and bucket-metadata reads;
  state object reads; creation/deletion of this workspace's `.tflock` object.
  It cannot write state, change infrastructure, publish plans, or mark an apply successful.
- **Apply account:** `container.admin`, `compute.networkAdmin`, `dns.admin`,
  `storage.admin`, and `serviceusage.serviceUsageAdmin`, matching the current GKE
  stack. Bootstrap grants no account-management or WIF-administration roles.
  If legacy VM management is deliberately enabled later, review the additional
  instance and VM-service-account permissions before planning that change.
- **Main plan publisher:** direct federation, without account impersonation,
  can create objects under `plans/` only. PR identities cannot publish executable
  plans. Existing objects cannot be overwritten. Apply checks the plan checksum,
  commit, workspace, state path, and run/attempt before use.
- **Existing deploy account:** keeps its deployment key and workload permissions;
  bootstrap adds object reads on the state and CI buckets. No apply role is added.

State access exposes the complete Terraform state, not just outputs. Plan files
can also contain sensitive values. Binary plans and their metadata remain in the
private CI bucket; only Terraform's human-readable, sensitive-value-redacted
plan output is published in GitHub summaries. Do not upload raw state or binary
plans as public workflow artifacts.

Terraform uses `deployment/terraform/.terraform-version` (currently 1.13.5) and
the checked-in provider lockfile. Local operators must also install
`gke-gcloud-auth-plugin` (`gcloud components install gke-gcloud-auth-plugin`) and
authenticate with ADC. The Kubernetes provider invokes the plugin separately
for planning/applying instead of saving a short-lived token in the plan.

## Enable and verify

1. With the flag disabled, first land the reusable `terraform-plan.yml` alone
   on `main` through a bootstrap PR, then merge the full implementation. The
   checks reference this workflow at `@main`, which must exist even when their
   credentialed job is disabled. The reusable file has no automatic trigger.
2. Propagate the updated reusable deployment workflow to every enabled
   collaborator branch **before** turning the flag on. Old branch workflows do
   not understand the readiness gate. Keep automatic collaborator promotion paused
   during this rollout using their existing deployment enablement variables.
3. Complete bootstrap, variable setup, and Environment protection. Preserve the
   existing secrets. Allow time for GCP IAM propagation.
4. Set `ENABLE_TERRAFORM_CI=true` and manually dispatch **Deploy Staging Server to
   GKE** on `main`. The initial absent readiness record requires a reviewed apply,
   even if the plan has no resource changes. Inspect the complete plan before approval.
5. Verify the apply job waits for review, the apply and target read succeed, and
   staging rolls out the expected image. Test a Terraform PR and a Kubernetes-only
   PR, a rejected apply, a backend-only follow-up, and a collaborator deployment.
6. Restore collaborator promotion settings. Delete `K8S_DEPLOY_TARGETS` only after
   all active branches use the enabled path and the rollout is confirmed. Keep
   `DEPLOYMENT_SERVICE_KEY_JSON`; deployment WIF migration is phase two.

The existing GKE enablement variables still control automatic application
deployment. Infrastructure reconciliation is independently controlled by
`ENABLE_TERRAFORM_CI`, even when automatic staging application deployment is off.
Manual staging dispatch also reconciles infrastructure and always requires `main`.

## Retry and recovery

- **Apply rejected, failed, or interrupted:** no successful readiness record is
  published. A marker written just before mutation remains `applying` after
  failure, including failed forced applies of unchanged inputs. Rerun the entire
  staging workflow on current `main` to generate a new plan and approval.
- **Saved plan stale or expired:** run the entire workflow again. Do not rerun
  only the apply job: a new run attempt deliberately cannot reuse another
  attempt's approval artifact. Do not regenerate a plan inside an approved job.
- **Application rollout failed after apply:** the infrastructure success remains
  recorded. A new staging run skips Terraform unless inputs or state changed.
- **Drift or emergency local apply:** coordinate with CI so no operation runs
  concurrently. Dispatch staging with `force_terraform_plan=true` for a fresh
  drift check and gated reconciliation. Out-of-band state writes invalidate
  readiness through the GCS generation check. No automatic force-unlock or
  state migration is performed.
- **Revert infrastructure:** merge a reviewed revert and approve its new plan.
  Do not apply an old saved plan or merely switch application image tags.
- **Disable automation:** setting the flag to `false` restores the rollout
  fallback, including its dependency on a current `K8S_DEPLOY_TARGETS` secret.
  Coordinate the switch with running workflows, refresh/recreate that secret,
  and verify there is no pending or failed infrastructure apply first.

## Manual plan and apply

Manual planning and applying remain supported. Use the
[manual Terraform commands](../terraform/README.md#terraform-commands) for
authentication, workspace selection, plan review, and saved-plan apply.

Speculative local plans use Terraform's state lock and can coexist with CI.
For a manual **apply** while CI is enabled, reserve a maintenance window with
other operators and pause workflow entry points first. Do not toggle
`ENABLE_TERRAFORM_CI` off just to run Terraform locally; that would restore
secret-based deployment behavior.

From the backend repository root, save the IDs of currently active deployment
and Terraform-check workflows, then disable those entry points:

```bash
export OGRRE_REPO=CATALOG-Historic-Records/orphaned-wells-ui-server
OGRRE_PAUSE_DIR="$(mktemp -d)"
gh workflow list --repo "$OGRRE_REPO" --all --limit 100 --json id,path,state \
  > "$OGRRE_PAUSE_DIR/workflows.json"
jq -r '.[] | select(.state == "active") |
  select((.path | test("/deploy-k8s-.*\\.yml$")) or
         (.path == ".github/workflows/terraform-checks.yml")) | .id' \
  "$OGRRE_PAUSE_DIR/workflows.json" > "$OGRRE_PAUSE_DIR/paused-ids.txt"
while IFS= read -r workflow_id; do
  gh workflow disable "$workflow_id" --repo "$OGRRE_REPO" || break
done < "$OGRRE_PAUSE_DIR/paused-ids.txt"
```

Check that every listed workflow is disabled before continuing. Disabling a
workflow does **not** stop an existing run. Inspect all incomplete runs:

```bash
gh run list --repo "$OGRRE_REPO" --limit 100 \
  --json databaseId,workflowName,status,url \
  --jq '.[] | select(.status != "completed")'
```

Let running applies finish; reject approval-waiting deployment runs and cancel
queued deployment runs as appropriate. Check Actions for older pending runs if
the first 100 results are insufficient. Proceed only when none of the paused
workflows has an active, queued, or approval-waiting run, and other operators
have agreed not to apply concurrently. Keep the pause-directory path available
in the shell while following the manual Terraform commands.

After the manual apply, enable **only** the staging coordinator first and run a
new full reconciliation on `main`:

```bash
gh workflow enable deploy-k8s-staging.yml --repo "$OGRRE_REPO"
gh workflow run deploy-k8s-staging.yml --repo "$OGRRE_REPO" --ref main \
  -f force_terraform_plan=true
```

Review and approve the fresh plan. This also builds and deploys current `main`
to staging. Local state writes change its GCS generation, so CI's old readiness
record deliberately fails until reconciliation succeeds. Investigate any
unexpected changes, especially uncommitted local overrides, rather than letting
CI silently undo them.

Once staging reconciliation succeeds, re-enable only the workflows that were
active before the maintenance window:

```bash
while IFS= read -r workflow_id; do
  gh workflow enable "$workflow_id" --repo "$OGRRE_REPO" || break
done < "$OGRRE_PAUSE_DIR/paused-ids.txt"
```

Verify each saved workflow is active. If staging was disabled before the
maintenance window, disable it again after reconciliation to restore that
setting too. Do not delete the pause record until restoration is verified.
If CI has never been enabled, use the manual Terraform path and refresh the
`K8S_DEPLOY_TARGETS` fallback secret before deployment; no CI readiness record
is required in that mode.

## Local checks

```bash
python -m pytest ogrre/tests/test_terraform_ci.py ogrre/tests/test_deployment_resources.py -q
python -m py_compile deployment/ci/*.py ogrre/tests/test_terraform_ci.py ogrre/tests/test_deployment_resources.py
python deployment/ci/validate_manifest.py
shellcheck deployment/ci/bootstrap_terraform_ci.sh
actionlint .github/workflows/terraform-*.yml .github/workflows/deploy-k8s-*.yml
terraform -chdir=deployment/terraform fmt -check -recursive
```

Use a clean temporary copy and `terraform init -backend=false -lockfile=readonly`
followed by `terraform validate` for provider-backed configuration validation
without contacting production state. Cloud IAM, Environment approvals, token
refresh, and real GKE rollout still require the staged integration checks above.
