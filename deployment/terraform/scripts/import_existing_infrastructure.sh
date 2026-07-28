#!/usr/bin/env bash

set -uo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARGET_WORKSPACE="${TARGET_WORKSPACE:-}"
VAR_FILE="${VAR_FILE:-terraform.tfvars}"
DRY_RUN=false
STRICT=false
BACKEND_VMS_ONLY=false
RESET_STATE=false
COLLABORATOR_FILTERS=()

PROJECT_ID=""
REGION="us-central1"
ENABLE_GKE="true"
MANAGE_PROJECT_SERVICES="true"
GKE_CLUSTER_NAME="uow-backend-gke"
GKE_LOCATION="us-central1"
CREATE_GKE_TEST_DNS_RECORDS="true"
GKE_REQUIRED_SERVICES=(
  "compute.googleapis.com"
  "container.googleapis.com"
  "dns.googleapis.com"
)

usage() {
  cat <<'EOF'
Import existing Google Cloud infrastructure into the selected Terraform workspace.

The import manifest is generated from this repo's Terraform configuration and
naming conventions. The script parses terraform.tfvars plus collaborator zones
from main.tf, then attempts to import each resource that the configuration
manages. It does not use terraform console, so manifest generation does not
depend on decoding existing Terraform state.

Resources already present in the target state are skipped. Failed imports are
reported and do not stop the script unless --strict is used.

Usage:
  bash scripts/import_existing_infrastructure.sh --target-workspace ogrre

Options:
  --target-workspace NAME    Workspace to import into. If omitted, uses the
                             currently selected workspace.
  --var-file PATH            Terraform variable file to parse.
                             Default: terraform.tfvars.
  --backend-vms-only         Import only module.backend_vms resources.
  --reset-state              Move the selected local workspace state file aside
                             before importing. This affects Terraform state
                             only; it does not change Google Cloud resources.
  --dry-run                  Print import actions without changing state.
  --strict                   Exit non-zero if any import fails.
  --help                     Show this help.

Optional positional args:
  collaborator names         Limit imports to resources for the named
                             collaborators. Root shared resources are included
                             only when no collaborator filter is provided.

Examples:
  bash scripts/import_existing_infrastructure.sh --target-workspace ogrre
  bash scripts/import_existing_infrastructure.sh --target-workspace ogrre --dry-run
  bash scripts/import_existing_infrastructure.sh --target-workspace ogrre --reset-state
  bash scripts/import_existing_infrastructure.sh --target-workspace ogrre --backend-vms-only staging
EOF
}

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s\n' "${value}"
}

normalize_bool() {
  local key="$1"
  local value
  value="$(printf '%s' "$2" | tr '[:upper:]' '[:lower:]')"

  case "${value}" in
    true|1|yes)
      printf 'true\n'
      ;;
    false|0|no)
      printf 'false\n'
      ;;
    *)
      fail "${key} must be true or false, got: $2"
      ;;
  esac
}

join_list() {
  local separator="$1"
  shift
  local output=""
  local item

  for item in "$@"; do
    if [[ -n "${output}" ]]; then
      output="${output}${separator}"
    fi
    output="${output}${item}"
  done

  printf '%s\n' "${output}"
}

tfvars_scalar() {
  local key="$1"
  local default_value="$2"
  local value

  value="$(
    awk -v key="${key}" '
      function trim_value(value) {
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
        return value
      }

      {
        line = $0
        sub(/[[:space:]]*#.*/, "", line)

        if (line ~ "^[[:space:]]*" key "[[:space:]]*=") {
          sub(/^[^=]*=[[:space:]]*/, "", line)
          line = trim_value(line)
          sub(/^"/, "", line)
          sub(/"$/, "", line)
          print line
          exit
        }
      }
    ' "${VAR_FILE}"
  )"

  if [[ -n "${value}" ]]; then
    printf '%s\n' "${value}"
  else
    printf '%s\n' "${default_value}"
  fi
}

parse_collaborators() {
  awk '
    function strip_comment(line) {
      sub(/[[:space:]]*#.*/, "", line)
      return line
    }

    function count_open_braces(line, tmp) {
      tmp = line
      return gsub(/\{/, "{", tmp)
    }

    function count_close_braces(line, tmp) {
      tmp = line
      return gsub(/\}/, "}", tmp)
    }

    BEGIN {
      in_collaborators = 0
      depth = 0
      current = ""
    }

    {
      line = strip_comment($0)

      if (!in_collaborators) {
        if (line ~ /^[[:space:]]*collaborators[[:space:]]*=[[:space:]]*{/) {
          in_collaborators = 1
          depth = 1
        }
        next
      }

      if (depth == 1 && line ~ /^[[:space:]]*"?[A-Za-z0-9_-]+"?[[:space:]]*=[[:space:]]*{/) {
        current = line
        sub(/^[[:space:]]*/, "", current)
        sub(/[[:space:]]*=.*/, "", current)
        gsub(/"/, "", current)
      } else if (depth == 2 && current != "" && line ~ /^[[:space:]]*zone[[:space:]]*=/) {
        zone = line
        sub(/^[^=]*=[[:space:]]*/, "", zone)
        sub(/^"/, "", zone)
        sub(/".*/, "", zone)
        gsub(/^[[:space:]]+|[[:space:]]+$/, "", zone)
        print current "\t" zone
      }

      depth += count_open_braces(line) - count_close_braces(line)

      if (depth == 1) {
        current = ""
      }

      if (depth <= 0) {
        exit
      }
    }
  ' main.tf
}

state_file_for_workspace() {
  if [[ "${CURRENT_WORKSPACE}" == "default" ]]; then
    printf '%s/terraform.tfstate\n' "${TERRAFORM_DIR}"
  else
    printf '%s/terraform.tfstate.d/%s/terraform.tfstate\n' "${TERRAFORM_DIR}" "${CURRENT_WORKSPACE}"
  fi
}

reset_workspace_state() {
  local state_file
  local backup_file
  local timestamp

  state_file="$(state_file_for_workspace)"
  timestamp="$(date +%Y%m%d%H%M%S)"
  backup_file="${state_file}.backup-before-import-${timestamp}"

  mkdir -p "$(dirname "${state_file}")"

  if [[ -f "${state_file}" ]]; then
    mv "${state_file}" "${backup_file}" || fail "Unable to move existing state file aside: ${state_file}"
    echo "Moved existing local workspace state to: ${backup_file}"
  else
    echo "No existing local workspace state file found for reset: ${state_file}"
  fi
}

add_manifest_entry() {
  local category="$1"
  local address="$2"
  local import_id="$3"

  printf '%s\t%s\t%s\n' "${category}" "${address}" "${import_id}" >>"${MANIFEST_FILE}"
}

build_manifest() {
  local collaborator
  local zone
  local service

  : >"${MANIFEST_FILE}"

  if [[ "${#COLLABORATOR_FILTERS[@]}" -eq 0 && "${BACKEND_VMS_ONLY}" != true ]]; then
    add_manifest_entry "firewall" "google_compute_firewall.backend_http_https" "projects/${PROJECT_ID}/global/firewalls/backend-http-https"
    add_manifest_entry "firewall" "google_compute_firewall.backend_ssh" "projects/${PROJECT_ID}/global/firewalls/backend-ssh"

    if [[ "${ENABLE_GKE}" == "true" && "${MANAGE_PROJECT_SERVICES}" == "true" ]]; then
      for service in "${GKE_REQUIRED_SERVICES[@]}"; do
        add_manifest_entry "gke-service" "google_project_service.gke_required[\"${service}\"]" "${PROJECT_ID}/${service}"
      done
    fi

    if [[ "${ENABLE_GKE}" == "true" ]]; then
      add_manifest_entry "gke-cluster" "google_container_cluster.backend[0]" "projects/${PROJECT_ID}/locations/${GKE_LOCATION}/clusters/${GKE_CLUSTER_NAME}"
    fi
  fi

  for i in "${!COLLABORATORS[@]}"; do
    collaborator="${COLLABORATORS[$i]}"
    zone="${COLLABORATOR_ZONES[$i]}"

    if [[ "${ENABLE_GKE}" == "true" && "${BACKEND_VMS_ONLY}" != true ]]; then
      add_manifest_entry "gke-backend" "google_compute_global_address.gke_backend[\"${collaborator}\"]" "projects/${PROJECT_ID}/global/addresses/${collaborator}-uow-gke-ip"

      if [[ "${CREATE_GKE_TEST_DNS_RECORDS}" == "true" ]]; then
        add_manifest_entry "gke-dns" "google_dns_record_set.gke_backend_test[\"${collaborator}\"]" "projects/${PROJECT_ID}/managedZones/uow-carbon-org/rrsets/${collaborator}-k8s-server.uow-carbon.org./A"
      fi
    fi

    add_manifest_entry "backend-vm" "module.backend_vms[\"${collaborator}\"].google_compute_address.ip" "projects/${PROJECT_ID}/regions/${REGION}/addresses/${collaborator}-static-ip-address"
    add_manifest_entry "backend-vm" "module.backend_vms[\"${collaborator}\"].google_compute_instance.vm" "projects/${PROJECT_ID}/zones/${zone}/instances/${collaborator}-uow-server"
    add_manifest_entry "backend-vm" "module.backend_vms[\"${collaborator}\"].google_dns_record_set.dns" "projects/${PROJECT_ID}/managedZones/uow-carbon-org/rrsets/${collaborator}-server.uow-carbon.org./A"
  done
}

address_is_in_target_state() {
  local address="$1"
  grep -Fxq -- "${address}" "${STATE_LIST_FILE}"
}

collaborator_is_requested() {
  local collaborator="$1"
  local requested

  if [[ "${#COLLABORATOR_FILTERS[@]}" -eq 0 ]]; then
    return 0
  fi

  for requested in "${COLLABORATOR_FILTERS[@]}"; do
    if [[ "${collaborator}" == "${requested}" ]]; then
      return 0
    fi
  done

  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-workspace)
      [[ $# -ge 2 ]] || fail "--target-workspace requires a value"
      TARGET_WORKSPACE="$2"
      shift 2
      ;;
    --var-file)
      [[ $# -ge 2 ]] || fail "--var-file requires a value"
      VAR_FILE="$2"
      shift 2
      ;;
    --backend-vms-only)
      BACKEND_VMS_ONLY=true
      shift
      ;;
    --reset-state)
      RESET_STATE=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --strict)
      STRICT=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      fail "Unknown option: $1"
      ;;
    *)
      COLLABORATOR_FILTERS+=("$1")
      shift
      ;;
  esac
done

while [[ $# -gt 0 ]]; do
  COLLABORATOR_FILTERS+=("$1")
  shift
done

require_command terraform
require_command awk
require_command grep

cd "${TERRAFORM_DIR}" || exit 1

[[ -f "${VAR_FILE}" ]] || fail "Terraform variable file not found: ${VAR_FILE}"
[[ -f "main.tf" ]] || fail "main.tf not found in ${TERRAFORM_DIR}"

unset GOOGLE_APPLICATION_CREDENTIALS
unset GOOGLE_AUTHORIZED_USER_CREDENTIALS
unset CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE

PROJECT_ID="$(tfvars_scalar "project_id" "${PROJECT_ID}")"
REGION="$(tfvars_scalar "region" "${REGION}")"
ENABLE_GKE="$(normalize_bool "enable_gke" "$(tfvars_scalar "enable_gke" "${ENABLE_GKE}")")"
MANAGE_PROJECT_SERVICES="$(normalize_bool "manage_project_services" "$(tfvars_scalar "manage_project_services" "${MANAGE_PROJECT_SERVICES}")")"
GKE_CLUSTER_NAME="$(tfvars_scalar "gke_cluster_name" "${GKE_CLUSTER_NAME}")"
GKE_LOCATION="$(tfvars_scalar "gke_location" "${GKE_LOCATION}")"
CREATE_GKE_TEST_DNS_RECORDS="$(normalize_bool "create_gke_test_dns_records" "$(tfvars_scalar "create_gke_test_dns_records" "${CREATE_GKE_TEST_DNS_RECORDS}")")"

[[ -n "${PROJECT_ID}" ]] || fail "project_id must be set in ${VAR_FILE}"

COLLABORATORS=()
COLLABORATOR_ZONES=()
while IFS=$'\t' read -r collaborator zone; do
  [[ -n "${collaborator}" && -n "${zone}" ]] || continue

  if collaborator_is_requested "${collaborator}"; then
    COLLABORATORS+=("${collaborator}")
    COLLABORATOR_ZONES+=("${zone}")
  fi
done < <(parse_collaborators)

if [[ "${#COLLABORATORS[@]}" -eq 0 ]]; then
  if [[ "${#COLLABORATOR_FILTERS[@]}" -gt 0 ]]; then
    fail "No matching collaborators found in main.tf for filter: ${COLLABORATOR_FILTERS[*]}"
  fi

  fail "No collaborators with zones were found in main.tf"
fi

STATE_LIST_FILE="$(mktemp)"
MANIFEST_FILE="$(mktemp)"
trap 'rm -f "${STATE_LIST_FILE}" "${MANIFEST_FILE}"' EXIT

build_manifest

if [[ -n "${TARGET_WORKSPACE}" ]]; then
  terraform workspace select "${TARGET_WORKSPACE}" >/dev/null || fail "Unable to select Terraform workspace: ${TARGET_WORKSPACE}"
fi

CURRENT_WORKSPACE="$(terraform workspace show)"

if [[ "${RESET_STATE}" == true ]]; then
  if [[ "${DRY_RUN}" == true ]]; then
    if [[ -f "$(state_file_for_workspace)" ]]; then
      echo "DRY RUN: would move selected workspace state file aside: $(state_file_for_workspace)"
    else
      echo "DRY RUN: no selected workspace state file exists to reset: $(state_file_for_workspace)"
    fi
  else
    reset_workspace_state
  fi
fi

IMPORT_ARGS=(-input=false -no-color)
IMPORT_ARGS+=("-var-file=${VAR_FILE}")

if [[ ! -f "$(state_file_for_workspace)" ]]; then
  : >"${STATE_LIST_FILE}"
elif ! terraform state list >"${STATE_LIST_FILE}" 2>/dev/null; then
  echo "WARN: could not read target workspace state list; assuming target state is empty"
  : >"${STATE_LIST_FILE}"
fi

attempted=0
imported=0
skipped=0
failed=0
matched=0
FAILED_IMPORTS=()

echo "Terraform target workspace: ${CURRENT_WORKSPACE}"
echo "Terraform variable file: ${VAR_FILE}"
echo "Import manifest source: parsed Terraform files and repo naming conventions"
echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Collaborators: $(join_list ", " "${COLLABORATORS[@]}")"
if [[ "${ENABLE_GKE}" == "true" ]]; then
  echo "GKE imports: enabled"
else
  echo "GKE imports: disabled by enable_gke=false"
fi
if [[ "${BACKEND_VMS_ONLY}" == true ]]; then
  echo "Filter: module.backend_vms resources only"
fi
if [[ "${#COLLABORATOR_FILTERS[@]}" -gt 0 ]]; then
  echo "Collaborator filter: $(join_list ", " "${COLLABORATOR_FILTERS[@]}")"
fi
if [[ "${RESET_STATE}" == true ]]; then
  echo "Reset state: enabled"
fi
if [[ "${DRY_RUN}" == true ]]; then
  echo "Mode: dry run"
fi
echo ""

while IFS=$'\t' read -r category address import_id; do
  if [[ -z "${category}" || -z "${address}" || -z "${import_id}" ]]; then
    continue
  fi

  matched=$((matched + 1))

  if address_is_in_target_state "${address}"; then
    echo "SKIP: ${address}"
    skipped=$((skipped + 1))
    continue
  fi

  attempted=$((attempted + 1))
  echo "IMPORT: ${address}"
  echo "        ${import_id}"

  if [[ "${DRY_RUN}" == true ]]; then
    continue
  fi

  if terraform import "${IMPORT_ARGS[@]}" "${address}" "${import_id}"; then
    imported=$((imported + 1))
    echo "${address}" >>"${STATE_LIST_FILE}"
  else
    failed=$((failed + 1))
    FAILED_IMPORTS+=("${address}|${import_id}")
    echo "WARN: import failed for ${address}"
  fi

  echo ""
done <"${MANIFEST_FILE}"

echo ""
echo "Import summary"
echo "  matched from config:      ${matched}"
echo "  attempted imports:        ${attempted}"
echo "  imported:                ${imported}"
echo "  already in target state: ${skipped}"
echo "  failed imports:           ${failed}"

if [[ "${#FAILED_IMPORTS[@]}" -gt 0 ]]; then
  echo ""
  echo "Resources that could not be imported:"
  for entry in "${FAILED_IMPORTS[@]}"; do
    address="${entry%%|*}"
    import_id="${entry#*|}"
    echo "  - ${address}"
    echo "    ${import_id}"
  done
fi

if [[ "${STRICT}" == true && "${failed}" -gt 0 ]]; then
  exit 1
fi

exit 0
