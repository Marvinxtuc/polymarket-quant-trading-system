#!/usr/bin/env bash

set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

read_dotenv_var() {
  local key="$1"
  local dotenv="$BASE/.env"
  [[ -f "$dotenv" ]] || return 0
  awk -F= -v key="$key" '
    $0 ~ "^[[:space:]]*" key "=" {
      sub(/^[[:space:]]*[^=]+=/, "", $0)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
      gsub(/^"|"$/, "", $0)
      gsub(/^'\''|'\''$/, "", $0)
      print $0
      exit
    }
  ' "$dotenv"
}

BLOCKBEATS_API_KEY="${BLOCKBEATS_API_KEY:-$(read_dotenv_var BLOCKBEATS_API_KEY)}"
BASE_URL="${BLOCKBEATS_BASE_URL:-$(read_dotenv_var BLOCKBEATS_BASE_URL)}"
BASE_URL="${BASE_URL:-https://api-pro.theblockbeats.info/v1}"
PUBLIC_BASE_URL="${BLOCKBEATS_PUBLIC_BASE_URL:-$(read_dotenv_var BLOCKBEATS_PUBLIC_BASE_URL)}"
PUBLIC_BASE_URL="${PUBLIC_BASE_URL:-https://api.theblockbeats.news/v1/open-api}"
FETCH_HELPER="$BASE/scripts/blockbeats_http_fetch.py"
PYTHON_FETCH_BIN="${BLOCKBEATS_PYTHON_BIN:-$BASE/.venv/bin/python}"
if [[ ! -x "${PYTHON_FETCH_BIN}" ]]; then
  PYTHON_FETCH_BIN="$(command -v python3 || true)"
fi
DEFAULT_LANG="${BLOCKBEATS_LANG:-$(read_dotenv_var BLOCKBEATS_LANG)}"
DEFAULT_LANG="${DEFAULT_LANG:-en}"
CONNECT_TIMEOUT_SECONDS="${BLOCKBEATS_CONNECT_TIMEOUT_SECONDS:-$(read_dotenv_var BLOCKBEATS_CONNECT_TIMEOUT_SECONDS)}"
CONNECT_TIMEOUT_SECONDS="${CONNECT_TIMEOUT_SECONDS:-5}"
MAX_TIME_SECONDS="${BLOCKBEATS_MAX_TIME_SECONDS:-$(read_dotenv_var BLOCKBEATS_MAX_TIME_SECONDS)}"
MAX_TIME_SECONDS="${MAX_TIME_SECONDS:-20}"
RETRY_COUNT="${BLOCKBEATS_RETRY_COUNT:-$(read_dotenv_var BLOCKBEATS_RETRY_COUNT)}"
RETRY_COUNT="${RETRY_COUNT:-0}"
DOH_URL="${BLOCKBEATS_DOH_URL:-$(read_dotenv_var BLOCKBEATS_DOH_URL)}"
DOH_URL="${DOH_URL:-https://1.1.1.1/dns-query}"
USE_DOH="${BLOCKBEATS_USE_DOH:-$(read_dotenv_var BLOCKBEATS_USE_DOH)}"
USE_DOH="${USE_DOH:-1}"
ALLOW_PUBLIC_FALLBACK="${BLOCKBEATS_ALLOW_PUBLIC_FALLBACK:-$(read_dotenv_var BLOCKBEATS_ALLOW_PUBLIC_FALLBACK)}"
ALLOW_PUBLIC_FALLBACK="${ALLOW_PUBLIC_FALLBACK:-1}"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/blockbeats_query.sh overview
  bash scripts/blockbeats_query.sh prediction [page] [size] [lang]
  bash scripts/blockbeats_query.sh search <keyword> [page] [size] [lang]
  bash scripts/blockbeats_query.sh newsflash <type> [page] [size] [lang]
  bash scripts/blockbeats_query.sh article <type> [page] [size] [lang]

Examples:
  bash scripts/blockbeats_query.sh overview
  bash scripts/blockbeats_query.sh prediction 1 10 en
  bash scripts/blockbeats_query.sh search "Trump tariffs" 1 8 en
  bash scripts/blockbeats_query.sh newsflash important 1 10 en
  bash scripts/blockbeats_query.sh article original 1 5 en

Notes:
  - Requires BLOCKBEATS_API_KEY in the environment or repo .env.
  - Uses bounded curl timeouts and retries with DNS-over-HTTPS when local DNS is unhealthy.
  - `prediction` can fall back to the public BlockBeats flash feed when the Pro API is unreachable.
  - Outputs pretty-printed JSON so the result can be reused in reports or LLM context.
EOF
}

require_api_key() {
  if [[ -z "${BLOCKBEATS_API_KEY:-}" ]]; then
    echo "BLOCKBEATS_API_KEY is required. Export it first or add it to ${BASE}/.env." >&2
    exit 1
  fi
}

pretty_print() {
  python3 -m json.tool
}

log_note() {
  printf '%s\n' "$*" >&2
}

has_python_fetch() {
  [[ -n "${PYTHON_FETCH_BIN:-}" ]] && [[ -x "${PYTHON_FETCH_BIN}" ]] && [[ -f "${FETCH_HELPER}" ]]
}

is_pro_url() {
  [[ "$1" == "${BASE_URL}"* ]]
}

python_fetch_to_file() {
  local url="$1"
  local output_path="$2"
  shift 2 || true

  if ! has_python_fetch; then
    return 127
  fi

  if [[ -n "${BLOCKBEATS_API_KEY:-}" ]]; then
    "${PYTHON_FETCH_BIN}" "${FETCH_HELPER}" \
      --url "${url}" \
      --output "${output_path}" \
      --timeout "${MAX_TIME_SECONDS}" \
      --api-key "${BLOCKBEATS_API_KEY}"
  else
    "${PYTHON_FETCH_BIN}" "${FETCH_HELPER}" \
      --url "${url}" \
      --output "${output_path}" \
      --timeout "${MAX_TIME_SECONDS}"
  fi
}

run_curl() {
  local url="$1"
  shift || true

  if [[ -n "${BLOCKBEATS_API_KEY:-}" ]]; then
    curl -fsS \
      --connect-timeout "${CONNECT_TIMEOUT_SECONDS}" \
      --max-time "${MAX_TIME_SECONDS}" \
      --retry "${RETRY_COUNT}" \
      --retry-all-errors \
      --retry-delay 1 \
      -H "accept: application/json" \
      -H "api-key: ${BLOCKBEATS_API_KEY}" \
      "$@" \
      "${url}"
  else
    curl -fsS \
      --connect-timeout "${CONNECT_TIMEOUT_SECONDS}" \
      --max-time "${MAX_TIME_SECONDS}" \
      --retry "${RETRY_COUNT}" \
      --retry-all-errors \
      --retry-delay 1 \
      -H "accept: application/json" \
      "$@" \
      "${url}"
  fi
}

fetch_to_file() {
  local url="$1"
  local output_path="$2"
  shift 2 || true

  if [[ "$#" -eq 0 ]] && is_pro_url "${url}" && has_python_fetch; then
    python_fetch_to_file "${url}" "${output_path}"
    return $?
  fi

  local err_path
  err_path="$(mktemp)"
  if run_curl "${url}" "$@" > "${output_path}" 2> "${err_path}"; then
    rm -f "${err_path}"
    return 0
  else
    local rc=$?
    cat "${err_path}" >&2
    rm -f "${err_path}"
    return "${rc}"
  fi
}

fetch_to_file_with_retries() {
  local url="$1"
  local output_path="$2"

  if fetch_to_file "${url}" "${output_path}"; then
    return 0
  else
    local rc=$?
    if [[ "${USE_DOH}" == "1" ]]; then
      log_note "Primary request failed; retrying via DNS-over-HTTPS (${DOH_URL})."
      if fetch_to_file "${url}" "${output_path}" --doh-url "${DOH_URL}"; then
        return 0
      else
        rc=$?
      fi
    fi

    return "${rc}"
  fi
}

pretty_print_url() {
  local url="$1"
  local tmp_path
  tmp_path="$(mktemp)"
  if fetch_to_file_with_retries "${url}" "${tmp_path}"; then
    pretty_print < "${tmp_path}"
    rm -f "${tmp_path}"
    return 0
  else
    local rc=$?
    rm -f "${tmp_path}"
    return "${rc}"
  fi
}

pretty_print_url_via_doh() {
  local url="$1"
  local tmp_path
  tmp_path="$(mktemp)"
  if fetch_to_file "${url}" "${tmp_path}" --doh-url "${DOH_URL}"; then
    pretty_print < "${tmp_path}"
    rm -f "${tmp_path}"
    return 0
  else
    local rc=$?
    rm -f "${tmp_path}"
    return "${rc}"
  fi
}

run_public_prediction_fallback() {
  local page="${1:-1}"
  local size="${2:-10}"
  local lang="${3:-${DEFAULT_LANG}}"
  log_note "BlockBeats Pro prediction feed unavailable; falling back to public flash feed."
  pretty_print_url_via_doh "${PUBLIC_BASE_URL}/open-flash?page=${page}&size=${size}&type=push&lang=${lang}"
}

newsflash_path() {
  case "${1}" in
    latest) echo "newsflash" ;;
    24h|important|original|first|macro|onchain|financing|ai|prediction) echo "newsflash/${1}" ;;
    category/*) echo "newsflash/${1}" ;;
    *)
      echo "Unsupported newsflash type: ${1}" >&2
      exit 1
      ;;
  esac
}

article_path() {
  case "${1}" in
    latest) echo "article" ;;
    important|original) echo "article/${1}" ;;
    *)
      echo "Unsupported article type: ${1}" >&2
      exit 1
      ;;
  esac
}

run_overview() {
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "${tmp_dir}"' EXIT

  fetch_to_file_with_retries "${BASE_URL}/data/bottom_top_indicator" "${tmp_dir}/sentiment.json"
  fetch_to_file_with_retries "${BASE_URL}/newsflash/important?page=1&size=10&lang=${DEFAULT_LANG}" "${tmp_dir}/important_news.json"
  fetch_to_file_with_retries "${BASE_URL}/data/btc_etf" "${tmp_dir}/btc_etf.json"
  fetch_to_file_with_retries "${BASE_URL}/data/daily_tx" "${tmp_dir}/daily_tx.json"

  python3 - "${tmp_dir}" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
payload = {}
for name in ("sentiment", "important_news", "btc_etf", "daily_tx"):
    payload[name] = json.loads((root / f"{name}.json").read_text())
print(json.dumps(payload, ensure_ascii=False, indent=2))
PY
}

run_prediction() {
  local page="${1:-1}"
  local size="${2:-10}"
  local lang="${3:-${DEFAULT_LANG}}"
  if pretty_print_url "${BASE_URL}/newsflash/prediction?page=${page}&size=${size}&lang=${lang}"; then
    return 0
  else
    local rc=$?
    if [[ "${ALLOW_PUBLIC_FALLBACK}" == "1" ]]; then
      run_public_prediction_fallback "${page}" "${size}" "${lang}"
      return $?
    fi

    return "${rc}"
  fi
}

run_search() {
  local keyword="${1:-}"
  local page="${2:-1}"
  local size="${3:-10}"
  local lang="${4:-${DEFAULT_LANG}}"

  if [[ -z "${keyword}" ]]; then
    echo "search requires a keyword" >&2
    exit 1
  fi

  local encoded_keyword
  encoded_keyword="$("${PYTHON_FETCH_BIN}" - <<'PY' "${keyword}"
import sys
import urllib.parse
print(urllib.parse.quote(sys.argv[1], safe=""))
PY
)"

  pretty_print_url "${BASE_URL}/search?keyword=${encoded_keyword}&page=${page}&size=${size}&lang=${lang}"
}

run_newsflash() {
  local type="${1:-latest}"
  local page="${2:-1}"
  local size="${3:-10}"
  local lang="${4:-${DEFAULT_LANG}}"
  local path
  path="$(newsflash_path "${type}")"
  pretty_print_url "${BASE_URL}/${path}?page=${page}&size=${size}&lang=${lang}"
}

run_article() {
  local type="${1:-latest}"
  local page="${2:-1}"
  local size="${3:-10}"
  local lang="${4:-${DEFAULT_LANG}}"
  local path
  path="$(article_path "${type}")"
  pretty_print_url "${BASE_URL}/${path}?page=${page}&size=${size}&lang=${lang}"
}

main() {
  local command="${1:-}"
  shift || true

  if [[ -z "${command}" ]]; then
    usage
    exit 1
  fi

  case "${command}" in
    prediction)
      if [[ -z "${BLOCKBEATS_API_KEY:-}" ]] && [[ "${ALLOW_PUBLIC_FALLBACK}" == "1" ]]; then
        log_note "BLOCKBEATS_API_KEY not set; using public flash fallback for prediction."
      else
        require_api_key
      fi
      run_prediction "$@"
      ;;
    overview)
      require_api_key
      run_overview "$@"
      ;;
    search)
      require_api_key
      run_search "$@"
      ;;
    newsflash)
      require_api_key
      run_newsflash "$@"
      ;;
    article)
      require_api_key
      run_article "$@"
      ;;
    help|-h|--help)
      usage
      ;;
    *)
      echo "Unknown command: ${command}" >&2
      usage
      exit 1
      ;;
  esac
}

main "$@"
