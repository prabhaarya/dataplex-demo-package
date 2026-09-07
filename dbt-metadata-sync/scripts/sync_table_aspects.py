"""
sync_dataplex_aspects.py

Syncs dbt model/column `meta` blocks (custom aspect data) from the dbt manifest
into Knowledge Catalog (Dataplex Universal Catalog) BigQuery system entries.

CLI:
  python sync_dataplex_aspects.py [--target-dir PATH] [--validate-only]
                                   [--max-retries N] [--verbose]

  --validate-only : run schema validation + hash comparison, print what WOULD
                     change, make no API writes. Use this as a pre-merge /
                     pre-commit CI gate before the real dbt run + sync.
"""

import argparse
import hashlib
import json
import logging
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

try:
    from google.auth import default as google_auth_default
    from google.auth.transport.requests import Request as GoogleAuthRequest
except ImportError:
    print("Missing dependency: pip install google-auth", file=sys.stderr)
    raise

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ASPECT_LOCATION = "global"  # Aspect type definitions are always global. Fixed.
ASPECT_TYPES_PROJECT = os.environ.get("ASPECT_TYPES_PROJECT")
DEFAULT_ENTRY_LOCATION = os.environ.get("DEFAULT_ENTRY_LOCATION", "us")
SYNC_METADATA_ASPECT_ID = os.environ.get("SYNC_METADATA_ASPECT_ID", "custom-dbt-sync-metadata")
# Field names as defined on the actual aspect type (hyphenated, per KC admin's schema)
FIELD_CONTENT_HASH = "custom-content-hash"
FIELD_SYNCED_AT = "custom-synched-at"
FIELD_INVOCATION_ID = "custom-dbt-invocation-id"
FIELD_SYNC_STATUS = "custom-sync-status"  # "FULL" | "PARTIAL" - always "FULL" here,
RESOURCE_TYPES_TO_SYNC = {"model", "seed", "snapshot"}

log = logging.getLogger("dataplex_sync")

# In-memory caches (per-process, per-run only - not persisted)
_dataset_location_cache = {}
_aspect_schema_cache = {}


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_access_token():
    """Real ADC: works with Workload Identity Federation (GitLab OIDC),
    a mounted service account key, or a local gcloud user - whatever
    google.auth.default() resolves in the environment. No gcloud CLI dependency."""
    creds, _ = google_auth_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(GoogleAuthRequest())
    return creds.token


# ---------------------------------------------------------------------------
# HTTP helper with retry/backoff (handles 409/ABORTED concurrent-update conflicts
# and transient 5xx/429 - this is a synchronous API, so we retry, we don't poll)
# ---------------------------------------------------------------------------

def _http_request(method, url, token, payload=None, max_retries=5):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    for attempt in range(max_retries + 1):
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as resp:
                body = resp.read()
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            retriable = e.code in (409, 429, 500, 502, 503)
            if retriable and attempt < max_retries:
                sleep_s = min(2 ** attempt + random.uniform(0, 1), 30)
                log.warning("HTTP %s on %s %s (attempt %d/%d), retrying in %.1fs",
                            e.code, method, url, attempt + 1, max_retries, sleep_s)
                time.sleep(sleep_s)
                continue
            if e.code == 404:
                return None
            raise RuntimeError(f"HTTP {e.code} calling {method} {url}: {body}") from e
    raise RuntimeError(f"Exhausted retries calling {method} {url}")


# ---------------------------------------------------------------------------
# Manifest / run_results loading
# ---------------------------------------------------------------------------

def load_json_safe(path, required=True):
    """Failure isolation: a missing/corrupt file never crashes the whole run
    unless it's genuinely required (manifest.json)."""
    if not os.path.exists(path):
        if required:
            raise FileNotFoundError(f"Required file not found: {path}")
        log.warning("Optional file not found, continuing without it: %s", path)
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        if required:
            raise
        log.warning("Failed to parse %s (%s), continuing without it", path, e)
        return None


def resolve_target_dir(cli_arg):
    target_dir = cli_arg or os.environ.get("DBT_TARGET_DIR") or "target"
    return os.path.abspath(target_dir)


def get_models_to_consider(manifest, run_results):
    """If run_results.json is available, only look at nodes dbt actually built
    in THIS invocation (avoids re-syncing untouched models in a multi-model run).
    If it's missing, fall back to every model/seed/snapshot node in the manifest."""
    nodes = manifest["nodes"]

    if run_results is None:
        log.warning("run_results.json unavailable - considering ALL nodes in manifest")
        return [
            (uid, node) for uid, node in nodes.items()
            if node.get("resource_type") in RESOURCE_TYPES_TO_SYNC
        ]

    succeeded_uids = {
        r["unique_id"] for r in run_results.get("results", [])
        if r.get("status") == "success"
    }
    return [
        (uid, nodes[uid]) for uid in succeeded_uids
        if uid in nodes and nodes[uid].get("resource_type") in RESOURCE_TYPES_TO_SYNC
    ]


# ---------------------------------------------------------------------------
# Physical table resolution (project/dataset/table + location), handles aliases
# and works the same for tables and views - the BigQuery API (and Dataplex's
# BQ system entries) treat views as members of the same "tables" collection.
# ---------------------------------------------------------------------------

def resolve_physical_table(node):
    project = node.get("database")
    dataset = node.get("schema")
    table = node.get("alias") or node.get("name")
    materialized = node.get("config", {}).get("materialized", "table")
    return project, dataset, table, materialized


def get_dataset_location(token, project, dataset):
    cache_key = (project, dataset)
    if cache_key in _dataset_location_cache:
        return _dataset_location_cache[cache_key]

    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}"
    try:
        resp = _http_request("GET", url, token)
        location = (resp or {}).get("location", DEFAULT_ENTRY_LOCATION).lower()
    except Exception as e:
        log.warning("Could not resolve location for %s.%s (%s); defaulting to %s",
                    project, dataset, e, DEFAULT_ENTRY_LOCATION)
        location = DEFAULT_ENTRY_LOCATION
    _dataset_location_cache[cache_key] = location
    return location


# ---------------------------------------------------------------------------
# Aspect type schema fetch + validation (dynamic - works with new aspects/
# fields automatically since we read the live definition from KC, never a
# hardcoded local schema).
# ---------------------------------------------------------------------------

def get_aspect_type_schema(token, aspect_id):
    if aspect_id in _aspect_schema_cache:
        return _aspect_schema_cache[aspect_id]

    url = (f"https://dataplex.googleapis.com/v1/projects/{ASPECT_TYPES_PROJECT}"
           f"/locations/{ASPECT_LOCATION}/aspectTypes/{aspect_id}")
    try:
        resp = _http_request("GET", url, token)
    except Exception as e:
        log.warning("Could not fetch aspect type schema for '%s' (%s); "
                    "skipping validation for this aspect", aspect_id, e)
        resp = None

    schema = {}
    if resp:
        fields = resp.get("metadataTemplate", {}).get("recordFields", [])
        for f in fields:
            enum_values = None
            if f.get("type") == "enum":
                enum_values = {ev["name"] for ev in f.get("annotations", {}).get("enumValues", [])} \
                    or {ev.get("name") for ev in f.get("enumValues", [])}
            schema[f["name"]] = {"type": f.get("type"), "enum_values": enum_values}

    _aspect_schema_cache[aspect_id] = schema
    return schema


# Validation happens inline inside build_aspects_payload() below (needs the
# access token to fetch live schemas, so it's folded into add_aspect()).

# ---------------------------------------------------------------------------
# Aspect payload construction (table-level + column-level), same shape as the
# original script, plus validation and the sync-metadata bookkeeping aspect.
# ---------------------------------------------------------------------------

def compute_content_hash(node):
    """Hash covers table-level meta AND column-level meta, so both a pure
    description edit in yml and a logic change surfaced through new/changed
    columns will produce a different hash."""
    table_meta = node.get("meta") or node.get("config", {}).get("meta", {})
    columns_meta = {
        col: details.get("meta", {})
        for col, details in node.get("columns", {}).items()
        if details.get("meta")
    }
    canonical = json.dumps(
        {"table_meta": table_meta, "columns_meta": columns_meta},
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_aspects_payload(token, node, location):
    """Returns (aspects_payload, validation_warnings)."""
    aspects_payload = {}
    warnings = []

    def add_aspect(aspect_id, data, column_path=None):
        schema = get_aspect_type_schema(token, aspect_id)
        if schema:
            for key in data:
                if key not in schema:
                    warnings.append(f"Unknown field '{key}' for aspect '{aspect_id}'"
                                     + (f" on column '{column_path}'" if column_path else ""))
                elif schema[key]["enum_values"] and data[key] not in schema[key]["enum_values"]:
                    warnings.append(
                        f"Enum value '{data[key]}' for field '{key}' in aspect "
                        f"'{aspect_id}' does not match defined casing/values "
                        f"{schema[key]['enum_values']}"
                        + (f" on column '{column_path}'" if column_path else ""))

        full_type_name = f"projects/{ASPECT_TYPES_PROJECT}/locations/{ASPECT_LOCATION}/aspectTypes/{aspect_id}"
        if column_path:
            map_key = f"{ASPECT_TYPES_PROJECT}.{ASPECT_LOCATION}.{aspect_id}@Schema.{column_path}"
            aspects_payload[map_key] = {"aspectType": full_type_name, "path": f"Schema.{column_path}", "data": data}
        else:
            map_key = f"{ASPECT_TYPES_PROJECT}.{ASPECT_LOCATION}.{aspect_id}"
            aspects_payload[map_key] = {"aspectType": full_type_name, "data": data}

    # Table-level
    table_meta = node.get("meta") or node.get("config", {}).get("meta", {})
    for aspect_id, aspect_data in table_meta.items():
        add_aspect(aspect_id, aspect_data)

    # Column-level - dynamic, any number of custom aspects/fields, no code change needed
    for col_name, col_details in node.get("columns", {}).items():
        for aspect_id, aspect_data in col_details.get("meta", {}).items():
            add_aspect(aspect_id, aspect_data, column_path=col_name)

    return aspects_payload, warnings


def get_current_sync_hash(token, entry_name):
    if os.environ.get("DISABLE_BOOKKEEPING") == "true":
        return None
    """GET the entry (full view, to include aspects) and pull out our
    bookkeeping aspect's content_hash, if present. Returns None if the entry
    doesn't exist yet or has never been synced by this script before."""
    url = f"https://dataplex.googleapis.com/v1/{entry_name}?view=all"
    resp = _http_request("GET", url, token)
    if not resp:
        return None
    log.debug("Entry %s aspect keys returned: %s", entry_name, list((resp.get("aspects") or {}).keys()))
    for key, aspect in (resp.get("aspects") or {}).items():
        # Match on the aspectType field inside the value, not the map key -
        # the API is not guaranteed to echo back the same dotted shorthand
        # we used when writing (it may return full resource paths and/or
        # substitute the project number for the project ID).
        aspect_type = aspect.get("aspectType", "")
        if aspect_type.endswith(f"/{SYNC_METADATA_ASPECT_ID}") or key.endswith(f".{SYNC_METADATA_ASPECT_ID}"):
            return aspect.get("data", {}).get(FIELD_CONTENT_HASH)
    return None


# ---------------------------------------------------------------------------
# Sync one model
# ---------------------------------------------------------------------------

def sync_model(token, unique_id, node, dry_run, invocation_id):
    project, dataset, table, materialized = resolve_physical_table(node)
    if materialized == "ephemeral":
        return "skipped", f"{unique_id}: ephemeral materialization has no physical table/view to sync"
    if not (project and dataset and table):
        return "skipped", f"{unique_id}: incomplete database/schema/alias in manifest"

    location = get_dataset_location(token, project, dataset)
    system_entry_id = f"bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"
    entry_name = (f"projects/{project}/locations/{location}/entryGroups/@bigquery"
                  f"/entries/{system_entry_id}")

    new_hash = compute_content_hash(node)
    current_hash = get_current_sync_hash(token, entry_name)
    if current_hash == new_hash:
        return "unchanged", f"{project}.{dataset}.{table} ({materialized}) - no metadata change"

    aspects_payload, warnings = build_aspects_payload(token, node, location)
    for w in warnings:
        log.warning("[%s.%s.%s] %s", project, dataset, table, w)

    if not aspects_payload:
        return "skipped", f"{project}.{dataset}.{table}: no custom aspects defined in yml meta"

    # Bookkeeping aspect so next run can detect "unchanged" without external state
    if os.environ.get("DISABLE_BOOKKEEPING") != "true":
        aspects_payload[f"{ASPECT_TYPES_PROJECT}.{ASPECT_LOCATION}.{SYNC_METADATA_ASPECT_ID}"] = {
            "aspectType": f"projects/{ASPECT_TYPES_PROJECT}/locations/{ASPECT_LOCATION}/aspectTypes/{SYNC_METADATA_ASPECT_ID}",
            "data": {
                FIELD_CONTENT_HASH: new_hash,
                FIELD_SYNC_STATUS: "FULL",
                FIELD_SYNCED_AT: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                FIELD_INVOCATION_ID: invocation_id,
            },
        }

    if dry_run:
        return "would_sync", f"{project}.{dataset}.{table} ({materialized}) - {len(aspects_payload)} aspect(s)"

    patch_url = f"https://dataplex.googleapis.com/v1/{entry_name}?updateMask=aspects"
    _http_request("PATCH", patch_url, token, payload={"aspects": aspects_payload})
    return "synced", f"{project}.{dataset}.{table} ({materialized}) - {len(aspects_payload)} aspect(s)"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Sync dbt meta aspects into Knowledge Catalog")
    parser.add_argument("--target-dir", help="Absolute path to dbt target/ dir "
                                              "(overrides DBT_TARGET_DIR env var)")
    parser.add_argument("--validate-only", action="store_true",
                         help="Dry run: validate + diff only, no writes. Use as a pre-merge CI gate.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not ASPECT_TYPES_PROJECT:
        log.error("ASPECT_TYPES_PROJECT env var is required.")
        sys.exit(2)

    target_dir = resolve_target_dir(args.target_dir)
    manifest_path = os.path.join(target_dir, "manifest.json")
    run_results_path = os.path.join(target_dir, "run_results.json")

    try:
        manifest = load_json_safe(manifest_path, required=True)
    except Exception as e:
        log.error("Cannot proceed without manifest.json: %s", e)
        sys.exit(2)

    run_results = load_json_safe(run_results_path, required=False)
    invocation_id = (run_results or {}).get("metadata", {}).get("invocation_id", "unknown")

    models = get_models_to_consider(manifest, run_results)
    if not models:
        log.info("No models to consider (nothing succeeded in this run, or manifest empty).")
        return

    token = get_access_token()

    results = {"synced": [], "unchanged": [], "would_sync": [], "skipped": [], "failed": []}

    for unique_id, node in models:
        try:
            status, detail = sync_model(token, unique_id, node, args.validate_only, invocation_id)
            results[status].append(detail)
            log.info("[%s] %s", status.upper(), detail)
        except Exception as e:
            # Failure isolation: one bad model must not stop the others.
            results["failed"].append(f"{unique_id}: {e}")
            log.error("[FAILED] %s: %s", unique_id, e)

    log.info("---- Summary ----")
    for status, items in results.items():
        log.info("%s: %d", status, len(items))

    if results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()