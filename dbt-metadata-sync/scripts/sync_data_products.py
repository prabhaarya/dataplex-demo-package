"""
sync_data_products.py

Syncs Data Product definitions (data_products/*.yml) into Knowledge Catalog
(Dataplex Universal Catalog) - creates/updates the Data Product wrapper,
attaches built-in and custom aspects, and links BigQuery table/view assets
resolved from the dbt manifest.


CLI:
  python sync_data_products.py [--target-dir PATH] [--data-products-dir PATH]
                                [--validate-only] [--verbose]

  --validate-only : parse + validate every yml, resolve dbt models, print what
                     WOULD change (including unchanged/skip reasons), make NO
                     API writes. Use as a pre-merge / pre-commit CI gate.
"""

import argparse
import glob
import hashlib
import json
import logging
import os
import random
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import yaml

try:
    from google.auth import default as google_auth_default
    from google.auth.transport.requests import Request as GoogleAuthRequest
except ImportError:
    print("Missing dependency: pip install google-auth", file=sys.stderr)
    raise

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://dataplex.googleapis.com/v1"
ASPECT_LOCATION = "global"  # Aspect type definitions are always global. Fixed.

DATA_PRODUCTS_PROJECT = os.environ.get("DATA_PRODUCTS_PROJECT")
ASPECT_TYPES_PROJECT = os.environ.get("ASPECT_TYPES_PROJECT")
DP_LOCATION = os.environ.get("DP_LOCATION", "us")
SYNC_METADATA_ASPECT_ID = os.environ.get("SYNC_METADATA_ASPECT_ID", "custom-dbt-sync-metadata")
FIELD_CONTENT_HASH = "custom-content-hash"
FIELD_SYNCED_AT = "custom-synched-at"
FIELD_INVOCATION_ID = "custom-dbt-invocation-id"
FIELD_SYNC_STATUS = "custom-sync-status"  # "FULL" | "PARTIAL"

OOB_ASPECT_ALIASES = set(
    a.strip() for a in os.environ.get("OOB_ASPECT_ALIASES", "overview,queries,refresh-cadence").split(",") if a.strip()
)

VALID_STATES = {"active", "deleted"}
REQUIRED_TOP_KEYS = {"metadata", "spec"}

log = logging.getLogger("dp_sync")

# In-memory caches (per-process, per-run only - not persisted)
_aspect_schema_cache = {}
_asset_exists_cache = {}


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_access_token():
    """Real ADC: works with Workload Identity Federation, a mounted service
    account key, or a local gcloud user - whatever google.auth.default()
    resolves in the environment. No gcloud CLI dependency."""
    creds, _ = google_auth_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(GoogleAuthRequest())
    return creds.token


def get_project_number(project_id, token):
    """Resolves the numeric Project Number from the string Project ID.
    Doubles as an early auth sanity check - if the token is bad or the
    service account lacks resourcemanager access, we fail fast here instead
    of partway through a batch of Data Products."""
    url = f"https://cloudresourcemanager.googleapis.com/v1/projects/{project_id}"
    resp = _http_request("GET", url, token)
    if not resp or "projectNumber" not in resp:
        raise RuntimeError(f"Failed to resolve project number for '{project_id}'")
    return resp["projectNumber"]


# ---------------------------------------------------------------------------
# HTTP helper with retry/backoff (transient 5xx/429/409), and LRO polling
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
            if e.code == 404:
                return None
            retriable = e.code in (409, 429, 500, 502, 503)
            if retriable and attempt < max_retries:
                sleep_s = min(2 ** attempt + random.uniform(0, 1), 30)
                log.warning("HTTP %s on %s %s (attempt %d/%d), retrying in %.1fs",
                            e.code, method, url, attempt + 1, max_retries, sleep_s)
                time.sleep(sleep_s)
                continue
            raise RuntimeError(f"HTTP {e.code} calling {method} {url}: {body}") from e
    raise RuntimeError(f"Exhausted retries calling {method} {url}")


def wait_for_operation(operation_name, token, task_description="Operation"):
    """Synchronous polling for Dataplex LROs (DP wrapper create/update/delete,
    asset link/unlink). We poll rather than fire-and-forget so a second DP in
    the same pipeline run never collides with a concurrent-update lock."""
    if not operation_name:
        return True
    log.info("  - waiting: %s", task_description)
    delay = 2
    max_delay = 32
    op_url = f"{BASE_URL}/{operation_name}"
    while True:
        time.sleep(delay)
        status = _http_request("GET", op_url, token)
        if status is None:
            log.warning("  - could not read operation status for: %s", task_description)
            return False
        if status.get("done", False):
            if "error" in status:
                log.error("  - failed: %s (%s)", task_description, status["error"].get("message"))
                return False
            log.info("  - done: %s", task_description)
            return True
        delay = min(delay * 2, max_delay)


# ---------------------------------------------------------------------------
# File / manifest loading (absolute paths, failure isolation)
# ---------------------------------------------------------------------------

def resolve_abs_dir(cli_arg, env_var, default_relative):
    value = cli_arg or os.environ.get(env_var) or default_relative
    return os.path.abspath(value)


def load_json_safe(path, required=True):
    if not os.path.exists(path):
        if required:
            raise FileNotFoundError(f"Required file not found: {path}")
        log.warning("Optional file not found, continuing without it: %s", path)
        return None
    with open(path, "r") as f:
        return json.load(f)


def find_dp_yaml_files(dp_dir):
    files = sorted(glob.glob(os.path.join(dp_dir, "*.yml")) + glob.glob(os.path.join(dp_dir, "*.yaml")))
    return [f for f in files if "_TEMPLATE" not in os.path.basename(f).upper()]


def load_yaml_safe(path):
    """Failure isolation: a malformed yml raises here, and the caller is
    responsible for catching it and skipping just this one file."""
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Pre-commit schema validation (structural + live aspect-type schema check)
# ---------------------------------------------------------------------------

def validate_dp_structure(config, file_path):
    """Cheap structural checks that don't need network access - keys, types,
    enum casing on state. Returns a list of error strings (empty = valid)."""
    errors = []
    if not isinstance(config, dict):
        return [f"{file_path}: file did not parse into a mapping"]

    missing_top = REQUIRED_TOP_KEYS - set(config.keys())
    if missing_top:
        errors.append(f"missing top-level key(s): {sorted(missing_top)}")
        return errors  # nothing else to check safely

    metadata = config.get("metadata") or {}
    spec = config.get("spec") or {}

    if not metadata.get("id"):
        errors.append("metadata.id is required")

    state = str(metadata.get("state", "active")).lower()
    if state not in VALID_STATES:
        errors.append(f"metadata.state '{metadata.get('state')}' must be one of {sorted(VALID_STATES)} (lowercase)")

    region = metadata.get("region")
    if region is not None and not isinstance(region, str):
        errors.append("metadata.region must be a string (e.g. 'us', 'us-central1', 'eu')")

    if state == "active":
        if not spec.get("displayName"):
            errors.append("spec.displayName is required for an active Data Product")
        owner_emails = spec.get("ownerEmails", [])
        if not isinstance(owner_emails, list) or not owner_emails:
            errors.append("spec.ownerEmails must be a non-empty list")
        aspects = spec.get("aspects", {})
        if aspects and not isinstance(aspects, dict):
            errors.append("spec.aspects must be a mapping of alias -> field data")
        assets = spec.get("assets", [])
        if assets and not isinstance(assets, list):
            errors.append("spec.assets must be a list")
        for asset in assets or []:
            if not isinstance(asset, dict) or not asset.get("dbtModel"):
                errors.append(f"each entry in spec.assets needs a 'dbtModel' key, got: {asset}")

    return errors


def get_aspect_type_schema(token, aspect_id):
    """Live schema fetch from KC - this is what makes aspect handling dynamic:
    new fields/aspects introduced later are validated automatically, no code
    change needed here. Built-in (dataplex-types) aspects are skipped since
    we don't own that schema."""
    if aspect_id in _aspect_schema_cache:
        return _aspect_schema_cache[aspect_id]

    url = f"{BASE_URL}/projects/{ASPECT_TYPES_PROJECT}/locations/{ASPECT_LOCATION}/aspectTypes/{aspect_id}"
    try:
        resp = _http_request("GET", url, token)
    except Exception as e:
        log.warning("Could not fetch aspect type schema for '%s' (%s); skipping validation for it", aspect_id, e)
        resp = None

    schema = {}
    if resp:
        for f in resp.get("metadataTemplate", {}).get("recordFields", []):
            enum_values = None
            if f.get("type") == "enum":
                enum_values = {ev.get("name") for ev in f.get("annotations", {}).get("enumValues", [])} \
                    or {ev.get("name") for ev in f.get("enumValues", [])}
            schema[f["name"]] = {"type": f.get("type"), "enum_values": enum_values}

    _aspect_schema_cache[aspect_id] = schema
    return schema


def validate_aspect_field_values(token, alias, aspect_data):
    """Returns a list of warnings (not hard failures) for unknown fields or
    enum values that don't match the live aspect type schema's casing."""
    if alias in OOB_ASPECT_ALIASES:
        return []  # built-in aspect - schema not ours to fetch/validate
    warnings = []
    schema = get_aspect_type_schema(token, alias)
    if not schema:
        return warnings
    for key, value in (aspect_data or {}).items():
        if key not in schema:
            warnings.append(f"aspect '{alias}': unknown field '{key}'")
        elif schema[key]["enum_values"] and value not in schema[key]["enum_values"]:
            warnings.append(
                f"aspect '{alias}': value '{value}' for field '{key}' does not match "
                f"defined casing/values {schema[key]['enum_values']}")
    return warnings


# ---------------------------------------------------------------------------
# dbt manifest -> physical asset resolution (works for both tables and views;
# BigQuery/Dataplex treat both as members of the same "tables" collection)
# ---------------------------------------------------------------------------

def resolve_asset_resource(manifest, model_name):
    """Resolves a model name against whichever manifest is active for this
    dbt invocation/connection. Because manifest.json reflects the target
    (dev/staging/prod) that actually ran, the same dbtModel name naturally
    resolves to the right project/dataset per environment - no per-connection
    branching needed in this script."""
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") == "model" and node.get("name") == model_name:
            project = node.get("database")
            dataset = node.get("schema")
            table = node.get("alias") or node.get("name")
            if not (project and dataset and table):
                raise ValueError(f"Model '{model_name}' is missing database/schema/alias in manifest")
            resource = f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}"
            return resource, project, dataset, table
    raise ValueError(f"Model '{model_name}' not found in dbt manifest")


def asset_exists_in_bigquery(token, project, dataset, table):
    """Best-effort existence check before we attempt to link an asset - lets
    us skip with a clear warning instead of failing an LRO create call."""
    cache_key = (project, dataset, table)
    if cache_key in _asset_exists_cache:
        return _asset_exists_cache[cache_key]
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}"
    try:
        resp = _http_request("GET", url, token)
        exists = resp is not None
    except Exception as e:
        log.warning("Could not verify BigQuery asset %s.%s.%s (%s); assuming it exists", project, dataset, table, e)
        exists = True
    _asset_exists_cache[cache_key] = exists
    return exists


# ---------------------------------------------------------------------------
# Aspect payload construction (dynamic - new aliases/fields need no code change)
# ---------------------------------------------------------------------------

def build_aspects_payload(spec_aspects):
    aspects_to_apply = {}
    for alias, data in (spec_aspects or {}).items():
        if alias in OOB_ASPECT_ALIASES:
            aspect_key = f"dataplex-types.{ASPECT_LOCATION}.{alias}"
            full_type_name = f"projects/dataplex-types/locations/{ASPECT_LOCATION}/aspectTypes/{alias}"
        else:
            aspect_key = f"{ASPECT_TYPES_PROJECT}.{ASPECT_LOCATION}.{alias}"
            full_type_name = f"projects/{ASPECT_TYPES_PROJECT}/locations/{ASPECT_LOCATION}/aspectTypes/{alias}"
        aspects_to_apply[aspect_key] = {"aspectType": full_type_name, "data": data}
    return aspects_to_apply


def compute_content_hash(dp_payload, aspects_to_apply, asset_resources, state):
    canonical = json.dumps(
        {
            "wrapper": dp_payload,
            "aspects": {k: v["data"] for k, v in aspects_to_apply.items()},
            "assets": sorted(asset_resources),
            "state": state,
        },
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def get_current_sync_hash(token, entry_name):
    if os.environ.get("DISABLE_BOOKKEEPING") == "true":
        return None
    """GET the DP's entry (full view) and pull out our bookkeeping aspect's
    content hash, if present. None means never synced (or entry doesn't exist
    yet) - either way, treated as "changed"."""
    url = f"{BASE_URL}/{entry_name}?view=all"
    resp = _http_request("GET", url, token)
    if not resp:
        return None
    for key, aspect in (resp.get("aspects") or {}).items():
        aspect_type = aspect.get("aspectType", "")
        if aspect_type.endswith(f"/{SYNC_METADATA_ASPECT_ID}") or key.endswith(f".{SYNC_METADATA_ASPECT_ID}"):
            return aspect.get("data", {}).get(FIELD_CONTENT_HASH)
    return None


# ---------------------------------------------------------------------------
# Sync one Data Product
# ---------------------------------------------------------------------------

def sync_one_dp(token, project_number, manifest, file_path, dry_run, invocation_id):
    config = load_yaml_safe(file_path)  # raises on bad yaml - caught by caller

    errors = validate_dp_structure(config, file_path)
    if errors:
        return "invalid", f"{os.path.basename(file_path)}: " + "; ".join(errors)

    metadata = config["metadata"]
    spec = config.get("spec", {})
    dp_id = metadata["id"]
    state = str(metadata.get("state", "active")).lower()
    # Per-DP region override - falls back to the DP_LOCATION env var (itself
    # defaulting to "us") when the yml doesn't set metadata.region.
    region = str(metadata.get("region") or DP_LOCATION).lower()

    dp_url = f"{BASE_URL}/projects/{DATA_PRODUCTS_PROJECT}/locations/{region}/dataProducts/{dp_id}"
    nested_entry_id = f"projects/{project_number}/locations/{region}/dataProducts/{dp_id}"
    entry_url = f"{BASE_URL}/projects/{DATA_PRODUCTS_PROJECT}/locations/{region}/entryGroups/@dataplex/entries/{nested_entry_id}"

    # =======================================================================
    # DELETION
    # =======================================================================
    if state == "deleted":
        if dry_run:
            return "would_delete", dp_id
        return delete_dp(token, dp_id, dp_url)

    # =======================================================================
    # RESOLVE ASSETS (dynamic project/dataset per dbt manifest, table or view)
    # =======================================================================
    resolved_assets = []  # list of (bq_resource, model_name)
    for asset in spec.get("assets", []):
        model_name = asset["dbtModel"]
        resource, project, dataset, table = resolve_asset_resource(manifest, model_name)
        resolved_assets.append((resource, model_name, project, dataset, table))

    # =======================================================================
    # BUILD PAYLOADS + VALIDATE ASPECT DATA (warnings only, not fatal)
    # =======================================================================
    dp_payload = {
        "displayName": spec.get("displayName", ""),
        "description": spec.get("description", ""),
        "ownerEmails": spec.get("ownerEmails", []),
    }
    aspects_to_apply = build_aspects_payload(spec.get("aspects", {}))

    for alias, data in (spec.get("aspects", {}) or {}).items():
        for w in validate_aspect_field_values(token, alias, data):
            log.warning("[%s] %s", dp_id, w)

    # =======================================================================
    # CHANGE DETECTION - skip entirely if nothing would actually change
    # =======================================================================
    new_hash = compute_content_hash(dp_payload, aspects_to_apply, [r[0] for r in resolved_assets], state)
    current_hash = get_current_sync_hash(token, entry_url.replace(f"{BASE_URL}/", ""))
    if current_hash == new_hash:
        return "unchanged", dp_id

    if dry_run:
        return "would_sync", f"{dp_id} - {len(aspects_to_apply)} aspect(s), {len(resolved_assets)} asset(s)"

    # =======================================================================
    # UPSERT DP WRAPPER (LRO)
    # =======================================================================
    existing = _http_request("GET", dp_url, token)
    if existing is None:
        create_url = f"{BASE_URL}/projects/{DATA_PRODUCTS_PROJECT}/locations/{region}/dataProducts?dataProductId={dp_id}"
        res = _http_request("POST", create_url, token, payload=dp_payload)
        wait_for_operation(res.get("name"), token, f"Create Data Product '{dp_id}'")
    else:
        update_url = f"{dp_url}?updateMask=displayName,description,ownerEmails"
        res = _http_request("PATCH", update_url, token, payload=dp_payload)
        wait_for_operation(res.get("name"), token, f"Update Data Product '{dp_id}'")

    # =======================================================================
    # APPLY ASPECTS (synchronous PATCH, not an LRO). The bookkeeping hash is
    # deliberately NOT written yet - it's only written once asset linking
    # below fully succeeds, so a partial asset failure gets retried on the
    # next run instead of being silently marked "unchanged" forever.
    # =======================================================================
    entry_patch_url = f"{entry_url}?updateMask=aspects"
    _http_request("PATCH", entry_patch_url, token, payload={"aspects": aspects_to_apply})

    # =======================================================================
    # LINK / UNLINK ASSETS (idempotent - skip ones already linked, LRO per
    # change). Each asset (link or unlink) is isolated with its own
    # try/except: one bad asset (e.g. wrong region, or a stuck unlink) must
    # not block the other, valid assets in the same Data Product.
    # =======================================================================
    existing_assets_url = f"{BASE_URL}/projects/{DATA_PRODUCTS_PROJECT}/locations/{region}/dataProducts/{dp_id}/dataAssets"
    existing_resp = _http_request("GET", existing_assets_url, token)
    already_linked = {}  # resource path -> full dataAsset resource name
    if existing_resp:
        for a in existing_resp.get("dataAssets", []):
            res_path = a.get("resourceSpec", {}).get("name") or a.get("resource")
            if res_path:
                already_linked[res_path] = a.get("name")

    desired_resources = {r[0] for r in resolved_assets}
    linked, skipped, unlinked, asset_failures = 0, 0, 0, []

    for resource, model_name, project, dataset, table in resolved_assets:
        try:
            if resource in already_linked:
                continue
            if not asset_exists_in_bigquery(token, project, dataset, table):
                log.warning("[%s] asset '%s' (%s.%s.%s) not found in BigQuery, skipping link",
                            dp_id, model_name, project, dataset, table)
                skipped += 1
                continue
            asset_id = model_name.replace("_", "-").lower()[:63]
            create_asset_url = f"{BASE_URL}/projects/{DATA_PRODUCTS_PROJECT}/locations/{region}/dataProducts/{dp_id}/dataAssets?dataAssetId={asset_id}"
            res = _http_request("POST", create_asset_url, token, payload={"resource": resource})
            if not wait_for_operation(res.get("name"), token, f"Link asset '{model_name}' to '{dp_id}'"):
                raise RuntimeError(f"link operation for '{model_name}' finished with an error - see logs above")
            linked += 1
        except Exception as e:
            # Isolate this asset's failure - keep going so the remaining,
            # valid assets in this same Data Product still get linked.
            asset_failures.append(f"link {model_name}: {e}")
            log.error("[%s] failed to link asset '%s': %s", dp_id, model_name, e)

    # Anything still linked in KC but no longer present in the yml's
    # spec.assets gets unlinked - this is what handles a dbtModel entry
    # being removed from the Data Product's yml.
    for resource, asset_full_name in already_linked.items():
        if resource in desired_resources or not asset_full_name:
            continue
        try:
            del_url = f"{BASE_URL}/{asset_full_name}"
            res = _http_request("DELETE", del_url, token)
            if res is not None:
                if not wait_for_operation(res.get("name"), token, f"Unlink stale asset from '{dp_id}'"):
                    raise RuntimeError(f"unlink operation for '{resource}' finished with an error - see logs above")
            unlinked += 1
        except Exception as e:
            asset_failures.append(f"unlink {resource}: {e}")
            log.error("[%s] failed to unlink stale asset '%s': %s", dp_id, resource, e)

    if asset_failures:
        # Record the attempt as "PARTIAL" so it's visible directly on the
        # catalog entry (not just in CI logs) - but deliberately DO NOT write
        # the content-hash field. Leaving it absent/cleared means next run's
        # hash comparison still sees this DP as "changed" and retries the
        # failed link(s)/unlink(s), instead of being silently marked as fully synced.
        partial_payload = {
            f"{ASPECT_TYPES_PROJECT}.{ASPECT_LOCATION}.{SYNC_METADATA_ASPECT_ID}": {
                "aspectType": f"projects/{ASPECT_TYPES_PROJECT}/locations/{ASPECT_LOCATION}/aspectTypes/{SYNC_METADATA_ASPECT_ID}",
                "data": {
                    FIELD_CONTENT_HASH: "",  # intentionally cleared - forces retry next run
                    FIELD_SYNC_STATUS: "PARTIAL",
                    FIELD_SYNCED_AT: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    FIELD_INVOCATION_ID: invocation_id,
                },
            }
        }
        if os.environ.get("DISABLE_BOOKKEEPING") != "true":
            _http_request("PATCH", entry_patch_url, token, payload={"aspects": partial_payload})
        return "partial", (
            f"{dp_id} - wrapper/aspects synced, {linked} asset(s) linked, {unlinked} unlinked, "
            f"{len(asset_failures)} asset change(s) failed and will retry next run: "
            + "; ".join(asset_failures)
        )

    # All asset changes applied (or intentionally skipped as missing) - safe
    # to record the sync hash now so an unchanged re-run can skip entirely.
    bookkeeping_payload = {
        f"{ASPECT_TYPES_PROJECT}.{ASPECT_LOCATION}.{SYNC_METADATA_ASPECT_ID}": {
            "aspectType": f"projects/{ASPECT_TYPES_PROJECT}/locations/{ASPECT_LOCATION}/aspectTypes/{SYNC_METADATA_ASPECT_ID}",
            "data": {
                FIELD_CONTENT_HASH: new_hash,
                FIELD_SYNC_STATUS: "FULL",
                FIELD_SYNCED_AT: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                FIELD_INVOCATION_ID: invocation_id,
            },
        }
    }
    if os.environ.get("DISABLE_BOOKKEEPING") != "true":
        _http_request("PATCH", entry_patch_url, token, payload={"aspects": bookkeeping_payload})

    return "synced", f"{dp_id} - {len(aspects_to_apply)} aspect(s), {linked} linked, {unlinked} unlinked, {skipped} skipped"


def delete_dp(token, dp_id, dp_url):
    assets_url = f"{dp_url}/dataAssets"
    assets_resp = _http_request("GET", assets_url, token)
    if assets_resp:
        for a in assets_resp.get("dataAssets", []):
            asset_path = a.get("name")
            del_url = f"{BASE_URL}/{asset_path}"
            res = _http_request("DELETE", del_url, token)
            if res is not None:
                wait_for_operation(res.get("name"), token, f"Unlink asset from '{dp_id}'")

    res = _http_request("DELETE", dp_url, token)
    if res is None:
        return "unchanged", f"{dp_id} already deleted"
    wait_for_operation(res.get("name"), token, f"Delete Data Product '{dp_id}'")
    return "deleted", dp_id


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Sync Data Product yml files into Knowledge Catalog")
    parser.add_argument("--target-dir", help="Absolute path to dbt target/ dir (overrides DBT_TARGET_DIR)")
    parser.add_argument("--data-products-dir", help="Absolute path to folder of DP yml files (overrides DP_YAML_DIR)")
    parser.add_argument("--validate-only", action="store_true",
                         help="Dry run: validate + diff only, no writes. Use as a pre-merge CI gate.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                         format="%(asctime)s %(levelname)s %(message)s")

    missing_env = [v for v in ("DATA_PRODUCTS_PROJECT", "ASPECT_TYPES_PROJECT")
                   if not os.environ.get(v) and v not in (args.__dict__ or {})]
    if not DATA_PRODUCTS_PROJECT:
        log.error("DATA_PRODUCTS_PROJECT env var is required.")
        sys.exit(2)
    if not ASPECT_TYPES_PROJECT:
        log.error("ASPECT_TYPES_PROJECT env var is required.")
        sys.exit(2)

    target_dir = resolve_abs_dir(args.target_dir, "DBT_TARGET_DIR", "target")
    dp_dir = resolve_abs_dir(args.data_products_dir, "DP_YAML_DIR", "data_products")

    try:
        manifest = load_json_safe(os.path.join(target_dir, "manifest.json"), required=True)
    except Exception as e:
        log.error("Cannot proceed without manifest.json: %s", e)
        sys.exit(2)

    run_results = load_json_safe(os.path.join(target_dir, "run_results.json"), required=False)
    invocation_id = (run_results or {}).get("metadata", {}).get("invocation_id", "unknown")

    yaml_files = find_dp_yaml_files(dp_dir)
    if not yaml_files:
        log.info("No Data Product yml files found in %s", dp_dir)
        return

    token = get_access_token()
    try:
        project_number = get_project_number(DATA_PRODUCTS_PROJECT, token)
    except Exception as e:
        log.error("Auth / project resolution failed - aborting before touching any Data Product: %s", e)
        sys.exit(2)
    log.info("Authenticated. Target project: %s (number %s)", DATA_PRODUCTS_PROJECT, project_number)

    results = {"synced": [], "unchanged": [], "would_sync": [], "would_delete": [],
               "deleted": [], "invalid": [], "partial": [], "failed": []}

    for file_path in yaml_files:
        try:
            status, detail = sync_one_dp(token, project_number, manifest, file_path, args.validate_only, invocation_id)
            results[status].append(detail)
            log.info("[%s] %s", status.upper(), detail)
        except Exception as e:
            # Failure isolation: one bad DP file must not stop the others.
            results["failed"].append(f"{os.path.basename(file_path)}: {e}")
            log.error("[FAILED] %s: %s", os.path.basename(file_path), e)

    log.info("---- Summary ----")
    for status, items in results.items():
        log.info("%s: %d", status, len(items))

    if results["failed"] or results["invalid"] or results["partial"]:
        sys.exit(1)


if __name__ == "__main__":
    main()