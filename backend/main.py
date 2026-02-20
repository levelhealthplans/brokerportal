from __future__ import annotations

import csv
import base64
import html
import hashlib
import hmac
import json
import mimetypes
import os
import re
import secrets
import shutil
import sqlite3
import threading
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

import openpyxl
import xlrd
from fastapi import FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
db_path_raw = os.getenv("DB_PATH", str(BASE_DIR / "app.db"))
DB_PATH = Path(db_path_raw).expanduser()
if not DB_PATH.is_absolute():
    DB_PATH = (BASE_DIR / DB_PATH).resolve()
else:
    DB_PATH = DB_PATH.resolve()

uploads_dir_raw = os.getenv("UPLOADS_DIR", str(BASE_DIR.parent / "uploads"))
UPLOADS_DIR = Path(uploads_dir_raw).expanduser()
if not UPLOADS_DIR.is_absolute():
    UPLOADS_DIR = (BASE_DIR.parent / UPLOADS_DIR).resolve()
else:
    UPLOADS_DIR = UPLOADS_DIR.resolve()
NETWORK_OPTIONS_PATH = (BASE_DIR / "data" / "network_options.csv").resolve()
NETWORK_SETTINGS_PATH = (BASE_DIR / "data" / "network_settings.json").resolve()
hubspot_settings_path_raw = os.getenv("HUBSPOT_SETTINGS_PATH", str(DB_PATH.with_name("hubspot_settings.json")))
HUBSPOT_SETTINGS_PATH = Path(hubspot_settings_path_raw).expanduser()
if not HUBSPOT_SETTINGS_PATH.is_absolute():
    HUBSPOT_SETTINGS_PATH = (BASE_DIR / HUBSPOT_SETTINGS_PATH).resolve()
else:
    HUBSPOT_SETTINGS_PATH = HUBSPOT_SETTINGS_PATH.resolve()
LEGACY_HUBSPOT_SETTINGS_PATH = (BASE_DIR / "data" / "hubspot_settings.json").resolve()

SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
SESSION_COOKIE_SAMESITE = os.getenv("SESSION_COOKIE_SAMESITE", "lax").strip().lower()
if SESSION_COOKIE_SAMESITE not in {"lax", "strict", "none"}:
    SESSION_COOKIE_SAMESITE = "lax"
ALLOW_QUERY_AUTH_FALLBACK = os.getenv("ALLOW_QUERY_AUTH_FALLBACK", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

DEFAULT_ADMIN_EMAIL = os.getenv("DEFAULT_ADMIN_EMAIL", "jake@levelhealthplans.com").strip().lower()
DEFAULT_ADMIN_FIRST_NAME = os.getenv("DEFAULT_ADMIN_FIRST_NAME", "Jake").strip()
DEFAULT_ADMIN_LAST_NAME = os.getenv("DEFAULT_ADMIN_LAST_NAME", "Page").strip()
DEFAULT_ADMIN_ORGANIZATION = os.getenv("DEFAULT_ADMIN_ORGANIZATION", "Level Health").strip()
DEFAULT_ADMIN_TITLE = os.getenv("DEFAULT_ADMIN_TITLE", "Admin").strip()
DEFAULT_ADMIN_PASSWORD = os.getenv("DEFAULT_ADMIN_PASSWORD", "ChangeMe123!").strip()
DEFAULT_USER_PASSWORD = os.getenv("DEFAULT_USER_PASSWORD", "ChangeMe123!").strip()

UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

SESSION_COOKIE_NAME = "lh_session"
SESSION_DURATION_HOURS = 24 * 7
MAGIC_LINK_DURATION_MINUTES = 15

BROKER_DOMAIN_MAP = {
    "legacybrokerskc.com": "Legacy Brokers KC",
    "heartlandbenefitpartners.com": "Heartland Benefit Partners",
}

IMPLEMENTATION_TASK_TITLES = [
    "Implementation Forms",
    "Set up ACH",
    "Final Enrollment",
    "Network Agreement",
    "Program Agreement",
    "SPD",
    "Stoploss Application",
    "Stoploss Contract",
    "Stoploss Disclosure",
    "Vendors Notified",
    "Ventegra vZip",
    "ID Cards Ready",
    "Plan is Live",
]

DEFAULT_STOPLOSS_DISCLOSURE_OPTIONS: List[tuple[str, str]] = [
    (
        "Arlo",
        "https://app.pandadoc.com/a/#/templates/bpN5tuyuHD7qzkr5t64PtQ",
    ),
    (
        "Ryan Specialty",
        "https://app.pandadoc.com/a/#/documents/MXBYyGs4H4ERZRrfjqxatN?new=true",
    ),
]

HUBSPOT_UPLOAD_FIELD_TYPES = [
    ("census", "census"),
    ("sbc", "sbc"),
    ("current_pricing", "current_pricing"),
    ("renewal", "renewal"),
    ("high_cost_claimant_report", "high_cost_claimant_report"),
    ("aggregate_report", "aggregate_report"),
    ("other_claims_data", "other_claims_data"),
    ("other_files", "other_files"),
]
HUBSPOT_UPLOAD_FILE_ID_FIELDS = [
    ("census", "member_level_census"),
    ("sbc", "sbc_file"),
    ("current_pricing", "current_pricing_file"),
    ("renewal", "renewal_file"),
    ("high_cost_claimant_report", "high_cost_claimant_report_file"),
    ("aggregate_report", "aggregate_report_file"),
    ("other_claims_data", "other_claims_data_file"),
    ("other_files", "other_files_file"),
]
HUBSPOT_SYNC_DETAIL_FIELDS = (
    "primary_network",
    "secondary_network",
    "tpa",
    "stoploss",
    "current_carrier",
    "renewal_comparison",
    "proposal_url",
)

BROKER_ADMIN_ONLY_TASKS = {"Vendors Notified", "Ventegra vZip"}
TASK_STATE_CANONICAL = {
    "not started": "Not Started",
    "in progress": "In Progress",
    "complete": "Complete",
    "done": "Complete",
}
ALLOWED_USER_ROLES = {"admin", "broker", "sponsor"}
ALLOWED_ACCESS_REQUEST_ROLES = {"broker", "sponsor"}
ALLOWED_ACCESS_REQUEST_STATUSES = {"pending", "approved", "rejected"}
PASSWORD_MIN_LENGTH = 8
HUBSPOT_API_BASE = "https://api.hubapi.com"
HUBSPOT_OAUTH_AUTHORIZE_URL = "https://app.hubspot.com/oauth/authorize"
HUBSPOT_OAUTH_TOKEN_URL = "https://api.hubapi.com/oauth/v1/token"
HUBSPOT_OAUTH_STATE_MINUTES = 15
hubspot_signature_max_age_raw = (os.getenv("HUBSPOT_SIGNATURE_MAX_AGE_SECONDS", "300") or "300").strip()
try:
    HUBSPOT_SIGNATURE_MAX_AGE_SECONDS = max(0, int(hubspot_signature_max_age_raw))
except Exception:
    HUBSPOT_SIGNATURE_MAX_AGE_SECONDS = 300
HUBSPOT_FILES_FOLDER_ROOT = (os.getenv("HUBSPOT_FILES_FOLDER_ROOT", "/level-health/quote-attachments") or "").strip()
hubspot_files_access_raw = (os.getenv("HUBSPOT_FILES_ACCESS", "PUBLIC_NOT_INDEXABLE") or "").strip().upper()
if hubspot_files_access_raw not in {"PUBLIC_INDEXABLE", "PUBLIC_NOT_INDEXABLE", "PRIVATE"}:
    HUBSPOT_FILES_ACCESS = "PUBLIC_NOT_INDEXABLE"
else:
    HUBSPOT_FILES_ACCESS = hubspot_files_access_raw
HUBSPOT_ATTACHMENTS_CREATE_NOTES = os.getenv(
    "HUBSPOT_ATTACHMENTS_CREATE_NOTES",
    "false",
).strip().lower() in {"1", "true", "yes", "on"}
HUBSPOT_TICKET_RESERVED_PROPERTIES = {
    "subject",
    "content",
    "hs_pipeline",
    "hs_pipeline_stage",
    "hs_ticket_id",
}
HUBSPOT_TICKET_READ_ONLY_PREFIXES = (
    "hs_all_associated_",
    "hs_primary_",
)
US_STATE_ABBREVIATIONS = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}
HUBSPOT_OAUTH_DEFAULT_SCOPES = (
    "oauth tickets "
    "files "
    "crm.objects.contacts.read crm.objects.contacts.write "
    "crm.objects.companies.read crm.objects.companies.write "
    "crm.schemas.companies.read crm.schemas.companies.write "
    "crm.schemas.contacts.read crm.schemas.contacts.write "
)
HUBSPOT_OAUTH_REQUIRED_SCOPES = ("oauth", "tickets", "files")
HUBSPOT_SYNC_LOCK_GUARD = threading.Lock()
HUBSPOT_SYNC_LOCKS: Dict[str, threading.Lock] = {}

app = FastAPI(title="Level Health Broker Portal API")

FRONTEND_BASE_URL = os.getenv("FRONTEND_BASE_URL", "http://localhost:5173").rstrip("/")
_extra_origins_raw = os.getenv("ALLOWED_ORIGINS", "")
EXTRA_ALLOWED_ORIGINS = [
    origin.strip().rstrip("/")
    for origin in _extra_origins_raw.split(",")
    if origin.strip()
]
ALLOW_ORIGIN_REGEX = os.getenv("ALLOWED_ORIGIN_REGEX", "").strip() or None
ALLOWED_ORIGINS = sorted(set([
    FRONTEND_BASE_URL,
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    *EXTRA_ALLOWED_ORIGINS,
]))
_allow_dev_magic_fallback_default = FRONTEND_BASE_URL.startswith("http://localhost") or FRONTEND_BASE_URL.startswith(
    "http://127.0.0.1"
)
ALLOW_DEV_MAGIC_LINK_FALLBACK = os.getenv(
    "ALLOW_DEV_MAGIC_LINK_FALLBACK",
    "true" if _allow_dev_magic_fallback_default else "false",
).strip().lower() in {"1", "true", "yes", "on"}
RESEND_NOTIFICATION_EMAILS_ENABLED = os.getenv(
    "RESEND_NOTIFICATION_EMAILS_ENABLED",
    "true",
).strip().lower() in {"1", "true", "yes", "on"}

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=ALLOW_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----------------------
# Database helpers
# ----------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def now_iso() -> str:
    return datetime.utcnow().isoformat()


def email_domain(email: Optional[str]) -> Optional[str]:
    if not email or "@" not in email:
        return None
    return email.split("@")[-1].strip().lower()


def broker_org_from_email(email: Optional[str]) -> Optional[str]:
    domain = email_domain(email)
    if not domain:
        return None
    return BROKER_DOMAIN_MAP.get(domain)


def normalize_task_state(state: Optional[str]) -> Optional[str]:
    if state is None:
        return None
    key = state.strip().lower()
    return TASK_STATE_CANONICAL.get(key)


def build_hubspot_form_popup_task_url(
    *,
    portal_id: str,
    form_id: str,
    region: str,
) -> Optional[str]:
    portal = str(portal_id or "").strip()
    form = str(form_id or "").strip()
    form_region = str(region or "").strip() or "na1"
    if not portal or not form:
        return None
    query = urlparse.urlencode(
        {
            "portal_id": portal,
            "form_id": form,
            "region": form_region,
        }
    )
    return f"hubspot-form://popup?{query}"


def parse_url_list(value: str) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    parts = re.split(r"[\n,;]+", raw)
    cleaned: List[str] = []
    for part in parts:
        candidate = str(part or "").strip()
        if not candidate:
            continue
        if candidate not in cleaned:
            cleaned.append(candidate)
    return cleaned


def normalize_hubspot_oauth_scopes(value: str) -> str:
    parts = re.split(r"[\s,]+", str(value or "").strip())
    scopes: List[str] = []
    for part in parts:
        candidate = str(part or "").strip()
        if candidate and candidate not in scopes:
            scopes.append(candidate)
    for required in HUBSPOT_OAUTH_REQUIRED_SCOPES:
        if required not in scopes:
            scopes.append(required)
    return " ".join(scopes)


def resolve_frontend_base_url(request: Optional[Request] = None) -> str:
    # In non-local environments, always use the configured canonical frontend URL.
    is_local_frontend = FRONTEND_BASE_URL.startswith("http://localhost") or FRONTEND_BASE_URL.startswith(
        "http://127.0.0.1"
    )
    if not is_local_frontend:
        return FRONTEND_BASE_URL

    if not request:
        return FRONTEND_BASE_URL

    origin = str(request.headers.get("origin") or "").strip().rstrip("/")
    if origin and origin in ALLOWED_ORIGINS:
        return origin

    referer = str(request.headers.get("referer") or "").strip()
    if referer:
        try:
            parsed = urlparse.urlparse(referer)
            if parsed.scheme and parsed.netloc:
                referer_origin = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
                if referer_origin in ALLOWED_ORIGINS:
                    return referer_origin
        except Exception:
            pass

    return FRONTEND_BASE_URL


def parse_labeled_url_list(value: str) -> List[tuple[Optional[str], str]]:
    raw = str(value or "").strip()
    if not raw:
        return []
    parts = re.split(r"[\n,;]+", raw)
    cleaned: List[tuple[Optional[str], str]] = []
    seen_urls: set[str] = set()
    for part in parts:
        candidate = str(part or "").strip()
        if not candidate:
            continue
        label: Optional[str] = None
        url = candidate
        if "|" in candidate:
            raw_label, raw_url = candidate.split("|", 1)
            label = str(raw_label or "").strip() or None
            url = str(raw_url or "").strip()
        if not url or url in seen_urls:
            continue
        cleaned.append((label, url))
        seen_urls.add(url)
    return cleaned


def build_pandadoc_dropdown_task_url_with_labels(
    options: List[tuple[Optional[str], str]],
) -> Optional[str]:
    normalized: List[tuple[Optional[str], str]] = []
    seen_urls: set[str] = set()
    for option in options:
        label, raw_url = option
        candidate = str(raw_url or "").strip()
        if not candidate or candidate in seen_urls:
            continue
        normalized_label = str(label or "").strip() or None
        normalized.append((normalized_label, candidate))
        seen_urls.add(candidate)
    if not normalized:
        return None
    if len(normalized) == 1:
        return normalized[0][1]
    query_items: List[tuple[str, str]] = []
    for label, url in normalized:
        query_items.append(("url", url))
        if label:
            query_items.append(("label", label))
    query = urlparse.urlencode(query_items, doseq=True)
    return f"pandadoc-dropdown://select?{query}"


def build_pandadoc_dropdown_task_url(urls: List[str]) -> Optional[str]:
    return build_pandadoc_dropdown_task_url_with_labels([(None, url) for url in urls])


def parse_pandadoc_dropdown_task_options(task_url: Optional[str]) -> List[tuple[Optional[str], str]]:
    raw = str(task_url or "").strip()
    if not raw:
        return []
    if raw.lower().startswith("pandadoc-dropdown://"):
        try:
            parsed = urlparse.urlparse(raw)
            search_params = urlparse.parse_qs(parsed.query, keep_blank_values=False)
            option_urls = [
                str(value or "").strip()
                for value in search_params.get("url", [])
                if str(value or "").strip()
            ]
            labels = [str(value or "").strip() for value in search_params.get("label", [])]
            options: List[tuple[Optional[str], str]] = []
            seen_urls: set[str] = set()
            for index, option_url in enumerate(option_urls):
                if option_url in seen_urls:
                    continue
                label = labels[index] if index < len(labels) else None
                options.append((label or None, option_url))
                seen_urls.add(option_url)
            return options
        except Exception:
            return []
    return [(None, raw)]


def parse_pandadoc_app_target(url: str) -> Optional[Dict[str, Any]]:
    raw = str(url or "").strip()
    if not raw:
        return None
    try:
        parsed = urlparse.urlparse(raw)
    except Exception:
        return None
    host = str(parsed.netloc or "").strip().lower()
    if "pandadoc.com" not in host:
        return None
    fragment = str(parsed.fragment or "").strip()
    fragment_path = ""
    fragment_query = ""
    if fragment:
        fragment_path, _, fragment_query = fragment.partition("?")
    path_to_parse = fragment_path or parsed.path
    segments = [segment for segment in path_to_parse.strip("/").split("/") if segment]
    if segments and segments[0] == "a":
        segments = segments[1:]
    if len(segments) < 2:
        return None
    object_type = segments[0].strip().lower()
    object_id = segments[1].strip()
    if object_type not in {"templates", "documents"} or not object_id:
        return None
    query_map = urlparse.parse_qs(fragment_query or parsed.query, keep_blank_values=False)
    new_raw = (query_map.get("new") or [""])[0]
    new_flag = str(new_raw or "").strip().lower() in {"1", "true", "yes", "on"}
    return {
        "type": object_type,
        "id": object_id,
        "new": new_flag,
        "url": raw,
    }


def sanitize_pandadoc_document_name(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", str(value or "").strip())
    if not cleaned:
        cleaned = "Stoploss Disclosure"
    return cleaned[:120]


def pandadoc_api_request(
    token: str,
    method: str,
    path: str,
    *,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_token = str(token or "").strip()
    if not normalized_token:
        raise HTTPException(status_code=500, detail="PandaDoc token is not configured")
    normalized_path = str(path or "").strip()
    if not normalized_path:
        raise HTTPException(status_code=500, detail="PandaDoc path is required")
    if normalized_path.startswith("http://") or normalized_path.startswith("https://"):
        url = normalized_path
    else:
        if not normalized_path.startswith("/"):
            normalized_path = f"/{normalized_path}"
        url = f"https://api.pandadoc.com/public/v1{normalized_path}"

    headers = {"Authorization": f"API-Key {normalized_token}"}
    data: Optional[bytes] = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urlrequest.Request(
        url,
        method=str(method or "GET").upper(),
        data=data,
        headers=headers,
    )
    try:
        with urlrequest.urlopen(req, timeout=20) as resp:
            payload = resp.read()
            if not payload:
                return {}
            try:
                parsed = json.loads(payload.decode("utf-8"))
            except Exception:
                return {}
            return parsed if isinstance(parsed, dict) else {}
    except urlerror.HTTPError as exc:
        raw_error = exc.read().decode("utf-8", errors="ignore")
        detail = f"PandaDoc API request failed ({exc.code})"
        if raw_error.strip():
            detail = f"{detail}: {raw_error.strip()}"
        raise HTTPException(status_code=502, detail=detail)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"PandaDoc API request failed: {exc}")


def get_task_assigned_user(conn: sqlite3.Connection, task: sqlite3.Row) -> Optional[sqlite3.Row]:
    assigned_user_id = str(task["assigned_user_id"] or "").strip()
    if not assigned_user_id:
        return None
    cur = conn.cursor()
    cur.execute("SELECT * FROM User WHERE id = ?", (assigned_user_id,))
    return cur.fetchone()


def resolve_stoploss_sponsor_user(
    conn: sqlite3.Connection,
    *,
    installation: sqlite3.Row,
    task: sqlite3.Row,
) -> Optional[sqlite3.Row]:
    assigned_user = get_task_assigned_user(conn, task)
    if assigned_user:
        return assigned_user
    sponsor_domain = str(installation["sponsor_domain"] or "").strip().lower()
    fallback_user_id = find_sponsor_user_id_for_domain(conn, sponsor_domain)
    if not fallback_user_id:
        return None
    cur = conn.cursor()
    cur.execute("SELECT * FROM User WHERE id = ?", (fallback_user_id,))
    return cur.fetchone()


def create_stoploss_disclosure_document_from_template(
    conn: sqlite3.Connection,
    *,
    installation: sqlite3.Row,
    task: sqlite3.Row,
    template_id: str,
    option_label: Optional[str],
) -> str:
    token = (
        os.getenv("PANDADOC_API_KEY", "").strip()
        or os.getenv("PANDADOC_API_TOKEN", "").strip()
    )
    if not token:
        raise HTTPException(
            status_code=503,
            detail="PandaDoc API key is not configured (set PANDADOC_API_KEY)",
        )

    recipient_user = resolve_stoploss_sponsor_user(conn, installation=installation, task=task)
    recipient_email = (
        str(recipient_user["email"] or "").strip().lower()
        if recipient_user
        else ""
    )
    if not recipient_email:
        raise HTTPException(
            status_code=400,
            detail="Stoploss Disclosure requires an assigned Plan Sponsor with an email address",
        )
    first_name = (
        str(recipient_user["first_name"] or "").strip()
        if recipient_user
        else ""
    ) or "Plan"
    last_name = (
        str(recipient_user["last_name"] or "").strip()
        if recipient_user
        else ""
    ) or "Sponsor"
    document_name = sanitize_pandadoc_document_name(
        f'{installation["company"] or "Client"} - Stoploss Disclosure'
        + (f" - {option_label}" if option_label else "")
    )
    recipient_payload: Dict[str, Any] = {
        "email": recipient_email,
        "first_name": first_name,
        "last_name": last_name,
    }
    role_name = (os.getenv("PANDADOC_STOPLOSS_DISCLOSURE_RECIPIENT_ROLE", "") or "").strip()
    if role_name:
        recipient_payload["role"] = role_name
    payload: Dict[str, Any] = {
        "name": document_name,
        "template_uuid": str(template_id or "").strip(),
        "recipients": [recipient_payload],
    }
    created = pandadoc_api_request(
        token,
        "POST",
        "/documents",
        body=payload,
    )
    document_id = str(created.get("id") or "").strip()
    if not document_id:
        raise HTTPException(status_code=502, detail="PandaDoc document creation returned no id")
    return f"https://app.pandadoc.com/a/#/documents/{document_id}"


def resolve_stoploss_disclosure_options() -> List[tuple[Optional[str], str]]:
    labeled_options = parse_labeled_url_list(
        os.getenv("PANDADOC_STOPLOSS_DISCLOSURE_OPTIONS", "")
    )
    if labeled_options:
        return labeled_options
    urls = parse_url_list(os.getenv("PANDADOC_STOPLOSS_DISCLOSURE_URLS", ""))
    if urls:
        return [(None, url) for url in urls]
    single = (os.getenv("PANDADOC_STOPLOSS_DISCLOSURE_URL", "") or "").strip()
    if single:
        return [(None, single)]
    return [(label, url) for label, url in DEFAULT_STOPLOSS_DISCLOSURE_OPTIONS]


def default_installation_task_owner(title: str) -> str:
    if (title or "").strip().lower() == "stoploss disclosure":
        return "Plan Sponsor"
    return "Level Health"


def find_sponsor_user_id_for_domain(
    conn: sqlite3.Connection,
    sponsor_domain: Optional[str],
) -> Optional[str]:
    domain = str(sponsor_domain or "").strip().lower()
    if not domain:
        return None
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, email, organization
        FROM User
        WHERE role = 'sponsor'
        ORDER BY created_at ASC
        """
    )
    for row in cur.fetchall():
        org_value = str(row["organization"] or "").strip().lower()
        email_value = str(row["email"] or "").strip().lower()
        if org_value == domain or email_domain(email_value) == domain:
            return str(row["id"] or "").strip() or None
    return None


def default_installation_task_assigned_user_id(
    conn: sqlite3.Connection,
    *,
    title: str,
    sponsor_domain: Optional[str],
) -> Optional[str]:
    if (title or "").strip().lower() != "stoploss disclosure":
        return None
    return find_sponsor_user_id_for_domain(conn, sponsor_domain)


def default_installation_task_url(title: str) -> Optional[str]:
    normalized_title = (title or "").strip().lower()
    if normalized_title == "implementation forms":
        url = (os.getenv("HUBSPOT_IMPLEMENTATION_FORM_URL", "") or "").strip()
        if url:
            return url
        portal_id = (
            os.getenv("HUBSPOT_IMPLEMENTATION_FORM_PORTAL_ID", "7106327")
            or "7106327"
        ).strip()
        form_id = (
            os.getenv(
                "HUBSPOT_IMPLEMENTATION_FORM_ID",
                "f215c8d6-451d-4b7b-826f-fdab43b80369",
            )
            or "f215c8d6-451d-4b7b-826f-fdab43b80369"
        ).strip()
        region = (os.getenv("HUBSPOT_IMPLEMENTATION_FORM_REGION", "na1") or "na1").strip()
        popup_url = build_hubspot_form_popup_task_url(
            portal_id=portal_id,
            form_id=form_id,
            region=region,
        )
        return popup_url
    if normalized_title == "stoploss disclosure":
        return build_pandadoc_dropdown_task_url_with_labels(
            resolve_stoploss_disclosure_options()
        )
    return None


def fetch_org_by_domain(
    conn: sqlite3.Connection, org_type: str, domain: Optional[str]
) -> Optional[sqlite3.Row]:
    if not domain:
        return None
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM Organization WHERE type = ? AND domain = ?",
        (org_type, domain),
    )
    return cur.fetchone()


def broker_org_name_for_domain(conn: sqlite3.Connection, domain: Optional[str]) -> Optional[str]:
    normalized_domain = normalize_domain_candidate(domain)
    if not normalized_domain:
        return None
    org = fetch_org_by_domain(conn, "broker", normalized_domain)
    if org:
        name = str(org["name"] or "").strip()
        if name:
            return name
    mapped_name = BROKER_DOMAIN_MAP.get(normalized_domain)
    if mapped_name:
        return mapped_name
    return None


DOMAIN_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]*\.[a-z]{2,}$")


def normalize_domain_candidate(value: Optional[str]) -> Optional[str]:
    candidate = str(value or "").strip().lower()
    if not candidate:
        return None
    if "@" in candidate:
        candidate = email_domain(candidate) or ""
    candidate = candidate.strip().strip(".")
    if not candidate or " " in candidate or "/" in candidate:
        return None
    if not DOMAIN_PATTERN.match(candidate):
        return None
    return candidate


def upsert_organization(
    conn: sqlite3.Connection,
    *,
    name: Optional[str],
    org_type: str,
    domain: Optional[str],
) -> bool:
    normalized_type = (org_type or "").strip().lower()
    normalized_domain = normalize_domain_candidate(domain)
    normalized_name = (name or "").strip()
    if normalized_type not in {"broker", "sponsor"} or not normalized_domain:
        return False
    if not normalized_name:
        normalized_name = normalized_domain
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name
        FROM Organization
        WHERE lower(type) = ? AND lower(domain) = ?
        ORDER BY created_at ASC
        LIMIT 1
        """,
        (normalized_type, normalized_domain),
    )
    existing = cur.fetchone()
    if existing:
        existing_name = str(existing["name"] or "").strip()
        if (
            normalized_name
            and existing_name.lower() != normalized_name.lower()
            and (not existing_name or existing_name.lower() == normalized_domain)
        ):
            cur.execute(
                "UPDATE Organization SET name = ? WHERE id = ?",
                (normalized_name, existing["id"]),
            )
            return True
        return False
    cur.execute(
        """
        INSERT INTO Organization (id, name, type, domain, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (str(uuid.uuid4()), normalized_name, normalized_type, normalized_domain, now_iso()),
    )
    return True


def sync_organizations_from_records(conn: sqlite3.Connection) -> None:
    changed = False

    for domain, name in BROKER_DOMAIN_MAP.items():
        if upsert_organization(conn, name=name, org_type="broker", domain=domain):
            changed = True

    cur = conn.cursor()
    cur.execute("SELECT role, email, organization FROM User")
    for row in cur.fetchall():
        role_value = (row["role"] or "").strip().lower()
        user_domain = normalize_domain_candidate(row["email"])
        user_org = str(row["organization"] or "").strip()
        if role_value in {"admin", "broker"} and user_domain:
            broker_name = user_org or BROKER_DOMAIN_MAP.get(user_domain) or user_domain
            if upsert_organization(
                conn,
                name=broker_name,
                org_type="broker",
                domain=user_domain,
            ):
                changed = True
        if role_value == "sponsor":
            sponsor_domain = normalize_domain_candidate(user_org) or user_domain
            if sponsor_domain and upsert_organization(
                conn,
                name=sponsor_domain,
                org_type="sponsor",
                domain=sponsor_domain,
            ):
                changed = True

    cur.execute("SELECT broker_org, broker_email, sponsor_domain, employer_domain FROM Quote")
    for row in cur.fetchall():
        broker_domain = normalize_domain_candidate(row["broker_email"])
        broker_name = str(row["broker_org"] or "").strip() or (
            BROKER_DOMAIN_MAP.get(broker_domain or "") if broker_domain else None
        )
        if broker_domain and upsert_organization(
            conn,
            name=broker_name or broker_domain,
            org_type="broker",
            domain=broker_domain,
        ):
            changed = True
        sponsor_domain = normalize_domain_candidate(row["sponsor_domain"]) or normalize_domain_candidate(
            row["employer_domain"]
        )
        if sponsor_domain and upsert_organization(
            conn,
            name=sponsor_domain,
            org_type="sponsor",
            domain=sponsor_domain,
        ):
            changed = True

    if changed:
        conn.commit()


def fetch_user_by_email(conn: sqlite3.Connection, email: Optional[str]) -> Optional[sqlite3.Row]:
    normalized_email = (email or "").strip().lower()
    if not normalized_email:
        return None
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM User WHERE email = ? LIMIT 1",
        (normalized_email,),
    )
    return cur.fetchone()


def resolve_sponsor_scope(
    conn: sqlite3.Connection,
    email: Optional[str],
) -> tuple[List[str], Optional[str]]:
    normalized_email = (email or "").strip().lower()
    domains: List[str] = []
    email_based_domain = email_domain(normalized_email)
    if email_based_domain:
        domains.append(email_based_domain)

    user = fetch_user_by_email(conn, normalized_email)
    user_id = None
    if user:
        user_id = (user["id"] or "").strip() or None
        org_domain = str(user["organization"] or "").strip().lower()
        if org_domain and org_domain not in domains:
            domains.append(org_domain)
    return domains, user_id


def build_sponsor_installation_filter(
    conn: sqlite3.Connection,
    email: Optional[str],
    *,
    include_assigned_user: bool = True,
) -> tuple[str, List[Any]]:
    sponsor_domains, user_id = resolve_sponsor_scope(conn, email)
    clauses: List[str] = []
    params: List[Any] = []
    for domain in sponsor_domains:
        clauses.append("sponsor_domain = ?")
        params.append(domain)
    if include_assigned_user and user_id:
        clauses.append("id IN (SELECT installation_id FROM Task WHERE assigned_user_id = ?)")
        params.append(user_id)
    if not clauses:
        return "WHERE 1 = 0", []
    return f"WHERE ({' OR '.join(clauses)})", params


def build_access_filter(
    conn: sqlite3.Connection,
    role: Optional[str],
    email: Optional[str],
    *,
    include_assigned_user: bool = True,
    resource: str = "quote",
) -> tuple[str, List[Any]]:
    if not role or role == "admin":
        return "", []
    if role == "broker":
        normalized_email = (email or "").strip().lower()
        domain = email_domain(normalized_email)
        org = fetch_org_by_domain(conn, "broker", domain)
        broker_org = org["name"] if org else broker_org_from_email(normalized_email)

        user = fetch_user_by_email(conn, normalized_email)
        user_id = None
        if user:
            user_id = (user["id"] or "").strip() or None
            if not broker_org:
                broker_org = (user["organization"] or "").strip() or None

        clauses: List[str] = []
        params: List[Any] = []
        if broker_org:
            clauses.append("broker_org = ?")
            params.append(broker_org)
        if include_assigned_user and user_id and resource in {"quote", "task"}:
            clauses.append("assigned_user_id = ?")
            params.append(user_id)
        if not clauses:
            return "WHERE 1 = 0", []
        return f"WHERE ({' OR '.join(clauses)})", params
    if role == "sponsor":
        sponsor_domains, user_id = resolve_sponsor_scope(conn, email)
        clauses: List[str] = []
        params: List[Any] = []
        for domain in sponsor_domains:
            clauses.append("sponsor_domain = ?")
            params.append(domain)
        if include_assigned_user and user_id:
            if resource in {"quote", "task"}:
                clauses.append("assigned_user_id = ?")
                params.append(user_id)
            if resource == "quote":
                clauses.append(
                    "id IN (SELECT i.quote_id FROM Installation i JOIN Task t ON t.installation_id = i.id WHERE t.assigned_user_id = ?)"
                )
                params.append(user_id)
        if not clauses:
            return "WHERE 1 = 0", []
        return f"WHERE ({' OR '.join(clauses)})", params
    return "WHERE 1 = 0", []


def init_db() -> None:
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Organization(
                id TEXT PRIMARY KEY,
                name TEXT,
                type TEXT,
                domain TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Quote(
                id TEXT PRIMARY KEY,
                company TEXT,
                employer_street TEXT,
                employer_city TEXT,
                state TEXT,
                employer_zip TEXT,
                employer_domain TEXT,
                quote_deadline TEXT,
                employer_sic TEXT,
                effective_date TEXT,
                current_enrolled INTEGER,
                current_eligible INTEGER,
                current_insurance_type TEXT,
                primary_network TEXT,
                secondary_network TEXT,
                tpa TEXT,
                stoploss TEXT,
                current_carrier TEXT,
                renewal_comparison TEXT,
                employees_eligible INTEGER,
                expected_enrollees INTEGER,
                broker_fee_pepm REAL,
                include_specialty INTEGER,
                notes TEXT,
                high_cost_info TEXT,
                broker_first_name TEXT,
                broker_last_name TEXT,
                broker_email TEXT,
                broker_phone TEXT,
                agent_of_record INTEGER,
                broker_org TEXT,
                sponsor_domain TEXT,
                assigned_user_id TEXT,
                manual_network TEXT,
                proposal_url TEXT,
                hubspot_ticket_id TEXT,
                hubspot_ticket_url TEXT,
                hubspot_last_synced_at TEXT,
                hubspot_sync_error TEXT,
                status TEXT,
                version INTEGER,
                needs_action INTEGER,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Upload(
                id TEXT PRIMARY KEY,
                quote_id TEXT,
                type TEXT,
                filename TEXT,
                path TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS StandardizationRun(
                id TEXT PRIMARY KEY,
                quote_id TEXT,
                issues_json TEXT,
                issue_count INTEGER,
                status TEXT,
                standardized_filename TEXT,
                standardized_path TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS AssignmentRun(
                id TEXT PRIMARY KEY,
                quote_id TEXT,
                result_json TEXT,
                recommendation TEXT,
                confidence REAL,
                rationale TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Proposal(
                id TEXT PRIMARY KEY,
                quote_id TEXT,
                filename TEXT,
                path TEXT,
                status TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Installation(
                id TEXT PRIMARY KEY,
                quote_id TEXT,
                company TEXT,
                broker_org TEXT,
                sponsor_domain TEXT,
                effective_date TEXT,
                status TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Task(
                id TEXT PRIMARY KEY,
                installation_id TEXT,
                title TEXT,
                owner TEXT,
                assigned_user_id TEXT,
                due_date TEXT,
                state TEXT,
                task_url TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS Notification(
                id TEXT PRIMARY KEY,
                user_id TEXT,
                kind TEXT,
                title TEXT,
                body TEXT,
                entity_type TEXT,
                entity_id TEXT,
                is_read INTEGER,
                created_at TEXT,
                read_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notification_user_created
            ON Notification(user_id, created_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notification_user_read
            ON Notification(user_id, is_read, created_at DESC)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS User(
                id TEXT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT UNIQUE,
                phone TEXT,
                job_title TEXT,
                organization TEXT,
                role TEXT,
                password_salt TEXT,
                password_hash TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS AccessRequest(
                id TEXT PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                requested_role TEXT,
                organization TEXT,
                requested_domain TEXT,
                status TEXT,
                review_note TEXT,
                created_at TEXT,
                reviewed_at TEXT,
                reviewed_by_user_id TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_access_request_status_created
            ON AccessRequest(status, created_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_access_request_email_created
            ON AccessRequest(email, created_at DESC)
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS InstallationDocument(
                id TEXT PRIMARY KEY,
                installation_id TEXT,
                filename TEXT,
                path TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS AuthMagicLink(
                id TEXT PRIMARY KEY,
                user_id TEXT,
                email TEXT,
                token_hash TEXT,
                expires_at TEXT,
                used_at TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS AuthSession(
                id TEXT PRIMARY KEY,
                user_id TEXT,
                session_hash TEXT,
                expires_at TEXT,
                created_at TEXT,
                last_seen_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS HubSpotOAuthState(
                id TEXT PRIMARY KEY,
                state TEXT UNIQUE,
                redirect_uri TEXT,
                expires_at TEXT,
                created_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS HubSpotTicketAttachmentSync(
                id TEXT PRIMARY KEY,
                upload_id TEXT,
                quote_id TEXT,
                ticket_id TEXT,
                hubspot_file_id TEXT,
                hubspot_note_id TEXT,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_hubspot_attachment_sync_upload_ticket
            ON HubSpotTicketAttachmentSync(upload_id, ticket_id)
            """
        )
        # Lightweight migration for new columns
        cur.execute("PRAGMA table_info(Quote)")
        quote_cols = {row["name"] for row in cur.fetchall()}
        if "broker_org" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN broker_org TEXT")
        if "sponsor_domain" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN sponsor_domain TEXT")
        if "assigned_user_id" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN assigned_user_id TEXT")
        if "manual_network" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN manual_network TEXT")
        if "proposal_url" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN proposal_url TEXT")
        if "hubspot_ticket_id" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN hubspot_ticket_id TEXT")
        if "hubspot_ticket_url" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN hubspot_ticket_url TEXT")
        if "hubspot_last_synced_at" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN hubspot_last_synced_at TEXT")
        if "hubspot_sync_error" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN hubspot_sync_error TEXT")
        if "employer_street" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN employer_street TEXT")
        if "employer_city" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN employer_city TEXT")
        if "employer_zip" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN employer_zip TEXT")
        if "employer_domain" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN employer_domain TEXT")
        if "quote_deadline" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN quote_deadline TEXT")
        if "employer_sic" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN employer_sic TEXT")
        if "current_enrolled" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN current_enrolled INTEGER")
        if "current_eligible" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN current_eligible INTEGER")
        if "current_insurance_type" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN current_insurance_type TEXT")
        if "primary_network" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN primary_network TEXT")
        if "secondary_network" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN secondary_network TEXT")
        if "tpa" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN tpa TEXT")
        if "stoploss" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN stoploss TEXT")
        if "current_carrier" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN current_carrier TEXT")
        if "renewal_comparison" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN renewal_comparison TEXT")
        if "high_cost_info" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN high_cost_info TEXT")
        if "broker_first_name" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN broker_first_name TEXT")
        if "broker_last_name" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN broker_last_name TEXT")
        if "broker_email" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN broker_email TEXT")
        if "broker_phone" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN broker_phone TEXT")
        if "agent_of_record" not in quote_cols:
            cur.execute("ALTER TABLE Quote ADD COLUMN agent_of_record INTEGER")

        cur.execute("PRAGMA table_info(StandardizationRun)")
        std_cols = {row["name"] for row in cur.fetchall()}
        if "standardized_filename" not in std_cols:
            cur.execute("ALTER TABLE StandardizationRun ADD COLUMN standardized_filename TEXT")
        if "standardized_path" not in std_cols:
            cur.execute("ALTER TABLE StandardizationRun ADD COLUMN standardized_path TEXT")

        cur.execute("PRAGMA table_info(Installation)")
        install_cols = {row["name"] for row in cur.fetchall()}
        if "broker_org" not in install_cols:
            cur.execute("ALTER TABLE Installation ADD COLUMN broker_org TEXT")
        if "sponsor_domain" not in install_cols:
            cur.execute("ALTER TABLE Installation ADD COLUMN sponsor_domain TEXT")

        cur.execute("PRAGMA table_info(Task)")
        task_cols = {row["name"] for row in cur.fetchall()}
        if "assigned_user_id" not in task_cols:
            cur.execute("ALTER TABLE Task ADD COLUMN assigned_user_id TEXT")
        if "task_url" not in task_cols:
            cur.execute("ALTER TABLE Task ADD COLUMN task_url TEXT")
        cur.execute("PRAGMA table_info(User)")
        user_cols = {row["name"] for row in cur.fetchall()}
        if "phone" not in user_cols:
            cur.execute("ALTER TABLE User ADD COLUMN phone TEXT")
        if "password_salt" not in user_cols:
            cur.execute("ALTER TABLE User ADD COLUMN password_salt TEXT")
        if "password_hash" not in user_cols:
            cur.execute("ALTER TABLE User ADD COLUMN password_hash TEXT")

        cur.execute("PRAGMA table_info(AccessRequest)")
        access_request_cols = {row["name"] for row in cur.fetchall()}
        if "first_name" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN first_name TEXT")
        if "last_name" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN last_name TEXT")
        if "email" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN email TEXT")
        if "requested_role" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN requested_role TEXT")
        if "organization" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN organization TEXT")
        if "requested_domain" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN requested_domain TEXT")
        if "status" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN status TEXT")
        if "review_note" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN review_note TEXT")
        if "created_at" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN created_at TEXT")
        if "reviewed_at" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN reviewed_at TEXT")
        if "reviewed_by_user_id" not in access_request_cols:
            cur.execute("ALTER TABLE AccessRequest ADD COLUMN reviewed_by_user_id TEXT")
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_access_request_status_created
            ON AccessRequest(status, created_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_access_request_email_created
            ON AccessRequest(email, created_at DESC)
            """
        )

        cur.execute("PRAGMA table_info(AuthMagicLink)")
        magic_cols = {row["name"] for row in cur.fetchall()}
        if "used_at" not in magic_cols:
            cur.execute("ALTER TABLE AuthMagicLink ADD COLUMN used_at TEXT")
        if "created_at" not in magic_cols:
            cur.execute("ALTER TABLE AuthMagicLink ADD COLUMN created_at TEXT")

        cur.execute("PRAGMA table_info(AuthSession)")
        session_cols = {row["name"] for row in cur.fetchall()}
        if "created_at" not in session_cols:
            cur.execute("ALTER TABLE AuthSession ADD COLUMN created_at TEXT")
        if "last_seen_at" not in session_cols:
            cur.execute("ALTER TABLE AuthSession ADD COLUMN last_seen_at TEXT")

        cur.execute("PRAGMA table_info(Notification)")
        notification_cols = {row["name"] for row in cur.fetchall()}
        if "user_id" not in notification_cols:
            cur.execute("ALTER TABLE Notification ADD COLUMN user_id TEXT")
        if "kind" not in notification_cols:
            cur.execute("ALTER TABLE Notification ADD COLUMN kind TEXT")
        if "title" not in notification_cols:
            cur.execute("ALTER TABLE Notification ADD COLUMN title TEXT")
        if "body" not in notification_cols:
            cur.execute("ALTER TABLE Notification ADD COLUMN body TEXT")
        if "entity_type" not in notification_cols:
            cur.execute("ALTER TABLE Notification ADD COLUMN entity_type TEXT")
        if "entity_id" not in notification_cols:
            cur.execute("ALTER TABLE Notification ADD COLUMN entity_id TEXT")
        if "is_read" not in notification_cols:
            cur.execute("ALTER TABLE Notification ADD COLUMN is_read INTEGER")
        if "created_at" not in notification_cols:
            cur.execute("ALTER TABLE Notification ADD COLUMN created_at TEXT")
        if "read_at" not in notification_cols:
            cur.execute("ALTER TABLE Notification ADD COLUMN read_at TEXT")
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notification_user_created
            ON Notification(user_id, created_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_notification_user_read
            ON Notification(user_id, is_read, created_at DESC)
            """
        )

        cur.execute("PRAGMA table_info(HubSpotTicketAttachmentSync)")
        attachment_cols = {row["name"] for row in cur.fetchall()}
        if "hubspot_note_id" not in attachment_cols:
            cur.execute("ALTER TABLE HubSpotTicketAttachmentSync ADD COLUMN hubspot_note_id TEXT")
        if "created_at" not in attachment_cols:
            cur.execute("ALTER TABLE HubSpotTicketAttachmentSync ADD COLUMN created_at TEXT")
        if "updated_at" not in attachment_cols:
            cur.execute("ALTER TABLE HubSpotTicketAttachmentSync ADD COLUMN updated_at TEXT")

        conn.commit()

        cur.execute("SELECT COUNT(*) as cnt FROM Quote")
        if cur.fetchone()["cnt"] == 0:
            seed_data(conn)

        cur.execute("SELECT COUNT(*) as cnt FROM Organization")
        if cur.fetchone()["cnt"] == 0:
            seed_organizations(conn)
        ensure_default_admin_user(conn)
        sync_organizations_from_records(conn)


def ensure_default_admin_user(conn: sqlite3.Connection) -> None:
    email = DEFAULT_ADMIN_EMAIL
    if not email:
        return
    now = now_iso()
    password = DEFAULT_ADMIN_PASSWORD or DEFAULT_USER_PASSWORD
    salt = None
    password_hash = None
    if password:
        salt, password_hash = create_password_credentials(password)
    cur = conn.cursor()
    cur.execute("SELECT * FROM User WHERE email = ?", (email,))
    existing = cur.fetchone()
    if existing:
        if (
            password
            and not (existing["password_salt"] and existing["password_hash"])
        ):
            cur.execute(
                """
                UPDATE User
                SET password_salt = ?, password_hash = ?, updated_at = ?
                WHERE id = ?
                """,
                (salt, password_hash, now, existing["id"]),
            )
            conn.commit()
        return
    cur.execute(
        """
        INSERT INTO User (
            id, first_name, last_name, email, phone, job_title, organization, role,
            password_salt, password_hash, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            DEFAULT_ADMIN_FIRST_NAME,
            DEFAULT_ADMIN_LAST_NAME,
            email,
            "",
            DEFAULT_ADMIN_TITLE,
            DEFAULT_ADMIN_ORGANIZATION,
            "admin",
            salt,
            password_hash,
            now,
            now,
        ),
    )
    conn.commit()


# ----------------------
# Seed data
# ----------------------

def seed_organizations(conn: sqlite3.Connection) -> None:
    now = now_iso()
    cur = conn.cursor()
    for domain, name in BROKER_DOMAIN_MAP.items():
        cur.execute(
            """
            INSERT INTO Organization (id, name, type, domain, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), name, "broker", domain, now),
        )
    cur.execute(
        """
        SELECT DISTINCT sponsor_domain
        FROM Quote
        WHERE sponsor_domain IS NOT NULL AND sponsor_domain != ''
        """
    )
    for row in cur.fetchall():
        domain = row["sponsor_domain"]
        cur.execute(
            """
            INSERT INTO Organization (id, name, type, domain, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), domain, "sponsor", domain, now),
        )
    conn.commit()


def seed_data(conn: sqlite3.Connection) -> None:
    now = now_iso()
    cur = conn.cursor()
    quote_ids = [str(uuid.uuid4()) for _ in range(3)]
    quotes = [
        (
            quote_ids[0],
            "Acme Manufacturing",
            "1200 Market Street",
            "San Diego",
            "CA",
            "92101",
            "acmemfg.com",
            "2026-01-20",
            "7373",
            "2026-04-01",
            84,
            120,
            "Fully Insured PPO",
            120,
            88,
            12.5,
            1,
            "Expanding to two new sites.",
            "Key claimant is in remission; COBRA candidate on kidney care.",
            "Alyssa",
            "Nguyen",
            "alyssa@legacybrokerskc.com",
            "816-555-4421",
            1,
            "Legacy Brokers KC",
            "acmemfg.com",
            "Submitted",
            1,
            1,
            now,
            now,
        ),
        (
            quote_ids[1],
            "Northwind Logistics",
            "200 Commerce Dr",
            "Dallas",
            "TX",
            "75201",
            "northwindlogistics.com",
            "2026-02-01",
            "4213",
            "2026-05-01",
            60,
            80,
            "Level Funded",
            80,
            60,
            10.0,
            0,
            "Looking to compare narrow vs broad networks.",
            "High-cost oncology claimant left plan in December.",
            "Marcus",
            "Reed",
            "marcus@heartlandbenefitpartners.com",
            "214-555-1903",
            0,
            "Heartland Benefit Partners",
            "northwindlogistics.com",
            "In Review",
            1,
            1,
            now,
            now,
        ),
        (
            quote_ids[2],
            "Blue Ridge Hotels",
            "45 Ridgeway Blvd",
            "Denver",
            "CO",
            "80202",
            "blueridgehotels.com",
            "2026-01-10",
            "7011",
            "2026-03-01",
            30,
            45,
            "Fully Insured HMO",
            45,
            30,
            8.0,
            1,
            "Seasonal staff included.",
            "Chronic kidney disease electing COBRA.",
            "Dana",
            "Lopez",
            "dana@legacybrokerskc.com",
            "720-555-8834",
            1,
            "Legacy Brokers KC",
            "blueridgehotels.com",
            "Proposal Ready",
            2,
            0,
            now,
            now,
        ),
    ]

    cur.executemany(
        """
        INSERT INTO Quote (
            id, company, employer_street, employer_city, state, employer_zip,
            employer_domain, quote_deadline, employer_sic, effective_date, current_enrolled,
            current_eligible, current_insurance_type, employees_eligible,
            expected_enrollees, broker_fee_pepm, include_specialty, notes,
            high_cost_info, broker_first_name, broker_last_name, broker_email,
            broker_phone, agent_of_record, broker_org, sponsor_domain, status,
            version, needs_action, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        quotes,
    )

    sample_dir = UPLOADS_DIR / quote_ids[1]
    sample_dir.mkdir(parents=True, exist_ok=True)
    sample_path = sample_dir / "census-sample.csv"
    if not sample_path.exists():
        sample_path.write_text(
            "employee_id,first_name,last_name,zip,age\n1,Jamie,Smith,78701,34\n",
            encoding="utf-8",
        )
    upload_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO Upload (id, quote_id, type, filename, path, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            upload_id,
            quote_ids[1],
            "census",
            "census-sample.csv",
            str(sample_path),
            now,
        ),
    )

    assignment_id = str(uuid.uuid4())
    assignment_result = {
        "ranked_contracts": [
            {"name": "Elevate PPO 3000", "score": 92, "fit": "Strong"},
            {"name": "Prime EPO 2500", "score": 86, "fit": "Good"},
            {"name": "Value HMO 1500", "score": 78, "fit": "Moderate"},
        ],
        "member_fit": {
            "in_network": 82,
            "out_of_network": 12,
            "no_match": 6,
        },
    }
    cur.execute(
        """
        INSERT INTO AssignmentRun (
            id, quote_id, result_json, recommendation, confidence, rationale, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            assignment_id,
            quote_ids[2],
            json.dumps(assignment_result),
            "Elevate PPO 3000",
            0.84,
            "Strong provider overlap in primary and specialty care across all regions.",
            now,
        ),
    )

    proposal_id = str(uuid.uuid4())
    proposal_dir = UPLOADS_DIR / quote_ids[2]
    proposal_dir.mkdir(parents=True, exist_ok=True)
    proposal_path = proposal_dir / "proposal-demo.txt"
    if not proposal_path.exists():
        proposal_path.write_text(
            "Level Health Proposal Demo\nIncludes summary, rates, and network fit.\n",
            encoding="utf-8",
        )
    cur.execute(
        """
        INSERT INTO Proposal (id, quote_id, filename, path, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            proposal_id,
            quote_ids[2],
            "proposal-demo.txt",
            str(proposal_path),
            "Ready",
            now,
        ),
    )

    installation_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO Installation (
            id, quote_id, company, broker_org, sponsor_domain, effective_date, status, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            installation_id,
            quote_ids[2],
            "Blue Ridge Hotels",
            "Legacy Brokers KC",
            "blueridgehotels.com",
            "2026-03-01",
            "In Progress",
            now,
            now,
        ),
    )

    tasks = [
        (str(uuid.uuid4()), installation_id, "Kickoff call", "Broker", "2026-02-12", "In Progress", None),
        (str(uuid.uuid4()), installation_id, "Collect eligibility file", "Employer", "2026-02-15", "Not Started", None),
        (str(uuid.uuid4()), installation_id, "Finalize plan design", "Level Health", "2026-02-20", "Not Started", None),
        (str(uuid.uuid4()), installation_id, "Open enrollment window", "Broker", "2026-02-25", "Not Started", None),
        (str(uuid.uuid4()), installation_id, "Go live", "Level Health", "2026-03-01", "Not Started", None),
    ]
    cur.executemany(
        """
        INSERT INTO Task (id, installation_id, title, owner, due_date, state, task_url)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        tasks,
    )

    conn.commit()


# ----------------------
# Models
# ----------------------

class QuoteCreate(BaseModel):
    company: str
    employer_street: Optional[str] = None
    employer_city: Optional[str] = None
    state: str
    employer_zip: Optional[str] = None
    employer_domain: Optional[str] = None
    quote_deadline: Optional[str] = None
    employer_sic: Optional[str] = None
    effective_date: str
    current_enrolled: int
    current_eligible: int
    current_insurance_type: str
    primary_network: Optional[str] = None
    secondary_network: Optional[str] = None
    tpa: Optional[str] = None
    stoploss: Optional[str] = None
    current_carrier: Optional[str] = None
    renewal_comparison: Optional[str] = None
    employees_eligible: int
    expected_enrollees: int
    broker_fee_pepm: float
    include_specialty: bool = False
    notes: str = ""
    high_cost_info: str = ""
    broker_first_name: Optional[str] = None
    broker_last_name: Optional[str] = None
    broker_email: Optional[str] = None
    broker_phone: Optional[str] = None
    agent_of_record: Optional[bool] = None
    broker_org: Optional[str] = None
    sponsor_domain: Optional[str] = None
    assigned_user_id: Optional[str] = None
    manual_network: Optional[str] = None
    proposal_url: Optional[str] = None
    status: Optional[str] = None


class QuoteUpdate(BaseModel):
    company: Optional[str] = None
    employer_street: Optional[str] = None
    employer_city: Optional[str] = None
    state: Optional[str] = None
    employer_zip: Optional[str] = None
    employer_domain: Optional[str] = None
    quote_deadline: Optional[str] = None
    employer_sic: Optional[str] = None
    effective_date: Optional[str] = None
    current_enrolled: Optional[int] = None
    current_eligible: Optional[int] = None
    current_insurance_type: Optional[str] = None
    primary_network: Optional[str] = None
    secondary_network: Optional[str] = None
    tpa: Optional[str] = None
    stoploss: Optional[str] = None
    current_carrier: Optional[str] = None
    renewal_comparison: Optional[str] = None
    employees_eligible: Optional[int] = None
    expected_enrollees: Optional[int] = None
    broker_fee_pepm: Optional[float] = None
    include_specialty: Optional[bool] = None
    notes: Optional[str] = None
    high_cost_info: Optional[str] = None
    broker_first_name: Optional[str] = None
    broker_last_name: Optional[str] = None
    broker_email: Optional[str] = None
    broker_phone: Optional[str] = None
    agent_of_record: Optional[bool] = None
    broker_org: Optional[str] = None
    sponsor_domain: Optional[str] = None
    assigned_user_id: Optional[str] = None
    manual_network: Optional[str] = None
    proposal_url: Optional[str] = None
    status: Optional[str] = None
    version: Optional[int] = None
    needs_action: Optional[bool] = None


class QuoteOut(BaseModel):
    id: str
    company: str
    employer_street: Optional[str]
    employer_city: Optional[str]
    state: str
    employer_zip: Optional[str]
    employer_domain: Optional[str]
    quote_deadline: Optional[str]
    employer_sic: Optional[str]
    effective_date: str
    current_enrolled: int
    current_eligible: int
    current_insurance_type: str
    primary_network: Optional[str]
    secondary_network: Optional[str]
    tpa: Optional[str]
    stoploss: Optional[str]
    current_carrier: Optional[str]
    renewal_comparison: Optional[str]
    employees_eligible: int
    expected_enrollees: int
    broker_fee_pepm: float
    include_specialty: bool
    notes: str
    high_cost_info: str
    broker_first_name: Optional[str]
    broker_last_name: Optional[str]
    broker_email: Optional[str]
    broker_phone: Optional[str]
    agent_of_record: Optional[bool]
    broker_org: Optional[str]
    sponsor_domain: Optional[str]
    assigned_user_id: Optional[str]
    manual_network: Optional[str]
    proposal_url: Optional[str]
    hubspot_ticket_id: Optional[str]
    hubspot_ticket_url: Optional[str]
    hubspot_last_synced_at: Optional[str]
    hubspot_sync_error: Optional[str]
    status: str
    version: int
    needs_action: bool
    created_at: str
    updated_at: str


class QuoteListOut(QuoteOut):
    latest_assignment: Optional[Dict[str, Any]] = None


class UploadOut(BaseModel):
    id: str
    quote_id: str
    type: str
    filename: str
    path: str
    created_at: str


class AssignmentOut(BaseModel):
    id: str
    quote_id: str
    result_json: Dict[str, Any]
    recommendation: str
    confidence: float
    rationale: str
    created_at: str


class StandardizationOut(BaseModel):
    id: str
    quote_id: str
    issues_json: List[Dict[str, Any]]
    issue_count: int
    status: str
    detected_headers: List[str]
    sample_data: Dict[str, List[str]]
    sample_rows: List[Dict[str, Any]]
    total_rows: int
    issue_rows: int
    standardized_filename: Optional[str]
    standardized_path: Optional[str]
    created_at: str


class StandardizationIn(BaseModel):
    gender_map: Optional[Dict[str, str]] = None
    relationship_map: Optional[Dict[str, str]] = None
    tier_map: Optional[Dict[str, str]] = None
    header_map: Optional[Dict[str, str]] = None


class StandardizationResolveIn(BaseModel):
    issues_json: List[Dict[str, Any]]


class ProposalOut(BaseModel):
    id: str
    quote_id: str
    filename: str
    path: str
    status: str
    created_at: str


class InstallationOut(BaseModel):
    id: str
    quote_id: str
    company: str
    broker_org: Optional[str]
    sponsor_domain: Optional[str]
    effective_date: str
    status: str
    created_at: str
    updated_at: str


class TaskOut(BaseModel):
    id: str
    installation_id: str
    title: str
    owner: str
    assigned_user_id: Optional[str]
    due_date: Optional[str]
    state: str
    task_url: Optional[str]


class TaskUpdateIn(BaseModel):
    state: Optional[str] = None
    task_url: Optional[str] = None
    due_date: Optional[str] = None
    assigned_user_id: Optional[str] = None


class StoplossDisclosureLaunchIn(BaseModel):
    selected_url: str


class StoplossDisclosureLaunchOut(BaseModel):
    status: str
    open_url: str
    created_via_api: bool


class TaskListOut(TaskOut):
    installation_company: Optional[str] = None


class InstallationDocumentOut(BaseModel):
    id: str
    installation_id: str
    filename: str
    path: str
    created_at: str


class InstallationRegressOut(BaseModel):
    status: str
    installation_id: str
    quote_id: str
    quote_status: str


class InstallationOrgBackfillOut(BaseModel):
    status: str
    scanned_installation_count: int
    updated_installation_count: int
    updated_broker_org_count: int
    updated_sponsor_domain_count: int


class NotificationOut(BaseModel):
    id: str
    user_id: str
    kind: str
    title: str
    body: str
    entity_type: Optional[str]
    entity_id: Optional[str]
    is_read: bool
    created_at: str
    read_at: Optional[str]


class NotificationUnreadCountOut(BaseModel):
    unread_count: int


class OrganizationIn(BaseModel):
    name: str
    type: str
    domain: str


class OrganizationOut(BaseModel):
    id: str
    name: str
    type: str
    domain: str
    created_at: str


class OrganizationUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    domain: Optional[str] = None


class OrganizationAssignIn(BaseModel):
    quote_ids: List[str]


class UserIn(BaseModel):
    first_name: str
    last_name: str
    email: str
    phone: str = ""
    job_title: str
    organization: str
    role: str
    password: Optional[str] = None


class UserOut(BaseModel):
    id: str
    first_name: str
    last_name: str
    email: str
    phone: str = ""
    job_title: str
    organization: str
    role: str
    created_at: str
    updated_at: str


class UserUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    job_title: Optional[str] = None
    organization: Optional[str] = None
    role: Optional[str] = None
    password: Optional[str] = None


class UserAssignIn(BaseModel):
    quote_ids: List[str] = []
    task_ids: List[str] = []


class AuthRequestIn(BaseModel):
    email: str


class AccessRequestIn(BaseModel):
    first_name: str
    last_name: str
    email: str
    requested_role: str = "broker"
    organization: Optional[str] = None


class AccessRequestOut(BaseModel):
    status: str
    message: str


class AccessRequestAdminOut(BaseModel):
    id: str
    first_name: str
    last_name: str
    email: str
    requested_role: str
    organization: Optional[str]
    requested_domain: Optional[str]
    status: str
    review_note: Optional[str]
    created_at: str
    reviewed_at: Optional[str]
    reviewed_by_user_id: Optional[str]


class AccessRequestDecisionIn(BaseModel):
    role: Optional[str] = None
    organization: Optional[str] = None
    note: Optional[str] = None


class AuthLoginIn(BaseModel):
    email: str
    password: str


class AuthVerifyOut(BaseModel):
    email: str
    role: str
    first_name: str
    last_name: str
    organization: str


class AuthProfileOut(BaseModel):
    email: str
    role: str
    first_name: str
    last_name: str
    organization: str
    phone: str
    job_title: str


class AuthProfileUpdateIn(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None
    job_title: Optional[str] = None
    password: Optional[str] = None


# ----------------------
# Utility functions
# ----------------------

def fetch_quote(conn: sqlite3.Connection, quote_id: str) -> sqlite3.Row:
    cur = conn.cursor()
    cur.execute("SELECT * FROM Quote WHERE id = ?", (quote_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Quote not found")
    return row


def fetch_user(conn: sqlite3.Connection, user_id: str) -> sqlite3.Row:
    cur = conn.cursor()
    cur.execute("SELECT * FROM User WHERE id = ?", (user_id,))
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return row


def to_user_out(row: sqlite3.Row) -> UserOut:
    data = dict(row)
    data["phone"] = data.get("phone") or ""
    return UserOut(**data)


def to_notification_out(row: sqlite3.Row) -> NotificationOut:
    data = dict(row)
    data["is_read"] = bool(data.get("is_read"))
    return NotificationOut(**data)


def to_access_request_admin_out(row: sqlite3.Row) -> AccessRequestAdminOut:
    data = dict(row)
    return AccessRequestAdminOut(**data)


def ensure_access_request_user(
    conn: sqlite3.Connection,
    *,
    first_name: str,
    last_name: str,
    email: str,
    role: str,
    organization: str,
) -> sqlite3.Row:
    normalized_email = normalize_user_email(email)
    normalized_role = normalize_user_role(role)
    if normalized_role not in {"broker", "sponsor"}:
        raise HTTPException(status_code=400, detail="Access request users must be broker or sponsor")
    now = now_iso()
    cur = conn.cursor()
    existing = fetch_user_by_email(conn, normalized_email)
    if existing:
        next_role = existing["role"] if (existing["role"] or "").strip().lower() == "admin" else normalized_role
        next_org = organization.strip() or (existing["organization"] or "").strip()
        cur.execute(
            """
            UPDATE User
            SET first_name = ?, last_name = ?, organization = ?, role = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                first_name.strip(),
                last_name.strip(),
                next_org,
                next_role,
                now,
                existing["id"],
            ),
        )
        cur.execute("SELECT * FROM User WHERE id = ?", (existing["id"],))
        updated = cur.fetchone()
        if not updated:
            raise HTTPException(status_code=500, detail="Failed to update access request user")
        return updated

    user_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO User (
            id, first_name, last_name, email, phone, job_title, organization, role,
            password_salt, password_hash, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            first_name.strip(),
            last_name.strip(),
            normalized_email,
            "",
            "",
            organization.strip(),
            normalized_role,
            None,
            None,
            now,
            now,
        ),
    )
    cur.execute("SELECT * FROM User WHERE id = ?", (user_id,))
    created = cur.fetchone()
    if not created:
        raise HTTPException(status_code=500, detail="Failed to create access request user")
    return created


def create_notification(
    conn: sqlite3.Connection,
    user_id: Optional[str],
    *,
    kind: str,
    title: str,
    body: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
) -> None:
    user_key = (user_id or "").strip()
    if not user_key:
        return
    cur = conn.cursor()
    cur.execute("SELECT id, email FROM User WHERE id = ?", (user_key,))
    user_row = cur.fetchone()
    if not user_row:
        return
    cur.execute(
        """
        INSERT INTO Notification (
            id, user_id, kind, title, body, entity_type, entity_id, is_read, created_at, read_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            user_key,
            kind.strip(),
            title.strip(),
            body.strip(),
            (entity_type or "").strip() or None,
            (entity_id or "").strip() or None,
            0,
            now_iso(),
            None,
        ),
    )
    recipient_email = str(user_row["email"] or "").strip().lower()
    if recipient_email:
        send_resend_notification_email(
            recipient_email,
            title=title,
            body=body,
            entity_type=entity_type,
            entity_id=entity_id,
        )


def auth_user_payload(row: sqlite3.Row) -> AuthVerifyOut:
    return AuthVerifyOut(
        email=row["email"],
        role=row["role"],
        first_name=row["first_name"],
        last_name=row["last_name"],
        organization=row["organization"],
    )


def auth_profile_payload(row: sqlite3.Row) -> AuthProfileOut:
    return AuthProfileOut(
        email=row["email"],
        role=row["role"],
        first_name=row["first_name"],
        last_name=row["last_name"],
        organization=row["organization"],
        phone=(row["phone"] or "").strip(),
        job_title=(row["job_title"] or "").strip(),
    )


def session_user_id(session_user: Any) -> Optional[str]:
    if not session_user:
        return None
    raw_user_id = None
    try:
        raw_user_id = session_user["user_id"]
    except Exception:
        raw_user_id = None
    if not raw_user_id:
        try:
            raw_user_id = session_user["id"]
        except Exception:
            raw_user_id = None
    user_id = str(raw_user_id or "").strip()
    return user_id or None


def sha256_hex(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def hash_password(password: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        120000,
    ).hex()


def create_password_credentials(password: str) -> tuple[str, str]:
    salt = secrets.token_hex(16)
    return salt, hash_password(password, salt)


def verify_password(password: str, salt: Optional[str], expected_hash: Optional[str]) -> bool:
    if not password or not salt or not expected_hash:
        return False
    actual_hash = hash_password(password, salt)
    return secrets.compare_digest(actual_hash, expected_hash)


def normalize_user_email(email: Optional[str]) -> str:
    value = (email or "").strip().lower()
    if not value or "@" not in value:
        raise HTTPException(status_code=400, detail="A valid email is required")
    return value


def normalize_user_role(role: Optional[str]) -> str:
    value = (role or "").strip().lower()
    if value not in ALLOWED_USER_ROLES:
        allowed = ", ".join(sorted(ALLOWED_USER_ROLES))
        raise HTTPException(status_code=400, detail=f"Role must be one of: {allowed}")
    return value


def normalize_access_request_role(role: Optional[str]) -> str:
    value = (role or "").strip().lower() or "broker"
    if value not in ALLOWED_ACCESS_REQUEST_ROLES:
        allowed = ", ".join(sorted(ALLOWED_ACCESS_REQUEST_ROLES))
        raise HTTPException(status_code=400, detail=f"Requested role must be one of: {allowed}")
    return value


def normalize_access_request_status(status: Optional[str]) -> str:
    value = (status or "").strip().lower()
    if value not in ALLOWED_ACCESS_REQUEST_STATUSES:
        allowed = ", ".join(sorted(ALLOWED_ACCESS_REQUEST_STATUSES))
        raise HTTPException(status_code=400, detail=f"Status must be one of: {allowed}")
    return value


def require_valid_password(password: Optional[str], *, required: bool) -> Optional[str]:
    value = (password or "").strip()
    if not value:
        if required:
            raise HTTPException(status_code=400, detail="Password is required")
        return None
    if len(value) < PASSWORD_MIN_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"Password must be at least {PASSWORD_MIN_LENGTH} characters",
        )
    return value


def revoke_user_sessions(conn: sqlite3.Connection, user_id: str) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM AuthSession WHERE user_id = ?", (user_id,))


def parse_resend_error_message(exc: Exception) -> str:
    default_message = "Failed to send email."
    if isinstance(exc, urlerror.HTTPError):
        raw_body = ""
        try:
            raw_body = exc.read().decode("utf-8", errors="ignore").strip()
        except Exception:
            raw_body = ""
        lowered_body = raw_body.lower()
        if "error code 1010" in lowered_body or "error 1010" in lowered_body:
            ray_id_match = re.search(r"ray id[:\s]+([a-z0-9\-]+)", raw_body, flags=re.IGNORECASE)
            ray_id = ray_id_match.group(1).strip() if ray_id_match else ""
            if ray_id:
                return (
                    "Resend request was blocked by Cloudflare (error 1010). "
                    f"Ray ID: {ray_id}. Contact Resend support and provide this Ray ID."
                )
            return (
                "Resend request was blocked by Cloudflare (error 1010). "
                "Contact Resend support for allowlisting."
            )
        if raw_body:
            try:
                parsed = json.loads(raw_body)
                if isinstance(parsed, dict):
                    for key in ("message", "detail", "error"):
                        value = parsed.get(key)
                        if isinstance(value, str) and value.strip():
                            return value.strip()
                        if isinstance(value, dict):
                            nested = value.get("message")
                            if isinstance(nested, str) and nested.strip():
                                return nested.strip()
            except Exception:
                pass
            if len(raw_body) <= 300:
                return raw_body
        if getattr(exc, "reason", None):
            return f"{default_message} {str(exc.reason).strip()}"
    return default_message


def send_resend_email(
    *,
    to_email: str,
    subject: str,
    html_body: str,
    text_body: Optional[str] = None,
    from_email: Optional[str] = None,
    raise_delivery_error: bool = False,
) -> bool:
    api_key = os.getenv("RESEND_API_KEY", "").strip()
    sender_email = (
        (from_email or "").strip()
        or os.getenv("RESEND_FROM_EMAIL", "").strip()
    )
    if not api_key or not sender_email:
        return False
    payload = {
        "from": sender_email,
        "to": [to_email],
        "subject": subject,
        "html": html_body,
    }
    if text_body:
        payload["text"] = text_body
    req = urlrequest.Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "LevelHealthBrokerPortal/1.0",
        },
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=10) as resp:
            if resp.status >= 300:
                if raise_delivery_error:
                    raise HTTPException(status_code=502, detail="Failed to send email.")
                return False
    except urlerror.HTTPError as exc:
        if raise_delivery_error:
            raise HTTPException(status_code=502, detail=parse_resend_error_message(exc))
        return False
    except HTTPException:
        if raise_delivery_error:
            raise
        return False
    except Exception as exc:
        if raise_delivery_error:
            raise HTTPException(status_code=502, detail=parse_resend_error_message(exc))
        return False
    return True


def send_resend_magic_link(to_email: str, link: str) -> bool:
    safe_link = html.escape(link, quote=True)
    sender = os.getenv("RESEND_MAGIC_LINK_FROM_EMAIL", "").strip() or None
    return send_resend_email(
        to_email=to_email,
        subject="Your Level Health sign-in link",
        html_body=(
            "<p>Use this secure sign-in link:</p>"
            f"<p><a href=\"{safe_link}\">{safe_link}</a></p>"
            f"<p>This link expires in {MAGIC_LINK_DURATION_MINUTES} minutes.</p>"
        ),
        text_body=(
            "Use this secure sign-in link:\n"
            f"{link}\n\n"
            f"This link expires in {MAGIC_LINK_DURATION_MINUTES} minutes."
        ),
        from_email=sender,
        raise_delivery_error=True,
    )


def notification_entity_href(entity_type: Optional[str], entity_id: Optional[str]) -> Optional[str]:
    entity_key = (entity_type or "").strip().lower()
    entity_value = (entity_id or "").strip()
    if not entity_value:
        return None
    if entity_key == "quote":
        return f"{FRONTEND_BASE_URL}/quotes/{entity_value}"
    if entity_key == "installation":
        return f"{FRONTEND_BASE_URL}/implementations/{entity_value}"
    return None


def send_resend_notification_email(
    to_email: str,
    *,
    title: str,
    body: str,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
) -> bool:
    if not RESEND_NOTIFICATION_EMAILS_ENABLED:
        return False
    sender = os.getenv("RESEND_NOTIFICATION_FROM_EMAIL", "").strip() or None
    safe_title = html.escape(title.strip() or "Level Health notification")
    safe_body = html.escape(body.strip())
    href = notification_entity_href(entity_type, entity_id)
    safe_href = html.escape(href, quote=True) if href else None
    html_body_parts = [
        "<p>You have a new Level Health notification.</p>",
        f"<p><strong>{safe_title}</strong></p>",
        f"<p>{safe_body}</p>",
    ]
    if safe_href:
        html_body_parts.append(f"<p><a href=\"{safe_href}\">Open in Broker Portal</a></p>")

    text_body = f"{title.strip() or 'Level Health notification'}\n\n{body.strip()}"
    if href:
        text_body = f"{text_body}\n\nOpen in Broker Portal: {href}"

    return send_resend_email(
        to_email=to_email,
        subject=f"Level Health notification: {title.strip() or 'Update'}",
        html_body="".join(html_body_parts),
        text_body=text_body,
        from_email=sender,
        raise_delivery_error=False,
    )


def create_auth_session(conn: sqlite3.Connection, user_id: str) -> str:
    token = secrets.token_urlsafe(32)
    session_hash = sha256_hex(token)
    now = now_iso()
    expires_at = (datetime.utcnow() + timedelta(hours=SESSION_DURATION_HOURS)).isoformat()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO AuthSession (id, user_id, session_hash, expires_at, created_at, last_seen_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (str(uuid.uuid4()), user_id, session_hash, expires_at, now, now),
    )
    conn.commit()
    return token


def get_session_user(conn: sqlite3.Connection, request: Request) -> Optional[sqlite3.Row]:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not token:
        return None
    session_hash = sha256_hex(token)
    now = now_iso()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.*, u.*
        FROM AuthSession s
        JOIN User u ON u.id = s.user_id
        WHERE s.session_hash = ? AND s.expires_at > ?
        ORDER BY s.created_at DESC
        LIMIT 1
        """,
        (session_hash, now),
    )
    row = cur.fetchone()
    if not row:
        return None
    cur.execute(
        "UPDATE AuthSession SET last_seen_at = ? WHERE session_hash = ?",
        (now_iso(), session_hash),
    )
    conn.commit()
    return row


def require_session_user(conn: sqlite3.Connection, request: Request) -> sqlite3.Row:
    user = get_session_user(conn, request)
    if not user:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


def require_session_role(
    conn: sqlite3.Connection, request: Request, allowed_roles: set[str]
) -> sqlite3.Row:
    user = require_session_user(conn, request)
    if (user["role"] or "").strip().lower() not in allowed_roles:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return user


def resolve_access_scope(
    conn: sqlite3.Connection,
    request: Request,
    role: Optional[str],
    email: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    # Prefer authenticated session identity over client-supplied query params.
    session_user = get_session_user(conn, request)
    if session_user:
        return session_user["role"], session_user["email"]
    if ALLOW_QUERY_AUTH_FALLBACK:
        return role, email
    raise HTTPException(status_code=401, detail="Authentication required")


def latest_census_upload(conn: sqlite3.Connection, quote_id: str) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM Upload
        WHERE quote_id = ? AND type = 'census'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (quote_id,),
    )
    return cur.fetchone()


def load_census_rows(path: Path) -> tuple[List[str], List[Dict[str, Any]]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            sample = f.read(2048)
            f.seek(0)
            delimiter = ","
            try:
                delimiter = csv.Sniffer().sniff(sample).delimiter
            except csv.Error:
                delimiter = ","
            reader = csv.DictReader(f, delimiter=delimiter)
            if not reader.fieldnames:
                raise HTTPException(status_code=400, detail="Census file has no header row")
            rows = [dict(row) for row in reader]
            return list(reader.fieldnames), rows
    if suffix == ".xlsx":
        wb = openpyxl.load_workbook(path, data_only=True)
        ws = wb.active
        rows_iter = list(ws.iter_rows(values_only=True))
        if not rows_iter:
            raise HTTPException(status_code=400, detail="Census file has no rows")
        headers = [str(cell).strip() if cell is not None else "" for cell in rows_iter[0]]
        if not any(headers):
            raise HTTPException(status_code=400, detail="Census file has no header row")
        rows = []
        for row in rows_iter[1:]:
            row_dict: Dict[str, Any] = {}
            for idx, header in enumerate(headers):
                if header == "":
                    continue
                value = row[idx] if idx < len(row) else ""
                row_dict[header] = "" if value is None else str(value)
            rows.append(row_dict)
        return headers, rows
    if suffix == ".xls":
        book = xlrd.open_workbook(path)
        sheet = book.sheet_by_index(0)
        if sheet.nrows == 0:
            raise HTTPException(status_code=400, detail="Census file has no rows")
        headers = [str(cell.value).strip() for cell in sheet.row(0)]
        if not any(headers):
            raise HTTPException(status_code=400, detail="Census file has no header row")
        rows = []
        for r in range(1, sheet.nrows):
            row_dict: Dict[str, Any] = {}
            for c, header in enumerate(headers):
                if header == "":
                    continue
                value = sheet.cell_value(r, c)
                row_dict[header] = "" if value is None else str(value)
            rows.append(row_dict)
        return headers, rows
    raise HTTPException(
        status_code=400,
        detail="Unsupported file type. Please upload a .csv or .xls/.xlsx file.",
    )


def normalize_census_dob(value: str) -> tuple[bool, str]:
    raw = (value or "").strip()
    if not raw:
        return False, raw

    today_date = datetime.utcnow().date()

    # Handle Excel date serial values commonly found in exported census files.
    # Example: 26076 -> 1971-05-18.
    if re.fullmatch(r"\d+(?:\.\d+)?", raw):
        try:
            serial_value = float(raw)
            parsed_date = date(1899, 12, 30) + timedelta(days=int(serial_value))
            if date(1900, 1, 1) <= parsed_date <= today_date:
                return True, parsed_date.isoformat()
        except Exception:
            pass

    date_only_formats = (
        ("%m/%d/%Y", False),
        ("%m/%d/%y", True),
        ("%Y-%m-%d", False),
        ("%Y/%m/%d", False),
    )
    for fmt, is_two_digit_year in date_only_formats:
        try:
            parsed_date = datetime.strptime(raw, fmt).date()
            if is_two_digit_year and parsed_date > today_date:
                parsed_date = parsed_date.replace(year=parsed_date.year - 100)
            return True, parsed_date.isoformat()
        except ValueError:
            continue

    datetime_formats = (
        ("%m/%d/%Y %H:%M", False),
        ("%m/%d/%Y %H:%M:%S", False),
        ("%m/%d/%y %H:%M", True),
        ("%m/%d/%y %H:%M:%S", True),
        ("%Y-%m-%d %H:%M", False),
        ("%Y-%m-%d %H:%M:%S", False),
        ("%Y/%m/%d %H:%M", False),
        ("%Y/%m/%d %H:%M:%S", False),
    )
    for fmt, is_two_digit_year in datetime_formats:
        try:
            parsed = datetime.strptime(raw, fmt)
            parsed_date = parsed.date()
            if is_two_digit_year and parsed_date > today_date:
                parsed_date = parsed_date.replace(year=parsed_date.year - 100)
            return True, parsed_date.isoformat()
        except ValueError:
            continue

    # Handle ISO-style datetimes (for example: 1968-01-26T00:00:00).
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return True, parsed.date().isoformat()
    except ValueError:
        pass

    # Handle common export values like "YYYY-MM-DD 00:00:00" by parsing the first token.
    first_token = raw.split()[0] if " " in raw else raw
    for fmt, is_two_digit_year in date_only_formats:
        try:
            parsed_date = datetime.strptime(first_token, fmt).date()
            if is_two_digit_year and parsed_date > today_date:
                parsed_date = parsed_date.replace(year=parsed_date.year - 100)
            return True, parsed_date.isoformat()
        except ValueError:
            continue

    return False, raw


def normalize_zip(value: str) -> Optional[str]:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) != 5:
        return None
    return digits


def normalize_member_zip(value: str) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    if re.fullmatch(r"\d{5}", text):
        return text
    if re.fullmatch(r"\d{5}-\d{4}", text):
        return text[:5]
    if re.fullmatch(r"\d{9}", text):
        return text[:5]
    return None


def resolve_zip_header(headers: List[str]) -> Optional[str]:
    aliases = ["zip", "zipcode", "zip code", "postal code"]
    header_map = {h.lower().replace(" ", ""): h for h in headers}
    for alias in aliases:
        key = alias.lower().replace(" ", "")
        if key in header_map:
            return header_map[key]
    return None


def compute_network_assignment(
    rows: List[Dict[str, Any]],
    zip_header: str,
    mapping: Dict[str, str],
    default_network: str,
    coverage_threshold: float,
) -> Dict[str, Any]:
    member_assignments: List[Dict[str, Any]] = []
    network_counts: Dict[str, int] = {}
    invalid_rows: List[Dict[str, Any]] = []

    for idx, row in enumerate(rows, start=2):
        if all((str(value).strip() == "" for value in row.values())):
            continue
        raw_zip = str(row.get(zip_header, "")).strip()
        normalized = normalize_member_zip(raw_zip)
        if not normalized:
            invalid_rows.append({"row": idx, "zip": raw_zip, "error": "Invalid ZIP"})
            continue
        network = mapping.get(normalized, default_network)
        matched = normalized in mapping
        network_counts[network] = network_counts.get(network, 0) + 1
        member_assignments.append(
            {
                "row": idx,
                "zip": normalized,
                "assigned_network": network,
                "matched": matched,
            }
        )

    total_valid = sum(network_counts.values())
    coverage_by_network = {
        net: (network_counts[net] / total_valid) if total_valid else 0
        for net in sorted(network_counts.keys())
    }

    direct_counts = {
        net: count
        for net, count in network_counts.items()
        if net != default_network
    }
    direct_sorted = sorted(
        direct_counts.items(),
        key=lambda item: (-item[1], item[0]),
    )

    census_incomplete = bool(invalid_rows)
    review_required = census_incomplete
    primary_network = default_network
    coverage_percentage = coverage_by_network.get(default_network, 0)
    if total_valid == 0:
        review_required = True
        coverage_percentage = 0
    elif len(direct_sorted) >= 2:
        first_net, first_count = direct_sorted[0]
        second_net, second_count = direct_sorted[1]
        if first_count / total_valid > 0.40 and second_count / total_valid > 0.40:
            review_required = True
            primary_network = "MIXED_NETWORK"
            coverage_percentage = 0
        elif first_count / total_valid >= coverage_threshold:
            primary_network = first_net
            coverage_percentage = first_count / total_valid
    elif len(direct_sorted) == 1:
        first_net, first_count = direct_sorted[0]
        if first_count / total_valid >= coverage_threshold:
            primary_network = first_net
            coverage_percentage = first_count / total_valid

    fallback_used = primary_network == default_network
    result = {
        "group_summary": {
            "primary_network": primary_network,
            "coverage_percentage": coverage_percentage,
            "fallback_used": fallback_used,
            "review_required": review_required,
            "census_incomplete": census_incomplete,
            "total_members": total_valid,
            "invalid_rows": invalid_rows,
        },
        "coverage_by_network": coverage_by_network,
        "member_assignments": member_assignments,
    }
    return result


def parse_assignment_primary_network(result_json: Any) -> Optional[str]:
    if not isinstance(result_json, dict):
        return None
    group_summary = result_json.get("group_summary")
    if isinstance(group_summary, dict):
        primary = str(group_summary.get("primary_network") or "").strip()
        if primary:
            return primary
    primary = str(result_json.get("primary_network") or "").strip()
    return primary or None


def latest_assignment_primary_network(
    conn: sqlite3.Connection,
    quote_id: str,
) -> Optional[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT result_json, recommendation
        FROM AssignmentRun
        WHERE quote_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (quote_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    parsed_result: Dict[str, Any] = {}
    try:
        parsed = json.loads(row["result_json"] or "{}")
        if isinstance(parsed, dict):
            parsed_result = parsed
    except Exception:
        parsed_result = {}
    primary_from_result = parse_assignment_primary_network(parsed_result)
    if primary_from_result:
        return primary_from_result
    recommendation = str(row["recommendation"] or "").strip()
    return recommendation or None


def load_network_mapping(mapping_path: Path) -> Dict[str, str]:
    if not mapping_path.exists():
        raise HTTPException(status_code=500, detail="Network mapping file not found")
    mapping: Dict[str, str] = {}
    with mapping_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "zip" not in reader.fieldnames or "network" not in reader.fieldnames:
            raise HTTPException(status_code=500, detail="Mapping file must include zip,network columns")
        for row in reader:
            zip_value = normalize_zip(str(row.get("zip", "")))
            network = (row.get("network") or "").strip()
            if zip_value and network:
                mapping[zip_value] = network
    return mapping


def read_network_mappings() -> List[Dict[str, str]]:
    mapping_path = (BASE_DIR / "data" / "network_mappings.csv").resolve()
    if not mapping_path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with mapping_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "zip" not in reader.fieldnames or "network" not in reader.fieldnames:
            return []
        for row in reader:
            zip_value = normalize_zip(str(row.get("zip", "")))
            network = (row.get("network") or "").strip()
            if zip_value and network:
                rows.append({"zip": zip_value, "network": network})
    rows.sort(key=lambda item: item["zip"])
    return rows


def write_network_mappings(rows: List[Dict[str, str]]) -> None:
    mapping_path = (BASE_DIR / "data" / "network_mappings.csv").resolve()
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    dedup: Dict[str, str] = {}
    for row in rows:
        zip_value = normalize_zip(str(row.get("zip", "")))
        network = (row.get("network") or "").strip()
        if zip_value and network:
            dedup[zip_value] = network
    with mapping_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["zip", "network"])
        writer.writeheader()
        for zip_value in sorted(dedup.keys()):
            writer.writerow({"zip": zip_value, "network": dedup[zip_value]})


def remove_upload_file(path_value: Optional[str]) -> None:
    if not path_value:
        return
    try:
        file_path = Path(path_value).expanduser().resolve()
    except Exception:
        return
    try:
        uploads_root = UPLOADS_DIR.resolve()
    except Exception:
        uploads_root = UPLOADS_DIR
    if uploads_root not in file_path.parents:
        return
    try:
        file_path.unlink(missing_ok=True)
    except Exception:
        return
    parent = file_path.parent
    while parent != uploads_root and uploads_root in parent.parents:
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


def delete_installation_with_dependencies(
    conn: sqlite3.Connection,
    installation_id: str,
) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute("SELECT * FROM Installation WHERE id = ?", (installation_id,))
    installation = cur.fetchone()
    if not installation:
        raise HTTPException(status_code=404, detail="Installation not found")

    quote_id = str(installation["quote_id"] or "").strip()
    files_to_remove: List[str] = []

    cur.execute(
        "SELECT path FROM InstallationDocument WHERE installation_id = ?",
        (installation_id,),
    )
    files_to_remove.extend(
        [row["path"] for row in cur.fetchall() if row["path"]]
    )

    cur.execute(
        "SELECT COUNT(*) AS cnt FROM Task WHERE installation_id = ?",
        (installation_id,),
    )
    deleted_task_count = int(cur.fetchone()["cnt"] or 0)

    cur.execute("DELETE FROM InstallationDocument WHERE installation_id = ?", (installation_id,))
    cur.execute("DELETE FROM Task WHERE installation_id = ?", (installation_id,))
    cur.execute("DELETE FROM Installation WHERE id = ?", (installation_id,))

    return {
        "installation_id": installation_id,
        "quote_id": quote_id,
        "files_to_remove": files_to_remove,
        "deleted_task_count": deleted_task_count,
    }


def delete_quote_with_dependencies(conn: sqlite3.Connection, quote_id: str) -> Dict[str, Any]:
    cur = conn.cursor()
    files_to_remove: List[str] = []
    installation_ids: List[str] = []
    deleted_task_count = 0
    upload_ids: List[str] = []

    cur.execute("SELECT id, path FROM Upload WHERE quote_id = ?", (quote_id,))
    upload_rows = cur.fetchall()
    upload_ids = [str(row["id"] or "").strip() for row in upload_rows if str(row["id"] or "").strip()]
    files_to_remove.extend(
        [row["path"] for row in upload_rows if row["path"]]
    )

    cur.execute("SELECT path FROM Proposal WHERE quote_id = ?", (quote_id,))
    files_to_remove.extend(
        [row["path"] for row in cur.fetchall() if row["path"]]
    )

    cur.execute(
        "SELECT standardized_path FROM StandardizationRun WHERE quote_id = ?",
        (quote_id,),
    )
    files_to_remove.extend(
        [row["standardized_path"] for row in cur.fetchall() if row["standardized_path"]]
    )

    cur.execute("SELECT id FROM Installation WHERE quote_id = ?", (quote_id,))
    installation_ids = [row["id"] for row in cur.fetchall()]

    if installation_ids:
        placeholders = ",".join(["?"] * len(installation_ids))
        cur.execute(
            f"SELECT COUNT(*) AS cnt FROM Task WHERE installation_id IN ({placeholders})",
            installation_ids,
        )
        deleted_task_count = int(cur.fetchone()["cnt"] or 0)
        cur.execute(
            f"SELECT path FROM InstallationDocument WHERE installation_id IN ({placeholders})",
            installation_ids,
        )
        files_to_remove.extend(
            [row["path"] for row in cur.fetchall() if row["path"]]
        )
        cur.execute(
            f"DELETE FROM InstallationDocument WHERE installation_id IN ({placeholders})",
            installation_ids,
        )
        cur.execute(
            f"DELETE FROM Task WHERE installation_id IN ({placeholders})",
            installation_ids,
        )
        cur.execute(
            f"DELETE FROM Installation WHERE id IN ({placeholders})",
            installation_ids,
        )

    cur.execute("DELETE FROM Upload WHERE quote_id = ?", (quote_id,))
    if upload_ids:
        placeholders = ",".join(["?"] * len(upload_ids))
        cur.execute(
            f"DELETE FROM HubSpotTicketAttachmentSync WHERE upload_id IN ({placeholders})",
            upload_ids,
        )
    cur.execute("DELETE FROM HubSpotTicketAttachmentSync WHERE quote_id = ?", (quote_id,))
    cur.execute("DELETE FROM StandardizationRun WHERE quote_id = ?", (quote_id,))
    cur.execute("DELETE FROM AssignmentRun WHERE quote_id = ?", (quote_id,))
    cur.execute("DELETE FROM Proposal WHERE quote_id = ?", (quote_id,))
    cur.execute("DELETE FROM Quote WHERE id = ?", (quote_id,))

    return {
        "files_to_remove": files_to_remove,
        "installation_ids": installation_ids,
        "deleted_task_count": deleted_task_count,
    }


def remove_quote_artifacts(
    quote_id: str,
    installation_ids: List[str],
    files_to_remove: List[str],
) -> None:
    for path_value in files_to_remove:
        remove_upload_file(path_value)

    quote_dir = UPLOADS_DIR / quote_id
    try:
        if quote_dir.exists():
            shutil.rmtree(quote_dir, ignore_errors=True)
    except Exception:
        pass

    for installation_id in installation_ids:
        install_dir = UPLOADS_DIR / f"installation-{installation_id}"
        try:
            if install_dir.exists():
                shutil.rmtree(install_dir, ignore_errors=True)
        except Exception:
            pass


def _read_network_options_file() -> List[str]:
    if not NETWORK_OPTIONS_PATH.exists():
        return []
    options: List[str] = []
    with NETWORK_OPTIONS_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "network" not in reader.fieldnames:
            return []
        for row in reader:
            value = (row.get("network") or "").strip()
            if value:
                options.append(value)
    return options


def _write_network_options_file(options: List[str]) -> None:
    NETWORK_OPTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    normalized = sorted(set((option or "").strip() for option in options if (option or "").strip()))
    with NETWORK_OPTIONS_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["network"])
        writer.writeheader()
        for option in normalized:
            writer.writerow({"network": option})


def list_network_options() -> List[str]:
    mapping_path = (BASE_DIR / "data" / "network_mappings.csv").resolve()
    mapping = load_network_mapping(mapping_path)
    file_options = _read_network_options_file()
    settings = read_network_settings()
    options = sorted(
        set(mapping.values()) | set(file_options) | {settings["default_network"], "Cigna_PPO"}
    )
    return options


class NetworkOptionIn(BaseModel):
    name: str


class NetworkMappingIn(BaseModel):
    zip: str
    network: str


class NetworkMappingOut(BaseModel):
    zip: str
    network: str


class NetworkSettingsOut(BaseModel):
    default_network: str
    coverage_threshold: float


class HubSpotSettingsOut(BaseModel):
    enabled: bool
    portal_id: str
    pipeline_id: str
    default_stage_id: str
    sync_quote_to_hubspot: bool
    sync_hubspot_to_quote: bool
    ticket_subject_template: str
    ticket_content_template: str
    property_mappings: Dict[str, str]
    quote_status_to_stage: Dict[str, str]
    stage_to_quote_status: Dict[str, str]
    token_configured: bool
    oauth_connected: bool
    oauth_hub_id: Optional[str]
    oauth_redirect_uri: Optional[str]


class HubSpotSettingsUpdate(BaseModel):
    enabled: bool
    portal_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    default_stage_id: Optional[str] = None
    sync_quote_to_hubspot: bool = True
    sync_hubspot_to_quote: bool = True
    ticket_subject_template: Optional[str] = None
    ticket_content_template: Optional[str] = None
    property_mappings: Optional[Dict[str, str]] = None
    quote_status_to_stage: Optional[Dict[str, str]] = None
    stage_to_quote_status: Optional[Dict[str, str]] = None
    private_app_token: Optional[str] = None
    oauth_redirect_uri: Optional[str] = None


class HubSpotOAuthStartIn(BaseModel):
    redirect_uri: Optional[str] = None


class HubSpotOAuthStartOut(BaseModel):
    authorize_url: str


class HubSpotOAuthStatusOut(BaseModel):
    status: str


class HubSpotTestResponse(BaseModel):
    status: str
    pipelines_found: int


class HubSpotSyncResponse(BaseModel):
    status: str
    quote_id: str
    ticket_id: Optional[str]
    quote_status: str
    ticket_stage: Optional[str]


class HubSpotBulkMismatchBucketOut(BaseModel):
    message: str
    count: int
    quote_ids: List[str]


class HubSpotBulkQuoteMismatchOut(BaseModel):
    quote_id: str
    company: str
    hubspot_ticket_id: Optional[str]
    hubspot_sync_error: str


class HubSpotBulkResyncResponse(BaseModel):
    status: str
    integration_enabled: bool
    quote_to_hubspot_sync_enabled: bool
    total_quotes: int
    attempted_quotes: int
    clean_quotes: int
    mismatch_quotes: int
    buckets: List[HubSpotBulkMismatchBucketOut]
    mismatches: List[HubSpotBulkQuoteMismatchOut]


class HubSpotPipelineStageOut(BaseModel):
    id: str
    label: str


class HubSpotPipelineOut(BaseModel):
    id: str
    label: str
    stages: List[HubSpotPipelineStageOut]


class HubSpotTicketPropertyOut(BaseModel):
    name: str
    label: str


def read_network_settings() -> Dict[str, Any]:
    default = {"default_network": "Cigna_PPO", "coverage_threshold": 0.90}
    if not NETWORK_SETTINGS_PATH.exists():
        return default
    try:
        raw = json.loads(NETWORK_SETTINGS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return default
    default_network = (raw.get("default_network") or default["default_network"]).strip() or default["default_network"]
    threshold = raw.get("coverage_threshold", default["coverage_threshold"])
    try:
        threshold = float(threshold)
    except Exception:
        threshold = default["coverage_threshold"]
    threshold = max(0.0, min(1.0, threshold))
    return {"default_network": default_network, "coverage_threshold": threshold}


def write_network_settings(default_network: str, coverage_threshold: float) -> Dict[str, Any]:
    NETWORK_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "default_network": (default_network or "Cigna_PPO").strip() or "Cigna_PPO",
        "coverage_threshold": max(0.0, min(1.0, float(coverage_threshold))),
    }
    NETWORK_SETTINGS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def normalize_mapping_dict(value: Optional[Dict[str, Any]]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for key, mapped_value in (value or {}).items():
        left = str(key or "").strip()
        right = str(mapped_value or "").strip()
        if left and right:
            mapping[left] = right
    return mapping


def is_blocked_hubspot_ticket_property(name: Optional[str]) -> bool:
    candidate = str(name or "").strip()
    if not candidate:
        return True
    lowered = candidate.lower()
    if lowered in HUBSPOT_TICKET_RESERVED_PROPERTIES:
        return True
    return any(lowered.startswith(prefix) for prefix in HUBSPOT_TICKET_READ_ONLY_PREFIXES)


def normalize_ticket_property_mappings(value: Optional[Dict[str, Any]]) -> Dict[str, str]:
    mapping = normalize_mapping_dict(value)
    cleaned: Dict[str, str] = {}
    for local_key, hubspot_property in mapping.items():
        if is_blocked_hubspot_ticket_property(hubspot_property):
            continue
        cleaned[local_key] = hubspot_property
    return cleaned


def merge_ticket_property_mappings(
    configured: Optional[Dict[str, Any]],
    defaults: Optional[Dict[str, Any]],
) -> Dict[str, str]:
    merged = normalize_ticket_property_mappings(defaults)
    merged.update(normalize_ticket_property_mappings(configured))
    return merged


LEGACY_HUBSPOT_TICKET_PROPERTY_MIGRATIONS: Dict[str, Dict[str, str]] = {
    "broker_fee_pepm": {
        "level_health_broker_fee_pepm": "requested_broker_fee__pepm_",
    },
    "primary_network": {
        "level_health_primary_network": "primary_network",
    },
    "secondary_network": {
        "level_health_secondary_network": "secondary_network",
    },
    "renewal_comparison": {
        "level_health_renewal_comparison": "renewal_comparison",
    },
}

CENSUS_PROPERTY_MAPPING_ALIASES: tuple[str, ...] = (
    "member_level_census",
    "upload_files",
    "census_latest_hubspot_file_id",
    "member_level_census_url",
    "census_latest_file_url",
    "census_latest_filename",
    "census_latest_uploaded_at",
    "census_uploaded",
)


def migrate_legacy_ticket_property_mappings(value: Optional[Dict[str, Any]]) -> Dict[str, str]:
    mapping = normalize_ticket_property_mappings(value)
    migrated: Dict[str, str] = {}
    for local_key, hubspot_property in mapping.items():
        replacement = LEGACY_HUBSPOT_TICKET_PROPERTY_MIGRATIONS.get(local_key, {}).get(hubspot_property)
        migrated[local_key] = replacement or hubspot_property

    canonical_census_property = ""
    for key in CENSUS_PROPERTY_MAPPING_ALIASES:
        candidate = str(migrated.get(key) or "").strip()
        if candidate:
            canonical_census_property = candidate
            break
    if canonical_census_property:
        migrated["member_level_census"] = canonical_census_property
    for key in CENSUS_PROPERTY_MAPPING_ALIASES:
        if key != "member_level_census":
            migrated.pop(key, None)
    return migrated


def default_hubspot_settings() -> Dict[str, Any]:
    return {
        "enabled": False,
        "portal_id": (os.getenv("HUBSPOT_PORTAL_ID", "7106327") or "7106327").strip(),
        "pipeline_id": (os.getenv("HUBSPOT_PIPELINE_ID", "98238573") or "98238573").strip(),
        "default_stage_id": (os.getenv("HUBSPOT_DEFAULT_STAGE_ID", "") or "").strip(),
        "sync_quote_to_hubspot": True,
        "sync_hubspot_to_quote": True,
        "ticket_subject_template": "{{company}}",
        "ticket_content_template": "Company: {{company}}\nQuote ID: {{quote_id}}\nStatus: {{status}}\nEffective Date: {{effective_date}}\nBroker Org: {{broker_org}}",
        "property_mappings": {
            "id": "level_health_quote_id",
            "company": "level_health_company",
            "status": "level_health_quote_status",
            "effective_date": "level_health_effective_date",
            "broker_org": "level_health_broker_org",
            "broker_fee_pepm": "requested_broker_fee__pepm_",
            "primary_network": "primary_network",
            "secondary_network": "secondary_network",
            "renewal_comparison": "renewal_comparison",
        },
        "quote_status_to_stage": {},
        "stage_to_quote_status": {},
        "oauth_redirect_uri": (os.getenv("HUBSPOT_OAUTH_REDIRECT_URI", "") or "").strip(),
        "private_app_token": (os.getenv("HUBSPOT_PRIVATE_APP_TOKEN", "") or "").strip(),
        "oauth_access_token": "",
        "oauth_refresh_token": "",
        "oauth_expires_at": "",
        "oauth_hub_id": "",
    }


def serialize_hubspot_settings_for_storage(settings: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "enabled": bool(settings.get("enabled", False)),
        "portal_id": str(settings.get("portal_id") or "").strip(),
        "pipeline_id": str(settings.get("pipeline_id") or "").strip(),
        "default_stage_id": str(settings.get("default_stage_id") or "").strip(),
        "sync_quote_to_hubspot": bool(settings.get("sync_quote_to_hubspot", True)),
        "sync_hubspot_to_quote": bool(settings.get("sync_hubspot_to_quote", True)),
        "ticket_subject_template": str(settings.get("ticket_subject_template") or "").strip(),
        "ticket_content_template": str(settings.get("ticket_content_template") or "").strip(),
        "property_mappings": merge_ticket_property_mappings(
            migrate_legacy_ticket_property_mappings(settings.get("property_mappings")),
            default_hubspot_settings()["property_mappings"],
        ),
        "quote_status_to_stage": normalize_mapping_dict(settings.get("quote_status_to_stage")),
        "stage_to_quote_status": normalize_mapping_dict(settings.get("stage_to_quote_status")),
        "oauth_redirect_uri": str(settings.get("oauth_redirect_uri") or "").strip(),
        "private_app_token": str(settings.get("private_app_token") or "").strip(),
        "oauth_access_token": str(settings.get("oauth_access_token") or "").strip(),
        "oauth_refresh_token": str(settings.get("oauth_refresh_token") or "").strip(),
        "oauth_expires_at": str(settings.get("oauth_expires_at") or "").strip(),
        "oauth_hub_id": str(settings.get("oauth_hub_id") or "").strip(),
    }


def persist_hubspot_settings(settings: Dict[str, Any]) -> None:
    HUBSPOT_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = serialize_hubspot_settings_for_storage(settings)
    HUBSPOT_SETTINGS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def normalize_hubspot_signature_uri(uri: str) -> str:
    parsed = urlparse.urlsplit(uri or "")
    scheme = (parsed.scheme or "").strip().lower()
    hostname = (parsed.hostname or "").strip()
    if not scheme or not hostname:
        return uri or ""
    port = parsed.port
    use_port = port and not ((scheme == "https" and port == 443) or (scheme == "http" and port == 80))
    netloc = f"{hostname}:{port}" if use_port else hostname
    path = urlparse.unquote(parsed.path or "")
    query = urlparse.unquote(parsed.query or "")
    normalized = f"{scheme}://{netloc}{path}"
    if query:
        normalized = f"{normalized}?{query}"
    return normalized


def build_hubspot_signature_v3(
    secret: str,
    *,
    method: str,
    uri: str,
    body: str,
    timestamp: str,
) -> str:
    secret_value = (secret or "").strip()
    source = f"{(method or '').upper()}{uri or ''}{body or ''}{timestamp or ''}"
    digest = hmac.new(
        secret_value.encode("utf-8"),
        source.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def is_hubspot_request_timestamp_fresh(timestamp: str, *, now_ms: Optional[int] = None) -> bool:
    text = str(timestamp or "").strip()
    if not text:
        return False
    try:
        parsed = int(text)
    except Exception:
        return False
    if HUBSPOT_SIGNATURE_MAX_AGE_SECONDS <= 0:
        return True
    current_ms = now_ms if now_ms is not None else int(datetime.utcnow().timestamp() * 1000)
    return abs(current_ms - parsed) <= HUBSPOT_SIGNATURE_MAX_AGE_SECONDS * 1000


def verify_hubspot_request_signature(request: Request, raw_body: bytes) -> None:
    secret = (
        (os.getenv("HUBSPOT_APP_CLIENT_SECRET", "") or "").strip()
        or (os.getenv("HUBSPOT_CLIENT_SECRET", "") or "").strip()
    )
    if not secret:
        raise HTTPException(status_code=503, detail="HubSpot request signature secret is not configured")

    timestamp = str(request.headers.get("x-hubspot-request-timestamp") or "").strip()
    provided_signature = str(request.headers.get("x-hubspot-signature-v3") or "").strip()
    if not timestamp or not provided_signature:
        raise HTTPException(status_code=401, detail="Missing HubSpot request signature")
    if not is_hubspot_request_timestamp_fresh(timestamp):
        raise HTTPException(status_code=401, detail="HubSpot request signature timestamp is stale")

    uri = normalize_hubspot_signature_uri(str(request.url))
    payload_text = raw_body.decode("utf-8")
    expected_signature = build_hubspot_signature_v3(
        secret,
        method=request.method,
        uri=uri,
        body=payload_text,
        timestamp=timestamp,
    )
    if not secrets.compare_digest(expected_signature, provided_signature):
        raise HTTPException(status_code=401, detail="Invalid HubSpot request signature")


def read_json_dict_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            return loaded
    except Exception:
        return {}
    return {}


def read_hubspot_settings(*, include_token: bool = False) -> Dict[str, Any]:
    defaults = default_hubspot_settings()
    raw: Dict[str, Any] = read_json_dict_file(HUBSPOT_SETTINGS_PATH)
    if (
        not raw
        and HUBSPOT_SETTINGS_PATH != LEGACY_HUBSPOT_SETTINGS_PATH
        and LEGACY_HUBSPOT_SETTINGS_PATH.exists()
    ):
        raw = read_json_dict_file(LEGACY_HUBSPOT_SETTINGS_PATH)
        if raw:
            # One-time migration for environments that used the legacy file path.
            try:
                persist_hubspot_settings(raw)
            except Exception:
                pass
    raw_property_mappings = normalize_ticket_property_mappings(raw.get("property_mappings"))
    migrated_property_mappings = migrate_legacy_ticket_property_mappings(raw.get("property_mappings"))
    if raw and migrated_property_mappings != raw_property_mappings:
        raw = dict(raw)
        raw["property_mappings"] = migrated_property_mappings
        try:
            persist_hubspot_settings(raw)
        except Exception:
            pass

    private_token = str(raw.get("private_app_token") or defaults["private_app_token"]).strip()
    oauth_access_token = str(raw.get("oauth_access_token") or defaults["oauth_access_token"]).strip()
    oauth_refresh_token = str(raw.get("oauth_refresh_token") or defaults["oauth_refresh_token"]).strip()
    oauth_expires_at = str(raw.get("oauth_expires_at") or defaults["oauth_expires_at"]).strip()
    oauth_hub_id = str(raw.get("oauth_hub_id") or defaults["oauth_hub_id"]).strip()
    oauth_redirect_uri = str(raw.get("oauth_redirect_uri") or defaults["oauth_redirect_uri"]).strip()

    payload = {
        "enabled": bool(raw.get("enabled", defaults["enabled"])),
        "portal_id": str(raw.get("portal_id") or defaults["portal_id"]).strip() or defaults["portal_id"],
        "pipeline_id": str(raw.get("pipeline_id") or defaults["pipeline_id"]).strip(),
        "default_stage_id": str(raw.get("default_stage_id") or defaults["default_stage_id"]).strip(),
        "sync_quote_to_hubspot": bool(raw.get("sync_quote_to_hubspot", defaults["sync_quote_to_hubspot"])),
        "sync_hubspot_to_quote": bool(raw.get("sync_hubspot_to_quote", defaults["sync_hubspot_to_quote"])),
        "ticket_subject_template": str(
            raw.get("ticket_subject_template") or defaults["ticket_subject_template"]
        ).strip()
        or defaults["ticket_subject_template"],
        "ticket_content_template": str(
            raw.get("ticket_content_template") or defaults["ticket_content_template"]
        ).strip()
        or defaults["ticket_content_template"],
        "property_mappings": merge_ticket_property_mappings(
            migrated_property_mappings,
            defaults["property_mappings"],
        ),
        "quote_status_to_stage": normalize_mapping_dict(raw.get("quote_status_to_stage")),
        "stage_to_quote_status": normalize_mapping_dict(raw.get("stage_to_quote_status")),
        "token_configured": bool(private_token or oauth_access_token),
        "oauth_connected": bool(oauth_access_token and oauth_refresh_token),
        "oauth_hub_id": oauth_hub_id or None,
        "oauth_redirect_uri": oauth_redirect_uri or None,
    }
    if include_token:
        payload["private_app_token"] = private_token
        payload["oauth_access_token"] = oauth_access_token
        payload["oauth_refresh_token"] = oauth_refresh_token
        payload["oauth_expires_at"] = oauth_expires_at
        payload["oauth_hub_id"] = oauth_hub_id
        payload["oauth_redirect_uri"] = oauth_redirect_uri
    return payload


def write_hubspot_settings(
    payload: HubSpotSettingsUpdate,
    *,
    existing_token: Optional[str],
) -> Dict[str, Any]:
    current = read_hubspot_settings(include_token=True)
    current_token = (existing_token if existing_token is not None else current.get("private_app_token")) or ""
    next_token = current_token
    if payload.private_app_token is not None:
        next_token = payload.private_app_token.strip()

    saved = {
        "enabled": bool(payload.enabled),
        "portal_id": str(payload.portal_id or current["portal_id"]).strip() or current["portal_id"],
        "pipeline_id": str(payload.pipeline_id or "").strip(),
        "default_stage_id": str(payload.default_stage_id or "").strip(),
        "sync_quote_to_hubspot": bool(payload.sync_quote_to_hubspot),
        "sync_hubspot_to_quote": bool(payload.sync_hubspot_to_quote),
        "ticket_subject_template": str(
            payload.ticket_subject_template or current["ticket_subject_template"]
        ).strip()
        or current["ticket_subject_template"],
        "ticket_content_template": str(
            payload.ticket_content_template or current["ticket_content_template"]
        ).strip()
        or current["ticket_content_template"],
        "property_mappings": merge_ticket_property_mappings(
            migrate_legacy_ticket_property_mappings(
                payload.property_mappings
                if payload.property_mappings is not None
                else current["property_mappings"]
            ),
            default_hubspot_settings()["property_mappings"],
        ),
        "quote_status_to_stage": normalize_mapping_dict(
            payload.quote_status_to_stage
            if payload.quote_status_to_stage is not None
            else current["quote_status_to_stage"]
        ),
        "stage_to_quote_status": normalize_mapping_dict(
            payload.stage_to_quote_status
            if payload.stage_to_quote_status is not None
            else current["stage_to_quote_status"]
        ),
        "oauth_redirect_uri": str(
            payload.oauth_redirect_uri
            if payload.oauth_redirect_uri is not None
            else (current.get("oauth_redirect_uri") or "")
        ).strip(),
        "private_app_token": next_token,
        "oauth_access_token": current.get("oauth_access_token") or "",
        "oauth_refresh_token": current.get("oauth_refresh_token") or "",
        "oauth_expires_at": current.get("oauth_expires_at") or "",
        "oauth_hub_id": current.get("oauth_hub_id") or "",
    }
    persist_hubspot_settings(saved)
    return read_hubspot_settings(include_token=False)


def exchange_hubspot_oauth_token(form_payload: Dict[str, str]) -> Dict[str, Any]:
    body = urlparse.urlencode(form_payload).encode("utf-8")
    req = urlrequest.Request(
        HUBSPOT_OAUTH_TOKEN_URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8").strip()
            if not raw:
                raise HTTPException(status_code=502, detail="HubSpot OAuth returned an empty response")
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                raise HTTPException(status_code=502, detail="Invalid HubSpot OAuth response")
            return parsed
    except urlerror.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8")
        except Exception:
            detail = str(exc)
        raise HTTPException(status_code=502, detail=f"HubSpot OAuth error ({exc.code}): {detail}")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"HubSpot OAuth request failed: {exc}")


def resolve_hubspot_api_token(settings: Dict[str, Any]) -> str:
    oauth_access_token = str(settings.get("oauth_access_token") or "").strip()
    oauth_refresh_token = str(settings.get("oauth_refresh_token") or "").strip()
    oauth_expires_at = parse_iso_datetime(settings.get("oauth_expires_at"))

    if oauth_access_token:
        # Refresh the OAuth access token when it is close to expiry.
        if oauth_refresh_token and oauth_expires_at and oauth_expires_at <= datetime.utcnow() + timedelta(minutes=1):
            client_id = os.getenv("HUBSPOT_CLIENT_ID", "").strip()
            client_secret = os.getenv("HUBSPOT_CLIENT_SECRET", "").strip()
            if not client_id or not client_secret:
                raise HTTPException(
                    status_code=400,
                    detail="HubSpot OAuth client credentials are not configured",
                )
            refreshed = exchange_hubspot_oauth_token(
                {
                    "grant_type": "refresh_token",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": oauth_refresh_token,
                }
            )
            next_access_token = str(refreshed.get("access_token") or "").strip()
            next_refresh_token = str(refreshed.get("refresh_token") or oauth_refresh_token).strip()
            expires_in = int(refreshed.get("expires_in") or 0)
            settings["oauth_access_token"] = next_access_token
            settings["oauth_refresh_token"] = next_refresh_token
            settings["oauth_expires_at"] = (
                (datetime.utcnow() + timedelta(seconds=max(expires_in - 60, 60))).isoformat()
                if expires_in
                else ""
            )
            if refreshed.get("hub_id"):
                settings["oauth_hub_id"] = str(refreshed.get("hub_id"))
            persist_hubspot_settings(settings)
            oauth_access_token = next_access_token
        if oauth_access_token:
            return oauth_access_token

    private_token = str(settings.get("private_app_token") or "").strip()
    if private_token:
        return private_token
    raise HTTPException(status_code=400, detail="HubSpot token is not configured")


def hubspot_api_request(
    token: str,
    method: str,
    path: str,
    *,
    body: Optional[Dict[str, Any]] = None,
    query: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    normalized_token = (token or "").strip()
    if not normalized_token:
        raise HTTPException(status_code=400, detail="HubSpot private app token is not configured")

    url = f"{HUBSPOT_API_BASE}{path}"
    if query:
        qs = urlparse.urlencode(query, doseq=True)
        url = f"{url}?{qs}"

    data = None
    headers = {"Authorization": f"Bearer {normalized_token}"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urlrequest.Request(
        url,
        data=data,
        headers=headers,
        method=method.upper(),
    )
    try:
        with urlrequest.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8").strip()
            if not raw:
                return {}
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
            return {"results": parsed}
    except urlerror.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8")
        except Exception:
            detail = str(exc)
        parsed_message = detail
        payload_obj: Dict[str, Any] = {}
        try:
            parsed = json.loads(detail)
            if isinstance(parsed, dict):
                payload_obj = parsed
            parsed_message = str(payload_obj.get("message") or payload_obj.get("detail") or detail)
        except Exception:
            parsed_message = detail or str(exc)
        missing_required = extract_hubspot_missing_required_properties(
            payload_obj,
            error_message=parsed_message,
        )
        if missing_required:
            suffix = f"Missing required properties: {', '.join(missing_required)}"
            if suffix not in parsed_message:
                parsed_message = f"{parsed_message} {suffix}".strip()
        raise HTTPException(
            status_code=502,
            detail=f"HubSpot API error ({exc.code}): {parsed_message}",
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"HubSpot API request failed: {exc}")


def build_hubspot_ticket_url(portal_id: str, ticket_id: str) -> Optional[str]:
    portal = (portal_id or "").strip()
    ticket = (ticket_id or "").strip()
    if not portal or not ticket:
        return None
    return f"https://app.hubspot.com/contacts/{portal}/record/0-5/{ticket}"


def first_non_empty_string(*values: Any) -> Optional[str]:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def parse_assignment_coverage_percentage(result_json: Any) -> Optional[float]:
    if not isinstance(result_json, dict):
        return None
    for key in (
        "coverage_percentage",
        "coverage",
        "direct_contract_coverage",
        "direct_contract_percentage",
    ):
        raw_value = result_json.get(key)
        if raw_value is None:
            continue
        try:
            parsed = float(raw_value)
        except Exception:
            continue
        if parsed < 0:
            continue
        if parsed > 1 and parsed <= 100:
            parsed = parsed / 100.0
        if parsed > 1:
            continue
        return round(parsed, 4)
    return None


def parse_assignment_flag(result_json: Any, key: str) -> Optional[bool]:
    if not isinstance(result_json, dict):
        return None
    raw = result_json.get(key)
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def lookup_quote_for_hubspot_card(
    conn: sqlite3.Connection,
    *,
    quote_id: Optional[str],
    hubspot_ticket_id: Optional[str],
) -> tuple[Optional[sqlite3.Row], Optional[str]]:
    cur = conn.cursor()
    normalized_quote_id = str(quote_id or "").strip()
    if normalized_quote_id:
        cur.execute("SELECT * FROM Quote WHERE id = ? LIMIT 1", (normalized_quote_id,))
        row = cur.fetchone()
        if row:
            return row, "quote_id"
    normalized_ticket_id = str(hubspot_ticket_id or "").strip()
    if normalized_ticket_id:
        cur.execute(
            """
            SELECT * FROM Quote
            WHERE hubspot_ticket_id = ?
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (normalized_ticket_id,),
        )
        row = cur.fetchone()
        if row:
            return row, "hubspot_ticket_id"
    return None, None


def build_hubspot_card_data_for_quote(
    conn: sqlite3.Connection,
    quote_row: sqlite3.Row,
    *,
    resolved_by: str,
    request_quote_id: Optional[str],
    request_ticket_id: Optional[str],
) -> Dict[str, Any]:
    quote = dict(quote_row)
    quote_id = str(quote.get("id") or "").strip()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT recommendation, confidence, result_json, created_at
        FROM AssignmentRun
        WHERE quote_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (quote_id,),
    )
    assignment_row = cur.fetchone()
    assignment_payload: Dict[str, Any] = {}
    if assignment_row:
        try:
            parsed = json.loads(assignment_row["result_json"] or "{}")
            if isinstance(parsed, dict):
                assignment_payload = parsed
        except Exception:
            assignment_payload = {}

    network_settings = read_network_settings()
    coverage_percentage = parse_assignment_coverage_percentage(assignment_payload)
    fallback_used = parse_assignment_flag(assignment_payload, "fallback_used")
    if fallback_used is None and coverage_percentage is not None:
        fallback_used = coverage_percentage < float(network_settings.get("coverage_threshold") or 0.9)
    review_required = parse_assignment_flag(assignment_payload, "review_required")

    quote_url = f"{FRONTEND_BASE_URL}/quotes/{quote_id}" if quote_id else None
    return {
        "status": "ok",
        "resolved_by": resolved_by,
        "request": {
            "quote_id": request_quote_id,
            "hubspot_ticket_id": request_ticket_id,
        },
        "quote": {
            "id": quote_id,
            "company": quote.get("company") or "",
            "status": quote.get("status") or "",
            "effective_date": quote.get("effective_date") or "",
            "primary_network": quote.get("primary_network"),
            "secondary_network": quote.get("secondary_network"),
            "tpa": quote.get("tpa"),
            "stoploss": quote.get("stoploss"),
            "current_carrier": quote.get("current_carrier"),
            "renewal_comparison": quote.get("renewal_comparison"),
            "needs_action": bool(quote.get("needs_action")),
            "hubspot_ticket_id": quote.get("hubspot_ticket_id"),
            "hubspot_ticket_url": quote.get("hubspot_ticket_url"),
            "hubspot_last_synced_at": quote.get("hubspot_last_synced_at"),
            "hubspot_sync_error": quote.get("hubspot_sync_error"),
            "quote_url": quote_url,
        },
        "assignment": {
            "recommendation": assignment_row["recommendation"] if assignment_row else None,
            "confidence": assignment_row["confidence"] if assignment_row else None,
            "coverage_percentage": coverage_percentage,
            "fallback_used": fallback_used,
            "review_required": review_required,
            "coverage_threshold": network_settings.get("coverage_threshold"),
            "default_network": network_settings.get("default_network"),
        },
    }


def render_hubspot_template(template: str, quote: Dict[str, Any]) -> str:
    rendered = template
    for key, value in quote.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", str(value or ""))
    return rendered


def to_hubspot_property_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def build_quote_upload_hubspot_fields(
    conn: sqlite3.Connection,
    quote_id: str,
    *,
    ticket_id: Optional[str] = None,
) -> Dict[str, Any]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, type, filename, path, created_at
        FROM Upload
        WHERE quote_id = ?
        ORDER BY created_at DESC
        """,
        (quote_id,),
    )
    rows = cur.fetchall()
    rows_by_type: Dict[str, List[sqlite3.Row]] = {}
    for row in rows:
        upload_type = str(row["type"] or "").strip()
        if not upload_type:
            continue
        rows_by_type.setdefault(upload_type, []).append(row)

    ticket_key = str(ticket_id or "").strip()
    synced_file_id_by_upload_id: Dict[str, str] = {}
    if ticket_key:
        upload_ids = [str(row["id"] or "").strip() for row in rows if str(row["id"] or "").strip()]
        if upload_ids:
            placeholders = ",".join(["?"] * len(upload_ids))
            cur.execute(
                f"""
                SELECT upload_id, hubspot_file_id
                FROM HubSpotTicketAttachmentSync
                WHERE ticket_id = ? AND upload_id IN ({placeholders})
                """,
                (ticket_key, *upload_ids),
            )
            for row in cur.fetchall():
                upload_id_value = str(row["upload_id"] or "").strip()
                file_id_value = str(row["hubspot_file_id"] or "").strip()
                if upload_id_value and upload_id_value not in synced_file_id_by_upload_id:
                    synced_file_id_by_upload_id[upload_id_value] = file_id_value

    fields: Dict[str, Any] = {}
    for upload_type, prefix in HUBSPOT_UPLOAD_FIELD_TYPES:
        matches = rows_by_type.get(upload_type, [])
        fields[f"{prefix}_uploaded"] = bool(matches)

    census_rows = rows_by_type.get("census", [])
    latest_census = census_rows[0] if census_rows else None
    fields["census_latest_filename"] = str(latest_census["filename"] or "").strip() if latest_census else ""
    fields["census_latest_uploaded_at"] = str(latest_census["created_at"] or "").strip() if latest_census else ""
    fields["upload_count"] = len(rows)

    def build_upload_link(row: Optional[sqlite3.Row]) -> str:
        if not row:
            return ""
        path = str(row["path"] or "").strip()
        if not path:
            return ""
        basename = Path(path).name.strip()
        if not basename:
            return ""
        encoded_name = urlparse.quote(basename)
        return f"{FRONTEND_BASE_URL}/uploads/{quote_id}/{encoded_name}"

    fields["census_latest_file_url"] = build_upload_link(latest_census)

    for upload_type, local_field in HUBSPOT_UPLOAD_FILE_ID_FIELDS:
        latest_upload = rows_by_type.get(upload_type, [None])[0]
        latest_upload_id = str((latest_upload["id"] if latest_upload else "") or "").strip()
        fields[local_field] = synced_file_id_by_upload_id.get(latest_upload_id, "") if latest_upload_id else ""

    # Backward-compatible alias kept for legacy mappings.
    fields["census_latest_hubspot_file_id"] = fields.get("member_level_census", "")
    # Optional text-link alias for customers mapping to a non-file/string property.
    fields["member_level_census_url"] = fields["census_latest_file_url"]

    upload_lines: List[str] = []
    for row in rows:
        filename = str(row["filename"] or "").strip()
        if not filename:
            continue
        link = build_upload_link(row)
        if not link:
            continue
        upload_lines.append(f"{filename}: {link}")
    fields["upload_files"] = "\n".join(upload_lines)
    return fields


def build_quote_hubspot_context(conn: sqlite3.Connection, quote: Dict[str, Any]) -> Dict[str, Any]:
    context = dict(quote)
    quote_id = str(context.get("id") or "").strip()
    if not quote_id:
        return context
    ticket_id = str(context.get("hubspot_ticket_id") or "").strip()
    context.update(build_quote_upload_hubspot_fields(conn, quote_id, ticket_id=ticket_id or None))
    return context


def build_hubspot_ticket_properties(quote: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, str]:
    status_key = str(quote.get("status") or "").strip()
    mapped_stage = settings["quote_status_to_stage"].get(status_key)
    stage_id = (mapped_stage or settings.get("default_stage_id") or "").strip()
    employer_name = str(quote.get("company") or "").strip()

    rendered_content = render_hubspot_template(settings["ticket_content_template"], quote)
    upload_files = str(quote.get("upload_files") or "").strip()
    if upload_files and "{{upload_files}}" not in settings["ticket_content_template"]:
        rendered_content = (
            f"{rendered_content}\n\nUploads:\n{upload_files}".strip()
            if rendered_content
            else f"Uploads:\n{upload_files}"
        )

    properties: Dict[str, str] = {
        "subject": employer_name
        or render_hubspot_template(settings["ticket_subject_template"], quote),
        "content": rendered_content,
    }
    if settings.get("pipeline_id"):
        properties["hs_pipeline"] = settings["pipeline_id"]
    if stage_id:
        properties["hs_pipeline_stage"] = stage_id

    property_mappings = merge_ticket_property_mappings(
        settings.get("property_mappings"),
        default_hubspot_settings()["property_mappings"],
    )
    for local_key, hubspot_property in property_mappings.items():
        if not hubspot_property:
            continue
        if is_blocked_hubspot_ticket_property(hubspot_property):
            continue
        if hubspot_property in properties:
            # Keep reserved/computed values sourced from integration settings.
            continue
        if local_key in quote:
            properties[hubspot_property] = to_hubspot_property_value(quote.get(local_key))
    return {key: value for key, value in properties.items() if value is not None}


def extract_hubspot_invalid_properties(error_message: str) -> List[Dict[str, Any]]:
    message = (error_message or "").strip()
    marker = "Property values were not valid:"
    marker_idx = message.find(marker)
    payload = message[marker_idx + len(marker) :].strip() if marker_idx >= 0 else message
    rows: List[Dict[str, Any]] = []
    start_idx = payload.find("[")
    end_idx = payload.rfind("]")
    if start_idx >= 0 and end_idx > start_idx:
        json_payload = payload[start_idx : end_idx + 1]
        try:
            parsed = json.loads(json_payload)
            if isinstance(parsed, list):
                rows = [row for row in parsed if isinstance(row, dict)]
        except Exception:
            rows = []
    if rows:
        return rows
    # Fallback for non-JSON payloads: pull property names from known fragments.
    names = [name.strip() for name in re.findall(r'"name"\s*:\s*"([^"]+)"', payload) if name.strip()]
    if not names:
        names = [name.strip() for name in re.findall(r"'name'\s*:\s*'([^']+)'", payload) if name.strip()]
    deduped: List[Dict[str, Any]] = []
    for name in names:
        if any(row.get("name") == name for row in deduped):
            continue
        deduped.append({"name": name})
    return deduped


def normalize_hubspot_option_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def parse_hubspot_allowed_options(error_message: str) -> List[str]:
    text = str(error_message or "")
    patterns = [
        r"allowed options:\s*\[(.*?)\]",
        r"one of the allowed options:\s*\[(.*?)\]",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        raw = match.group(1)
        options: List[str] = []
        for item in raw.split(","):
            option = item.strip().strip('"').strip("'")
            if option not in options:
                options.append(option)
        return options
    return []


def _collect_string_values(value: Any) -> List[str]:
    results: List[str] = []
    if isinstance(value, str):
        candidate = value.strip().strip('"').strip("'")
        if candidate:
            results.append(candidate)
        return results
    if isinstance(value, list):
        for item in value:
            results.extend(_collect_string_values(item))
        return results
    if isinstance(value, tuple):
        for item in value:
            results.extend(_collect_string_values(item))
        return results
    return results


def _append_unique_strings(target: List[str], values: List[str]) -> None:
    for value in values:
        if value not in target:
            target.append(value)


def extract_hubspot_missing_required_properties(
    payload: Optional[Dict[str, Any]],
    *,
    error_message: str,
) -> List[str]:
    message = str(error_message or "")
    names: List[str] = []

    bracket_patterns = [
        r"required properties were not set[:\s]*\[(.*?)\]",
        r"missing required properties[:\s]*\[(.*?)\]",
    ]
    for pattern in bracket_patterns:
        match = re.search(pattern, message, re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        raw = match.group(1)
        extracted = []
        for part in raw.split(","):
            value = part.strip().strip('"').strip("'")
            if value:
                extracted.append(value)
        _append_unique_strings(names, extracted)

    parsed_payload = payload if isinstance(payload, dict) else {}
    context = parsed_payload.get("context")
    if isinstance(context, dict):
        for key, value in context.items():
            key_text = str(key or "").strip().lower()
            if key_text in {"properties", "property"} and "required properties were not set" in message.lower():
                _append_unique_strings(names, _collect_string_values(value))
                continue
            if "required" in key_text or "missing" in key_text:
                _append_unique_strings(names, _collect_string_values(value))

    errors = parsed_payload.get("errors")
    if isinstance(errors, list):
        for row in errors:
            if not isinstance(row, dict):
                continue
            row_context = row.get("context")
            if not isinstance(row_context, dict):
                continue
            for key, value in row_context.items():
                key_text = str(key or "").strip().lower()
                if key_text in {"properties", "property"} and "required properties were not set" in message.lower():
                    _append_unique_strings(names, _collect_string_values(value))
                    continue
                if "required" in key_text or "missing" in key_text:
                    _append_unique_strings(names, _collect_string_values(value))

    return names


def suggest_hubspot_option_replacement(
    *,
    attempted_value: str,
    error_message: str,
) -> Optional[str]:
    value = str(attempted_value or "").strip()
    if not value:
        return None
    options = parse_hubspot_allowed_options(error_message)
    if not options:
        return None

    by_normalized = {normalize_hubspot_option_text(option): option for option in options}
    normalized_value = normalize_hubspot_option_text(value)
    exact = by_normalized.get(normalized_value)
    if exact is not None:
        return exact

    state_name = US_STATE_ABBREVIATIONS.get(value.upper())
    if state_name:
        normalized_state_name = normalize_hubspot_option_text(state_name)
        if normalized_state_name in by_normalized:
            return by_normalized[normalized_state_name]

    ordered_options = sorted(options, key=len, reverse=True)
    for option in ordered_options:
        normalized_option = normalize_hubspot_option_text(option)
        if not normalized_option:
            continue
        if normalized_value.startswith(normalized_option):
            return option

    value_tokens = set(re.findall(r"[a-z0-9]+", normalized_value))
    if value_tokens:
        for option in ordered_options:
            option_tokens = set(re.findall(r"[a-z0-9]+", normalize_hubspot_option_text(option)))
            if option_tokens and option_tokens.issubset(value_tokens):
                return option
    return None


def recover_invalid_ticket_properties(
    properties: Dict[str, str], invalid_rows: List[Dict[str, Any]]
) -> tuple[Dict[str, str], List[str], List[str]]:
    next_properties = dict(properties)
    removed_names: List[str] = []
    adjusted_pairs: List[str] = []
    for row in invalid_rows:
        name = str(row.get("name") or "").strip()
        if not name or name not in next_properties:
            continue
        error_code = str(row.get("error") or "").strip().upper()
        message = str(row.get("message") or row.get("localizedErrorMessage") or "")
        attempted = str(row.get("propertyValue") or next_properties.get(name) or "")

        if error_code == "INVALID_OPTION":
            replacement = suggest_hubspot_option_replacement(
                attempted_value=attempted,
                error_message=message,
            )
            if replacement and replacement != str(next_properties.get(name) or ""):
                next_properties[name] = replacement
                pair = f"{name}={replacement}"
                if pair not in adjusted_pairs:
                    adjusted_pairs.append(pair)
                continue

        next_properties.pop(name, None)
        if name not in removed_names:
            removed_names.append(name)
    return next_properties, removed_names, adjusted_pairs


def sanitize_hubspot_ticket_properties(properties: Dict[str, str]) -> tuple[Dict[str, str], List[str]]:
    cleaned: Dict[str, str] = {}
    removed: List[str] = []
    for raw_name, raw_value in properties.items():
        name = str(raw_name or "").strip()
        if not name:
            continue
        lowered = name.lower()
        if lowered == "hs_ticket_id" or any(lowered.startswith(prefix) for prefix in HUBSPOT_TICKET_READ_ONLY_PREFIXES):
            if name not in removed:
                removed.append(name)
            continue
        value = str(raw_value or "").strip()
        cleaned[name] = value
    return cleaned, removed


def upsert_hubspot_ticket_with_recovery(
    token: str,
    *,
    ticket_id: Optional[str],
    properties: Dict[str, str],
) -> tuple[Dict[str, Any], Optional[str]]:
    path = "/crm/v3/objects/tickets" if not ticket_id else f"/crm/v3/objects/tickets/{ticket_id}"
    method = "POST" if not ticket_id else "PATCH"
    attempt_properties, pre_removed = sanitize_hubspot_ticket_properties(properties)
    removed_all: List[str] = list(pre_removed)
    adjusted_all: List[str] = []
    for _ in range(3):
        try:
            result = hubspot_api_request(
                token,
                method,
                path,
                body={"properties": attempt_properties},
            )
            warning_parts: List[str] = []
            if adjusted_all:
                warning_parts.append(f"Adjusted option values: {', '.join(adjusted_all)}")
            if removed_all:
                warning_parts.append(f"Dropped invalid ticket properties: {', '.join(removed_all)}")
            if warning_parts:
                warning = " | ".join(warning_parts)
                return result, warning
            return result, None
        except Exception as exc:
            message = hubspot_exception_message(exc)
            invalid_rows = extract_hubspot_invalid_properties(message)
            if not invalid_rows:
                raise
            next_properties, removed_names, adjusted_pairs = recover_invalid_ticket_properties(
                attempt_properties, invalid_rows
            )
            if not removed_names and not adjusted_pairs:
                raise
            for name in removed_names:
                if name not in removed_all:
                    removed_all.append(name)
            for pair in adjusted_pairs:
                if pair not in adjusted_all:
                    adjusted_all.append(pair)
            attempt_properties = next_properties

    result = hubspot_api_request(
        token,
        method,
        path,
        body={"properties": attempt_properties},
    )
    warning_parts: List[str] = []
    if adjusted_all:
        warning_parts.append(f"Adjusted option values: {', '.join(adjusted_all)}")
    if removed_all:
        warning_parts.append(f"Dropped invalid ticket properties: {', '.join(removed_all)}")
    if warning_parts:
        warning = " | ".join(warning_parts)
        return result, warning
    return result, None


def combine_warnings(*warnings: Optional[str]) -> Optional[str]:
    merged: List[str] = []
    for warning in warnings:
        text = (warning or "").strip()
        if text and text not in merged:
            merged.append(text)
    return " | ".join(merged) if merged else None


def find_user_by_id(conn: sqlite3.Connection, user_id: Optional[str]) -> Optional[sqlite3.Row]:
    candidate = (user_id or "").strip()
    if not candidate:
        return None
    cur = conn.cursor()
    cur.execute("SELECT * FROM User WHERE id = ?", (candidate,))
    return cur.fetchone()


def find_organization_domain(conn: sqlite3.Connection, organization_name: Optional[str]) -> Optional[str]:
    name = (organization_name or "").strip().lower()
    if not name:
        return None
    cur = conn.cursor()
    cur.execute(
        """
        SELECT domain
        FROM Organization
        WHERE lower(trim(name)) = ? OR lower(trim(domain)) = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (name, name),
    )
    row = cur.fetchone()
    domain = (row["domain"] or "").strip().lower() if row else ""
    return domain or None


def hubspot_search_object_id(
    token: str,
    object_type: str,
    property_name: str,
    value: str,
    *,
    properties: Optional[List[str]] = None,
) -> Optional[str]:
    candidate = (value or "").strip()
    if not candidate:
        return None
    payload: Dict[str, Any] = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": property_name,
                        "operator": "EQ",
                        "value": candidate,
                    }
                ]
            }
        ],
        "limit": 1,
    }
    if properties:
        payload["properties"] = properties
    result = hubspot_api_request(
        token,
        "POST",
        f"/crm/v3/objects/{object_type}/search",
        body=payload,
    )
    rows = result.get("results") or []
    if not rows:
        return None
    first = rows[0] or {}
    object_id = str(first.get("id") or "").strip()
    return object_id or None


def hubspot_exception_message(exc: Exception) -> str:
    if isinstance(exc, HTTPException):
        return str(exc.detail)
    return str(exc)


def get_hubspot_sync_lock(quote_id: str) -> threading.Lock:
    key = str(quote_id or "").strip()
    with HUBSPOT_SYNC_LOCK_GUARD:
        lock = HUBSPOT_SYNC_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            HUBSPOT_SYNC_LOCKS[key] = lock
    return lock


def find_existing_hubspot_ticket_for_quote(
    token: str,
    quote: Dict[str, Any],
    settings: Dict[str, Any],
) -> Optional[str]:
    quote_id = str(quote.get("id") or "").strip()
    if not quote_id:
        return None
    property_mappings = merge_ticket_property_mappings(
        settings.get("property_mappings"),
        default_hubspot_settings()["property_mappings"],
    )
    quote_id_property = str(property_mappings.get("id") or "").strip()
    if not quote_id_property:
        return None
    return hubspot_search_object_id(
        token,
        "tickets",
        quote_id_property,
        quote_id,
        properties=[quote_id_property, "subject", "hs_pipeline_stage"],
    )


def build_multipart_form_data(
    *,
    fields: Dict[str, str],
    file_field_name: str,
    file_name: str,
    file_bytes: bytes,
    file_content_type: str,
) -> tuple[bytes, str]:
    boundary = f"----levelhealth-{uuid.uuid4().hex}"
    chunks: List[bytes] = []
    for key, value in fields.items():
        chunks.extend(
            [
                f"--{boundary}\r\n".encode("utf-8"),
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"),
                str(value or "").encode("utf-8"),
                b"\r\n",
            ]
        )
    safe_name = (file_name or "upload.bin").replace('"', "")
    chunks.extend(
        [
            f"--{boundary}\r\n".encode("utf-8"),
            (
                f'Content-Disposition: form-data; name="{file_field_name}"; filename="{safe_name}"\r\n'
            ).encode("utf-8"),
            f"Content-Type: {file_content_type or 'application/octet-stream'}\r\n\r\n".encode("utf-8"),
            file_bytes,
            b"\r\n",
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )
    return b"".join(chunks), boundary


def upload_file_to_hubspot(
    token: str,
    *,
    quote_id: str,
    source_path: Path,
    source_filename: str,
) -> str:
    file_path = Path(source_path)
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=400, detail=f"Attachment file not found: {source_filename}")

    folder_root = HUBSPOT_FILES_FOLDER_ROOT or "/level-health/quote-attachments"
    folder_root = "/" + folder_root.strip("/")
    folder_path = f"{folder_root}/{quote_id}".replace("//", "/")
    options = json.dumps({"access": HUBSPOT_FILES_ACCESS})

    with file_path.open("rb") as fh:
        file_bytes = fh.read()
    content_type = mimetypes.guess_type(source_filename)[0] or "application/octet-stream"
    body, boundary = build_multipart_form_data(
        fields={
            "fileName": source_filename,
            "folderPath": folder_path,
            "options": options,
        },
        file_field_name="file",
        file_name=source_filename,
        file_bytes=file_bytes,
        file_content_type=content_type,
    )

    req = urlrequest.Request(
        f"{HUBSPOT_API_BASE}/files/v3/files",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8").strip()
            parsed = json.loads(raw) if raw else {}
            if not isinstance(parsed, dict):
                raise HTTPException(status_code=502, detail="Invalid HubSpot file upload response")
            file_id = str(parsed.get("id") or "").strip()
            if not file_id:
                raise HTTPException(status_code=502, detail="HubSpot file upload returned no file id")
            return file_id
    except urlerror.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8")
        except Exception:
            detail = str(exc)
        raise HTTPException(status_code=502, detail=f"HubSpot file upload error ({exc.code}): {detail}")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"HubSpot file upload request failed: {exc}")


def associate_hubspot_note_to_ticket(token: str, *, note_id: str, ticket_id: str) -> None:
    # Prefer v4 default associations since they avoid fragile v3 type IDs.
    try:
        associate_hubspot_records_default(
            token,
            from_object_type="note",
            from_object_id=note_id,
            to_object_type="ticket",
            to_object_id=ticket_id,
        )
        return
    except Exception as exc:
        last_exc: Optional[Exception] = exc

    candidates = [
        f"/crm/v3/objects/notes/{note_id}/associations/ticket/{ticket_id}/note_to_ticket",
        f"/crm/v3/objects/notes/{note_id}/associations/ticket/{ticket_id}/228",
        f"/crm/v3/objects/notes/{note_id}/associations/tickets/{ticket_id}/note_to_ticket",
        f"/crm/v3/objects/notes/{note_id}/associations/tickets/{ticket_id}/228",
    ]
    for path in candidates:
        try:
            hubspot_api_request(token, "PUT", path)
            return
        except Exception as exc:
            last_exc = exc
    if last_exc is not None:
        raise last_exc


def create_hubspot_note_with_attachment(
    token: str,
    *,
    ticket_id: str,
    file_id: str,
    filename: str,
    quote_id: str,
) -> str:
    note_body = f"Level Health attachment synced from quote {quote_id}: {filename}"
    note = hubspot_api_request(
        token,
        "POST",
        "/crm/v3/objects/notes",
        body={
            "properties": {
                "hs_timestamp": str(int(datetime.utcnow().timestamp() * 1000)),
                "hs_note_body": note_body,
                "hs_attachment_ids": str(file_id),
            }
        },
    )
    note_id = str(note.get("id") or "").strip()
    if not note_id:
        raise HTTPException(status_code=502, detail="HubSpot note create returned no id")
    associate_hubspot_note_to_ticket(token, note_id=note_id, ticket_id=ticket_id)
    return note_id


def get_synced_upload_ids_for_ticket(
    conn: sqlite3.Connection,
    *,
    quote_id: str,
    ticket_id: str,
) -> set[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT upload_id
        FROM HubSpotTicketAttachmentSync
        WHERE quote_id = ? AND ticket_id = ?
        """,
        (quote_id, ticket_id),
    )
    return {
        str(row["upload_id"] or "").strip()
        for row in cur.fetchall()
        if str(row["upload_id"] or "").strip()
    }


def record_hubspot_attachment_sync(
    conn: sqlite3.Connection,
    *,
    upload_id: str,
    quote_id: str,
    ticket_id: str,
    hubspot_file_id: str,
    hubspot_note_id: str,
) -> None:
    now = now_iso()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO HubSpotTicketAttachmentSync (
            id, upload_id, quote_id, ticket_id, hubspot_file_id, hubspot_note_id, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(upload_id, ticket_id) DO UPDATE SET
            hubspot_file_id = excluded.hubspot_file_id,
            hubspot_note_id = excluded.hubspot_note_id,
            updated_at = excluded.updated_at
        """,
        (
            str(uuid.uuid4()),
            upload_id,
            quote_id,
            ticket_id,
            hubspot_file_id,
            hubspot_note_id,
            now,
            now,
        ),
    )
    conn.commit()


def sync_hubspot_ticket_file_attachments(
    conn: sqlite3.Connection,
    token: str,
    *,
    quote_id: str,
    ticket_id: str,
) -> Optional[str]:
    quote_key = str(quote_id or "").strip()
    ticket_key = str(ticket_id or "").strip()
    if not quote_key or not ticket_key:
        return None
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, type, filename, path, created_at
        FROM Upload
        WHERE quote_id = ?
        ORDER BY created_at ASC
        """,
        (quote_key,),
    )
    rows = cur.fetchall()
    if not rows:
        return None

    synced_upload_ids = get_synced_upload_ids_for_ticket(conn, quote_id=quote_key, ticket_id=ticket_key)
    warnings: List[str] = []
    for row in rows:
        upload_id = str(row["id"] or "").strip()
        filename = str(row["filename"] or "").strip() or f"{str(row['type'] or '').strip()}.bin"
        source_path_text = str(row["path"] or "").strip()
        if not upload_id or upload_id in synced_upload_ids:
            continue
        if not source_path_text:
            warnings.append(f"Attachment sync skipped ({filename}): missing local path")
            continue
        try:
            hubspot_file_id = upload_file_to_hubspot(
                token,
                quote_id=quote_key,
                source_path=Path(source_path_text),
                source_filename=filename,
            )
            hubspot_note_id = ""
            if HUBSPOT_ATTACHMENTS_CREATE_NOTES:
                hubspot_note_id = create_hubspot_note_with_attachment(
                    token,
                    ticket_id=ticket_key,
                    file_id=hubspot_file_id,
                    filename=filename,
                    quote_id=quote_key,
                )
            record_hubspot_attachment_sync(
                conn,
                upload_id=upload_id,
                quote_id=quote_key,
                ticket_id=ticket_key,
                hubspot_file_id=hubspot_file_id,
                hubspot_note_id=hubspot_note_id,
            )
        except Exception as exc:
            warnings.append(f"Attachment sync failed ({filename}): {hubspot_exception_message(exc)}")
            message = hubspot_exception_message(exc).lower()
            if "(403)" in message or "scope" in message or "forbidden" in message:
                break
    deduped: List[str] = []
    for warning in warnings:
        if warning not in deduped:
            deduped.append(warning)
    return " | ".join(deduped) if deduped else None


def is_hubspot_conflict_error(exc: Exception) -> bool:
    message = hubspot_exception_message(exc)
    return "(409)" in message or "409" in message


def upsert_hubspot_contact_for_quote(
    token: str,
    *,
    broker_email_value: Optional[str],
    broker_first_name: Optional[str],
    broker_last_name: Optional[str],
    broker_phone: Optional[str],
    broker_org_name: Optional[str],
) -> Optional[str]:
    broker_email = (broker_email_value or "").strip().lower()
    if not broker_email:
        return None
    contact_id = hubspot_search_object_id(
        token,
        "contacts",
        "email",
        broker_email,
        properties=["email", "firstname", "lastname", "phone", "company"],
    )
    properties = {
        "email": broker_email,
        "firstname": (broker_first_name or "").strip(),
        "lastname": (broker_last_name or "").strip(),
        "phone": (broker_phone or "").strip(),
        "company": (broker_org_name or "").strip(),
    }
    properties = {key: value for key, value in properties.items() if value}
    if contact_id:
        if properties:
            hubspot_api_request(
                token,
                "PATCH",
                f"/crm/v3/objects/contacts/{contact_id}",
                body={"properties": properties},
            )
        return contact_id
    created = hubspot_api_request(
        token,
        "POST",
        "/crm/v3/objects/contacts",
        body={"properties": properties},
    )
    created_id = str(created.get("id") or "").strip()
    return created_id or None


def upsert_hubspot_company_for_quote(
    conn: sqlite3.Connection,
    token: str,
    *,
    broker_org_name_value: Optional[str],
    broker_email_value: Optional[str],
) -> Optional[str]:
    broker_org_name = (broker_org_name_value or "").strip()
    if not broker_org_name:
        return None
    org_domain = (
        find_organization_domain(conn, broker_org_name)
        or email_domain(broker_email_value)
        or None
    )
    company_id: Optional[str] = None
    if org_domain:
        company_id = hubspot_search_object_id(
            token,
            "companies",
            "domain",
            org_domain,
            properties=["name", "domain"],
        )
    if not company_id:
        company_id = hubspot_search_object_id(
            token,
            "companies",
            "name",
            broker_org_name,
            properties=["name", "domain"],
        )

    properties = {"name": broker_org_name}
    if org_domain:
        properties["domain"] = org_domain
    if company_id:
        hubspot_api_request(
            token,
            "PATCH",
            f"/crm/v3/objects/companies/{company_id}",
            body={"properties": properties},
        )
        return company_id

    created = hubspot_api_request(
        token,
        "POST",
        "/crm/v3/objects/companies",
        body={"properties": properties},
    )
    created_id = str(created.get("id") or "").strip()
    return created_id or None


def associate_hubspot_records_default(
    token: str,
    *,
    from_object_type: str,
    from_object_id: str,
    to_object_type: str,
    to_object_id: str,
) -> None:
    hubspot_api_request(
        token,
        "PUT",
        f"/crm/v4/objects/{from_object_type}/{from_object_id}/associations/default/{to_object_type}/{to_object_id}",
    )


def sync_hubspot_ticket_associations(
    conn: sqlite3.Connection,
    token: str,
    quote: Dict[str, Any],
    ticket_id: str,
) -> Optional[str]:
    warnings: List[str] = []
    broker_user = find_user_by_id(conn, quote.get("assigned_user_id"))
    broker_email_value = (
        (broker_user["email"] if broker_user else quote.get("broker_email"))
        if (broker_user or quote.get("broker_email"))
        else None
    )
    broker_first_name = (
        (broker_user["first_name"] if broker_user else quote.get("broker_first_name"))
        if (broker_user or quote.get("broker_first_name"))
        else None
    )
    broker_last_name = (
        (broker_user["last_name"] if broker_user else quote.get("broker_last_name"))
        if (broker_user or quote.get("broker_last_name"))
        else None
    )
    broker_phone = (
        (broker_user["phone"] if broker_user else quote.get("broker_phone"))
        if (broker_user or quote.get("broker_phone"))
        else None
    )
    broker_org_name = (
        (broker_user["organization"] if broker_user else quote.get("broker_org"))
        if (broker_user or quote.get("broker_org"))
        else None
    )

    contact_id: Optional[str] = None
    try:
        contact_id = upsert_hubspot_contact_for_quote(
            token,
            broker_email_value=broker_email_value,
            broker_first_name=broker_first_name,
            broker_last_name=broker_last_name,
            broker_phone=broker_phone,
            broker_org_name=broker_org_name,
        )
    except Exception as exc:
        warnings.append(f"Contact sync failed: {hubspot_exception_message(exc)}")

    company_id: Optional[str] = None
    try:
        company_id = upsert_hubspot_company_for_quote(
            conn,
            token,
            broker_org_name_value=broker_org_name,
            broker_email_value=broker_email_value,
        )
    except Exception as exc:
        warnings.append(f"Company sync failed: {hubspot_exception_message(exc)}")

    if contact_id:
        try:
            associate_hubspot_records_default(
                token,
                from_object_type="ticket",
                from_object_id=ticket_id,
                to_object_type="contact",
                to_object_id=contact_id,
            )
        except Exception as exc:
            if not is_hubspot_conflict_error(exc):
                warnings.append(f"Ticket-contact association failed: {hubspot_exception_message(exc)}")
    if company_id:
        try:
            associate_hubspot_records_default(
                token,
                from_object_type="ticket",
                from_object_id=ticket_id,
                to_object_type="company",
                to_object_id=company_id,
            )
        except Exception as exc:
            if not is_hubspot_conflict_error(exc):
                warnings.append(f"Ticket-company association failed: {hubspot_exception_message(exc)}")
    if contact_id and company_id:
        try:
            associate_hubspot_records_default(
                token,
                from_object_type="contact",
                from_object_id=contact_id,
                to_object_type="company",
                to_object_id=company_id,
            )
        except Exception as exc:
            if not is_hubspot_conflict_error(exc):
                warnings.append(f"Contact-company association failed: {hubspot_exception_message(exc)}")

    deduped: List[str] = []
    for warning in warnings:
        if warning not in deduped:
            deduped.append(warning)
    return " | ".join(deduped) if deduped else None


def update_quote_hubspot_sync_state(
    conn: sqlite3.Connection,
    quote_id: str,
    *,
    ticket_id: Optional[str] = None,
    ticket_url: Optional[str] = None,
    sync_error: Optional[str] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE Quote
        SET hubspot_ticket_id = COALESCE(?, hubspot_ticket_id),
            hubspot_ticket_url = COALESCE(?, hubspot_ticket_url),
            hubspot_last_synced_at = ?,
            hubspot_sync_error = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            ticket_id,
            ticket_url,
            now_iso(),
            (sync_error or "").strip() or None,
            now_iso(),
            quote_id,
        ),
    )
    conn.commit()


def sync_quote_to_hubspot_async(quote_id: str, *, create_if_missing: bool) -> None:
    def worker() -> None:
        lock = get_hubspot_sync_lock(quote_id)
        try:
            with lock:
                with get_db() as conn:
                    sync_quote_to_hubspot(conn, quote_id, create_if_missing=create_if_missing)
        except Exception:
            # Best-effort background sync; quote save should never fail because HubSpot is slow/unavailable.
            return

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def sync_quote_to_hubspot(conn: sqlite3.Connection, quote_id: str, *, create_if_missing: bool) -> None:
    settings = read_hubspot_settings(include_token=True)
    if not settings["enabled"] or not settings["sync_quote_to_hubspot"]:
        return
    try:
        token = resolve_hubspot_api_token(settings)
    except HTTPException as exc:
        update_quote_hubspot_sync_state(
            conn,
            quote_id,
            sync_error=str(exc.detail),
        )
        return

    quote_row = fetch_quote(conn, quote_id)
    quote = build_quote_hubspot_context(conn, dict(quote_row))
    properties = build_hubspot_ticket_properties(quote, settings)
    ticket_id = (quote.get("hubspot_ticket_id") or "").strip()
    if not ticket_id:
        try:
            ticket_id = str(find_existing_hubspot_ticket_for_quote(token, quote, settings) or "").strip()
        except Exception as exc:
            update_quote_hubspot_sync_state(
                conn,
                quote_id,
                sync_error=f"HubSpot ticket lookup failed: {hubspot_exception_message(exc)}",
            )
            return
    if not ticket_id and not create_if_missing:
        return

    def build_ticket_properties_for_ticket(current_ticket_id: str) -> Dict[str, str]:
        refreshed_quote = dict(fetch_quote(conn, quote_id))
        refreshed_quote["hubspot_ticket_id"] = current_ticket_id
        refreshed_context = build_quote_hubspot_context(conn, refreshed_quote)
        return build_hubspot_ticket_properties(refreshed_context, settings)

    try:
        if ticket_id:
            _, property_warning = upsert_hubspot_ticket_with_recovery(
                token,
                ticket_id=ticket_id,
                properties=properties,
            )
            association_warning = sync_hubspot_ticket_associations(conn, token, quote, ticket_id)
            attachment_warning = sync_hubspot_ticket_file_attachments(
                conn,
                token,
                quote_id=quote_id,
                ticket_id=ticket_id,
            )
            post_attachment_property_warning = None
            try:
                post_attachment_properties = build_ticket_properties_for_ticket(ticket_id)
                _, post_attachment_property_warning = upsert_hubspot_ticket_with_recovery(
                    token,
                    ticket_id=ticket_id,
                    properties=post_attachment_properties,
                )
            except Exception as exc:
                post_attachment_property_warning = (
                    f"Post-attachment property sync failed: {hubspot_exception_message(exc)}"
                )
            ticket_url = build_hubspot_ticket_url(settings["portal_id"], ticket_id)
            update_quote_hubspot_sync_state(
                conn,
                quote_id,
                ticket_id=ticket_id,
                ticket_url=ticket_url,
                sync_error=combine_warnings(
                    property_warning,
                    association_warning,
                    attachment_warning,
                    post_attachment_property_warning,
                ),
            )
            return

        if not settings.get("pipeline_id") or not properties.get("hs_pipeline_stage"):
            update_quote_hubspot_sync_state(
                conn,
                quote_id,
                sync_error="HubSpot pipeline/stage is not configured",
            )
            return

        created, property_warning = upsert_hubspot_ticket_with_recovery(
            token,
            ticket_id=None,
            properties=properties,
        )
        new_ticket_id = str(created.get("id") or "").strip()
        association_warning = None
        attachment_warning = None
        post_attachment_property_warning = None
        if new_ticket_id:
            association_warning = sync_hubspot_ticket_associations(conn, token, quote, new_ticket_id)
            attachment_warning = sync_hubspot_ticket_file_attachments(
                conn,
                token,
                quote_id=quote_id,
                ticket_id=new_ticket_id,
            )
            try:
                post_attachment_properties = build_ticket_properties_for_ticket(new_ticket_id)
                _, post_attachment_property_warning = upsert_hubspot_ticket_with_recovery(
                    token,
                    ticket_id=new_ticket_id,
                    properties=post_attachment_properties,
                )
            except Exception as exc:
                post_attachment_property_warning = (
                    f"Post-attachment property sync failed: {hubspot_exception_message(exc)}"
                )
        ticket_url = build_hubspot_ticket_url(settings["portal_id"], new_ticket_id)
        update_quote_hubspot_sync_state(
            conn,
            quote_id,
            ticket_id=new_ticket_id or None,
            ticket_url=ticket_url,
            sync_error=combine_warnings(
                property_warning,
                association_warning,
                attachment_warning,
                post_attachment_property_warning,
            ),
        )
    except Exception as exc:
        detail = str(exc)
        if isinstance(exc, HTTPException):
            detail = str(exc.detail)
        update_quote_hubspot_sync_state(conn, quote_id, sync_error=detail)


def list_hubspot_ticket_pipelines(settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    token = resolve_hubspot_api_token(settings)
    raw = hubspot_api_request(token, "GET", "/crm/v3/pipelines/tickets")
    pipelines: List[Dict[str, Any]] = []
    for item in raw.get("results", []):
        stage_rows: List[Dict[str, str]] = []
        for stage in item.get("stages", []):
            stage_id = str(stage.get("id") or "").strip()
            stage_label = str(stage.get("label") or stage.get("displayOrder") or stage_id).strip()
            if stage_id:
                stage_rows.append({"id": stage_id, "label": stage_label or stage_id})
        pipelines.append(
            {
                "id": str(item.get("id") or "").strip(),
                "label": str(item.get("label") or item.get("id") or "").strip(),
                "stages": stage_rows,
            }
        )
    return [pipeline for pipeline in pipelines if pipeline["id"]]


def list_hubspot_ticket_properties(settings: Dict[str, Any]) -> List[Dict[str, str]]:
    token = resolve_hubspot_api_token(settings)
    raw = hubspot_api_request(
        token,
        "GET",
        "/crm/v3/properties/tickets",
        query={"archived": "false"},
    )
    properties: List[Dict[str, str]] = []
    for item in raw.get("results", []):
        name = str(item.get("name") or "").strip()
        label = str(item.get("label") or name).strip()
        if not name:
            continue
        if is_blocked_hubspot_ticket_property(name):
            continue
        metadata = item.get("modificationMetadata") or {}
        if bool(metadata.get("readOnlyValue")) or bool(metadata.get("readOnlyDefinition")):
            continue
        if bool(item.get("calculated")):
            continue
        properties.append({"name": name, "label": label or name})
    properties.sort(key=lambda row: row["label"].lower())
    return properties


def sync_quote_from_hubspot(conn: sqlite3.Connection, quote_id: str) -> Dict[str, Any]:
    settings = read_hubspot_settings(include_token=True)
    if not settings["enabled"]:
        raise HTTPException(status_code=400, detail="HubSpot integration is disabled")
    if not settings["sync_hubspot_to_quote"]:
        raise HTTPException(status_code=400, detail="HubSpot -> portal sync is disabled")
    token = resolve_hubspot_api_token(settings)

    quote = dict(fetch_quote(conn, quote_id))
    ticket_id = str(quote.get("hubspot_ticket_id") or "").strip()
    if not ticket_id:
        raise HTTPException(status_code=400, detail="Quote is not linked to a HubSpot ticket")

    property_mappings = merge_ticket_property_mappings(
        settings.get("property_mappings"),
        default_hubspot_settings()["property_mappings"],
    )
    requested_properties = ["subject", "hs_pipeline", "hs_pipeline_stage"]
    for local_key in HUBSPOT_SYNC_DETAIL_FIELDS:
        mapped_property = str(property_mappings.get(local_key) or "").strip()
        if mapped_property:
            requested_properties.append(mapped_property)
    deduped_properties: List[str] = []
    seen: set[str] = set()
    for property_name in requested_properties:
        if property_name in seen:
            continue
        seen.add(property_name)
        deduped_properties.append(property_name)

    ticket = hubspot_api_request(
        token,
        "GET",
        f"/crm/v3/objects/tickets/{ticket_id}",
        query={"properties": deduped_properties},
    )
    properties = ticket.get("properties") or {}
    ticket_stage = str(properties.get("hs_pipeline_stage") or "").strip()
    next_status = settings["stage_to_quote_status"].get(ticket_stage)

    cur = conn.cursor()
    updates: List[str] = []
    params: List[Any] = []
    if next_status and next_status != quote["status"]:
        updates.append("status = ?")
        params.append(next_status)
    for local_key in HUBSPOT_SYNC_DETAIL_FIELDS:
        mapped_property = str(property_mappings.get(local_key) or "").strip()
        if not mapped_property:
            continue
        raw_value = properties.get(mapped_property)
        normalized_value: Optional[str]
        if raw_value is None:
            normalized_value = None
        else:
            text_value = str(raw_value).strip()
            normalized_value = text_value or None
        if normalized_value != quote.get(local_key):
            updates.append(f"{local_key} = ?")
            params.append(normalized_value)

    updates.extend(
        [
            "hubspot_ticket_url = ?",
            "hubspot_last_synced_at = ?",
            "hubspot_sync_error = ?",
            "updated_at = ?",
        ]
    )
    params.extend(
        [
            build_hubspot_ticket_url(settings["portal_id"], ticket_id),
            now_iso(),
            None,
            now_iso(),
        ]
    )
    params.append(quote_id)
    cur.execute(
        f"UPDATE Quote SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    conn.commit()

    refreshed = dict(fetch_quote(conn, quote_id))
    return {
        "quote_id": quote_id,
        "ticket_id": ticket_id,
        "quote_status": refreshed.get("status") or "",
        "ticket_stage": ticket_stage or None,
    }


def build_hubspot_bulk_mismatch_report(
    conn: sqlite3.Connection,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, company, hubspot_ticket_id, hubspot_sync_error
        FROM Quote
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    mismatches: List[Dict[str, Any]] = []
    bucket_map: Dict[str, List[str]] = {}
    for row in rows:
        sync_error = str(row["hubspot_sync_error"] or "").strip()
        if not sync_error:
            continue
        quote_id = str(row["id"] or "").strip()
        if not quote_id:
            continue
        mismatches.append(
            {
                "quote_id": quote_id,
                "company": str(row["company"] or "").strip(),
                "hubspot_ticket_id": (str(row["hubspot_ticket_id"] or "").strip() or None),
                "hubspot_sync_error": sync_error,
            }
        )
        bucket_map.setdefault(sync_error, []).append(quote_id)

    buckets = [
        {"message": message, "count": len(quote_ids), "quote_ids": quote_ids}
        for message, quote_ids in bucket_map.items()
    ]
    buckets.sort(key=lambda row: (-row["count"], row["message"].lower()))
    clean_quotes = max(0, len(rows) - len(mismatches))
    return mismatches, buckets, clean_quotes


def recompute_needs_action(conn: sqlite3.Connection, quote_id: str) -> None:
    cur = conn.cursor()
    census = latest_census_upload(conn, quote_id)
    cur.execute(
        """
        SELECT * FROM StandardizationRun
        WHERE quote_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (quote_id,),
    )
    standardization = cur.fetchone()
    cur.execute(
        """
        SELECT * FROM AssignmentRun
        WHERE quote_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (quote_id,),
    )
    assignment = cur.fetchone()

    issue_count = standardization["issue_count"] if standardization else None
    needs_action = 0
    if not census:
        needs_action = 1
    elif issue_count is not None and issue_count > 0:
        needs_action = 1
    elif not assignment:
        needs_action = 1

    cur.execute(
        """
        UPDATE Quote SET needs_action = ?, updated_at = ? WHERE id = ?
        """,
        (needs_action, now_iso(), quote_id),
    )
    conn.commit()


def save_upload(quote_id: str, upload_type: str, file: UploadFile) -> UploadOut:
    quote_dir = UPLOADS_DIR / quote_id
    quote_dir.mkdir(parents=True, exist_ok=True)
    file_id = str(uuid.uuid4())
    safe_name = file.filename or f"upload-{file_id}"
    target_path = quote_dir / f"{file_id}-{safe_name}"
    with get_db() as conn:
        if upload_type == "census":
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM Upload WHERE quote_id = ? AND type = 'census'",
                (quote_id,),
            )
            existing = cur.fetchall()
            existing_ids = [
                str(row["id"] or "").strip()
                for row in existing
                if str(row["id"] or "").strip()
            ]
            for row in existing:
                try:
                    Path(row["path"]).unlink(missing_ok=True)
                except Exception:
                    pass
            cur.execute(
                "DELETE FROM Upload WHERE quote_id = ? AND type = 'census'",
                (quote_id,),
            )
            if existing_ids:
                placeholders = ",".join(["?"] * len(existing_ids))
                cur.execute(
                    f"DELETE FROM HubSpotTicketAttachmentSync WHERE upload_id IN ({placeholders})",
                    existing_ids,
                )
            cur.execute(
                "UPDATE Quote SET manual_network = NULL, updated_at = ? WHERE id = ?",
                (now_iso(), quote_id),
            )
            conn.commit()
    with target_path.open("wb") as f:
        f.write(file.file.read())
    created_at = now_iso()

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO Upload (id, quote_id, type, filename, path, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (file_id, quote_id, upload_type, safe_name, str(target_path), created_at),
        )
        conn.commit()
        recompute_needs_action(conn, quote_id)

    return UploadOut(
        id=file_id,
        quote_id=quote_id,
        type=upload_type,
        filename=safe_name,
        path=str(target_path),
        created_at=created_at,
    )


# ----------------------
# API routes
# ----------------------

@app.on_event("startup")
async def startup_event() -> None:
    init_db()


@app.get("/")
def root() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/api/auth/request-access", response_model=AccessRequestOut)
def request_access(payload: AccessRequestIn) -> AccessRequestOut:
    first_name = (payload.first_name or "").strip()
    last_name = (payload.last_name or "").strip()
    if not first_name or not last_name:
        raise HTTPException(status_code=400, detail="First and last name are required")
    email = normalize_user_email(payload.email)
    requested_role = normalize_access_request_role(payload.requested_role)
    organization_value = (payload.organization or "").strip()
    email_based_domain = email_domain(email)
    if requested_role == "sponsor":
        requested_domain = normalize_domain_candidate(organization_value) or normalize_domain_candidate(
            email_based_domain
        )
    else:
        requested_domain = normalize_domain_candidate(email_based_domain)

    with get_db() as conn:
        existing_user = fetch_user_by_email(conn, email)
        if existing_user:
            return AccessRequestOut(
                status="existing_user",
                message="Account already exists. Use password login or request a magic link.",
            )

        if requested_role == "broker":
            broker_name = broker_org_name_for_domain(conn, email_based_domain)
            if broker_name and email_based_domain:
                ensure_access_request_user(
                    conn,
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    role="broker",
                    organization=broker_name,
                )
                sync_organizations_from_records(conn)
                conn.commit()
                return AccessRequestOut(
                    status="approved",
                    message="Access approved. Request a magic link to sign in.",
                )

        now = now_iso()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id
            FROM AccessRequest
            WHERE email = ? AND status = 'pending'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (email,),
        )
        pending = cur.fetchone()
        if pending:
            access_request_id = str(pending["id"])
            cur.execute(
                """
                UPDATE AccessRequest
                SET first_name = ?, last_name = ?, requested_role = ?, organization = ?,
                    requested_domain = ?, review_note = NULL, reviewed_at = NULL, reviewed_by_user_id = NULL
                WHERE id = ?
                """,
                (
                    first_name,
                    last_name,
                    requested_role,
                    organization_value or None,
                    requested_domain,
                    access_request_id,
                ),
            )
        else:
            access_request_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO AccessRequest (
                    id, first_name, last_name, email, requested_role, organization, requested_domain,
                    status, review_note, created_at, reviewed_at, reviewed_by_user_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    access_request_id,
                    first_name,
                    last_name,
                    email,
                    requested_role,
                    organization_value or None,
                    requested_domain,
                    "pending",
                    None,
                    now,
                    None,
                    None,
                ),
            )

        cur.execute("SELECT id FROM User WHERE role = 'admin' ORDER BY created_at ASC")
        admin_rows = cur.fetchall()
        requester_display = f"{first_name} {last_name}".strip()
        for admin_row in admin_rows:
            create_notification(
                conn,
                str(admin_row["id"] or "").strip() or None,
                kind="access_request",
                title="Access request pending",
                body=f"{requester_display} requested {requested_role} access for {email}.",
                entity_type="access_request",
                entity_id=access_request_id,
            )

        conn.commit()
    return AccessRequestOut(
        status="pending_review",
        message="Access request submitted for admin review.",
    )


@app.post("/api/auth/request-link")
def request_magic_link(payload: AuthRequestIn, request: Request = None) -> Dict[str, str]:
    email = payload.email.strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Email is required")
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM User WHERE email = ?", (email,))
        user = cur.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="No user found for this email.")

        token = secrets.token_urlsafe(32)
        token_hash = sha256_hex(token)
        now = now_iso()
        expires_at = (datetime.utcnow() + timedelta(minutes=MAGIC_LINK_DURATION_MINUTES)).isoformat()
        cur.execute(
            """
            INSERT INTO AuthMagicLink (id, user_id, email, token_hash, expires_at, used_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), user["id"], email, token_hash, expires_at, None, now),
        )
        conn.commit()

    frontend_base = resolve_frontend_base_url(request)
    link = f"{frontend_base}/auth/verify?token={token}"
    try:
        sent = send_resend_magic_link(email, link)
    except HTTPException:
        if ALLOW_DEV_MAGIC_LINK_FALLBACK:
            return {"status": "dev_link", "link": link}
        raise

    if sent:
        return {"status": "sent"}

    # Local dev fallback when Resend keys are not configured.
    if ALLOW_DEV_MAGIC_LINK_FALLBACK:
        return {"status": "dev_link", "link": link}
    raise HTTPException(status_code=502, detail="Magic link email delivery is not configured.")


@app.post("/api/auth/login", response_model=AuthVerifyOut)
def login_with_password(payload: AuthLoginIn, response: Response) -> AuthVerifyOut:
    email = normalize_user_email(payload.email)
    password = payload.password
    if not password:
        raise HTTPException(status_code=400, detail="Email and password are required")

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM User WHERE email = ?", (email,))
        user = cur.fetchone()
        if not user or not verify_password(
            password,
            user["password_salt"],
            user["password_hash"],
        ):
            raise HTTPException(status_code=401, detail="Invalid email or password.")
        session_token = create_auth_session(conn, user["id"])

    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_token,
        httponly=True,
        samesite=SESSION_COOKIE_SAMESITE,
        secure=SESSION_COOKIE_SECURE,
        max_age=SESSION_DURATION_HOURS * 3600,
        path="/",
    )
    return auth_user_payload(user)


@app.get("/api/auth/verify", response_model=AuthVerifyOut)
def verify_magic_link(token: str, response: Response) -> AuthVerifyOut:
    if not token:
        raise HTTPException(status_code=400, detail="Token is required")
    token_hash = sha256_hex(token)
    now = now_iso()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM AuthMagicLink
            WHERE token_hash = ? AND used_at IS NULL AND expires_at > ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (token_hash, now),
        )
        link = cur.fetchone()
        if not link:
            raise HTTPException(status_code=400, detail="Magic link is invalid or expired.")
        cur.execute("SELECT * FROM User WHERE id = ?", (link["user_id"],))
        user = cur.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User no longer exists.")
        cur.execute("UPDATE AuthMagicLink SET used_at = ? WHERE id = ?", (now_iso(), link["id"]))
        session_token = create_auth_session(conn, user["id"])

    response.set_cookie(
        SESSION_COOKIE_NAME,
        session_token,
        httponly=True,
        samesite=SESSION_COOKIE_SAMESITE,
        secure=SESSION_COOKIE_SECURE,
        max_age=SESSION_DURATION_HOURS * 3600,
        path="/",
    )
    return auth_user_payload(user)


@app.get("/api/auth/me", response_model=AuthVerifyOut)
def get_auth_me(request: Request) -> AuthVerifyOut:
    with get_db() as conn:
        user = require_session_user(conn, request)
    return auth_user_payload(user)


@app.get("/api/auth/profile", response_model=AuthProfileOut)
def get_auth_profile(request: Request) -> AuthProfileOut:
    with get_db() as conn:
        user = require_session_user(conn, request)
    return auth_profile_payload(user)


@app.patch("/api/auth/profile", response_model=AuthProfileOut)
def update_auth_profile(payload: AuthProfileUpdateIn, request: Request) -> AuthProfileOut:
    updates = payload.dict(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No profile updates provided")

    with get_db() as conn:
        session_user = require_session_user(conn, request)
        user_id = session_user_id(session_user)
        if not user_id:
            raise HTTPException(status_code=401, detail="Authentication required")
        cur = conn.cursor()
        cur.execute("SELECT * FROM User WHERE id = ?", (user_id,))
        user = cur.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        data = dict(user)

        if "first_name" in updates:
            first_name = (updates.get("first_name") or "").strip()
            if not first_name:
                raise HTTPException(status_code=400, detail="First name is required")
            data["first_name"] = first_name
        if "last_name" in updates:
            last_name = (updates.get("last_name") or "").strip()
            if not last_name:
                raise HTTPException(status_code=400, detail="Last name is required")
            data["last_name"] = last_name
        if "job_title" in updates:
            job_title = (updates.get("job_title") or "").strip()
            if not job_title:
                raise HTTPException(status_code=400, detail="Job title is required")
            data["job_title"] = job_title
        if "phone" in updates:
            data["phone"] = (updates.get("phone") or "").strip()
        if "password" in updates:
            password_value = require_valid_password(updates.get("password"), required=True)
            password_salt, password_hash = create_password_credentials(password_value)
            data["password_salt"] = password_salt
            data["password_hash"] = password_hash

        data["updated_at"] = now_iso()
        cur.execute(
            """
            UPDATE User
            SET first_name = ?, last_name = ?, phone = ?, job_title = ?,
                password_salt = ?, password_hash = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                data["first_name"],
                data["last_name"],
                data.get("phone") or "",
                data.get("job_title") or "",
                data.get("password_salt"),
                data.get("password_hash"),
                data["updated_at"],
                user_id,
            ),
        )
        conn.commit()
        cur.execute("SELECT * FROM User WHERE id = ?", (user_id,))
        updated = cur.fetchone()
    return auth_profile_payload(updated)


@app.post("/api/auth/logout")
def logout(response: Response, request: Request) -> Dict[str, str]:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        session_hash = sha256_hex(token)
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM AuthSession WHERE session_hash = ?", (session_hash,))
            conn.commit()
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")
    return {"status": "ok"}


@app.get("/api/network-options", response_model=List[str])
def get_network_options() -> List[str]:
    return list_network_options()


@app.post("/api/network-options", response_model=List[str])
def create_network_option(payload: NetworkOptionIn, request: Request) -> List[str]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Network option name is required")
    options = set(_read_network_options_file())
    options.add(name)
    _write_network_options_file(sorted(options))
    return list_network_options()


@app.patch("/api/network-options/{current_name}", response_model=List[str])
def update_network_option(current_name: str, payload: NetworkOptionIn, request: Request) -> List[str]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    next_name = payload.name.strip()
    if not next_name:
        raise HTTPException(status_code=400, detail="Network option name is required")
    options = _read_network_options_file()
    replaced = False
    updated: List[str] = []
    for option in options:
        if option == current_name and not replaced:
            updated.append(next_name)
            replaced = True
        else:
            updated.append(option)
    if not replaced:
        if current_name in {"Cigna_PPO"}:
            raise HTTPException(status_code=400, detail="Default network cannot be renamed")
        raise HTTPException(status_code=404, detail="Network option not found")
    _write_network_options_file(updated)
    mappings = read_network_mappings()
    touched = False
    for row in mappings:
        if row["network"] == current_name:
            row["network"] = next_name
            touched = True
    if touched:
        write_network_mappings(mappings)
    settings = read_network_settings()
    if settings["default_network"] == current_name:
        write_network_settings(next_name, settings["coverage_threshold"])
    return list_network_options()


@app.delete("/api/network-options/{name}", response_model=List[str])
def delete_network_option(name: str, request: Request) -> List[str]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    if name in {"Cigna_PPO"}:
        raise HTTPException(status_code=400, detail="Default network cannot be deleted")
    settings = read_network_settings()
    if settings["default_network"] == name:
        raise HTTPException(status_code=400, detail="Default network cannot be deleted")
    if any(row["network"] == name for row in read_network_mappings()):
        raise HTTPException(status_code=400, detail="Network option is used in ZIP mappings")
    options = _read_network_options_file()
    if name not in options:
        raise HTTPException(status_code=404, detail="Network option not found")
    _write_network_options_file([option for option in options if option != name])
    return list_network_options()


@app.get("/api/network-mappings", response_model=List[NetworkMappingOut])
def get_network_mappings() -> List[NetworkMappingOut]:
    return [NetworkMappingOut(**row) for row in read_network_mappings()]


@app.post("/api/network-mappings", response_model=List[NetworkMappingOut])
def create_network_mapping(payload: NetworkMappingIn, request: Request) -> List[NetworkMappingOut]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    zip_value = normalize_zip(payload.zip)
    network = payload.network.strip()
    if not zip_value:
        raise HTTPException(status_code=400, detail="ZIP must be a valid 5-digit value")
    if not network:
        raise HTTPException(status_code=400, detail="Network is required")
    rows = read_network_mappings()
    rows = [row for row in rows if row["zip"] != zip_value]
    rows.append({"zip": zip_value, "network": network})
    write_network_mappings(rows)
    return [NetworkMappingOut(**row) for row in read_network_mappings()]


@app.patch("/api/network-mappings/{zip_code}", response_model=List[NetworkMappingOut])
def update_network_mapping(zip_code: str, payload: NetworkMappingIn, request: Request) -> List[NetworkMappingOut]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    source_zip = normalize_zip(zip_code)
    target_zip = normalize_zip(payload.zip)
    network = payload.network.strip()
    if not source_zip or not target_zip:
        raise HTTPException(status_code=400, detail="ZIP must be a valid 5-digit value")
    if not network:
        raise HTTPException(status_code=400, detail="Network is required")
    rows = read_network_mappings()
    if source_zip not in {row["zip"] for row in rows}:
        raise HTTPException(status_code=404, detail="Mapping not found")
    rows = [row for row in rows if row["zip"] != source_zip and row["zip"] != target_zip]
    rows.append({"zip": target_zip, "network": network})
    write_network_mappings(rows)
    return [NetworkMappingOut(**row) for row in read_network_mappings()]


@app.delete("/api/network-mappings/{zip_code}", response_model=List[NetworkMappingOut])
def delete_network_mapping(zip_code: str, request: Request) -> List[NetworkMappingOut]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    source_zip = normalize_zip(zip_code)
    if not source_zip:
        raise HTTPException(status_code=400, detail="ZIP must be a valid 5-digit value")
    rows = read_network_mappings()
    if source_zip not in {row["zip"] for row in rows}:
        raise HTTPException(status_code=404, detail="Mapping not found")
    write_network_mappings([row for row in rows if row["zip"] != source_zip])
    return [NetworkMappingOut(**row) for row in read_network_mappings()]


@app.get("/api/network-settings", response_model=NetworkSettingsOut)
def get_network_settings() -> NetworkSettingsOut:
    return NetworkSettingsOut(**read_network_settings())


@app.put("/api/network-settings", response_model=NetworkSettingsOut)
def update_network_settings(payload: NetworkSettingsOut, request: Request) -> NetworkSettingsOut:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    default_network = payload.default_network.strip()
    if not default_network:
        raise HTTPException(status_code=400, detail="Default network is required")
    saved = write_network_settings(default_network, payload.coverage_threshold)
    return NetworkSettingsOut(**saved)


@app.get("/api/integrations/hubspot/settings", response_model=HubSpotSettingsOut)
def get_hubspot_settings(request: Request) -> HubSpotSettingsOut:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    return HubSpotSettingsOut(**read_hubspot_settings(include_token=False))


@app.put("/api/integrations/hubspot/settings", response_model=HubSpotSettingsOut)
def update_hubspot_settings(payload: HubSpotSettingsUpdate, request: Request) -> HubSpotSettingsOut:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    current = read_hubspot_settings(include_token=True)
    saved = write_hubspot_settings(
        payload,
        existing_token=current.get("private_app_token"),
    )
    return HubSpotSettingsOut(**saved)


def hubspot_oauth_popup_response(status: str, message: str) -> HTMLResponse:
    payload_json = json.dumps(
        {
            "type": "hubspot-oauth",
            "status": status,
            "message": message,
        }
    )
    html = f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8" />
    <title>HubSpot Connection</title>
  </head>
  <body>
    <script>
      (function () {{
        var payload = {payload_json};
        try {{
          if (window.opener) {{
            window.opener.postMessage(payload, window.location.origin);
          }}
        }} catch (e) {{}}
        if (payload.status === "success") {{
          document.body.innerText = "HubSpot connected. You can close this window.";
        }} else {{
          document.body.innerText = "HubSpot connection failed: " + payload.message;
        }}
        setTimeout(function () {{
          try {{ window.close(); }} catch (e) {{}}
        }}, 300);
      }})();
    </script>
  </body>
</html>"""
    return HTMLResponse(content=html)


@app.post("/api/integrations/hubspot/oauth/start", response_model=HubSpotOAuthStartOut)
def start_hubspot_oauth(payload: HubSpotOAuthStartIn, request: Request) -> HubSpotOAuthStartOut:
    client_id = os.getenv("HUBSPOT_CLIENT_ID", "").strip()
    client_secret = os.getenv("HUBSPOT_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise HTTPException(
            status_code=400,
            detail="HubSpot OAuth client credentials are not configured",
        )
    settings = read_hubspot_settings(include_token=True)
    redirect_uri = (
        (payload.redirect_uri or "").strip()
        or (settings.get("oauth_redirect_uri") or "").strip()
        or f"{FRONTEND_BASE_URL}/api/integrations/hubspot/oauth/callback"
    )
    if not redirect_uri.startswith("https://") and not redirect_uri.startswith("http://"):
        raise HTTPException(status_code=400, detail="Invalid redirect_uri")

    scopes_raw = (os.getenv("HUBSPOT_OAUTH_SCOPES", HUBSPOT_OAUTH_DEFAULT_SCOPES) or "").strip()
    if not scopes_raw:
        scopes_raw = HUBSPOT_OAUTH_DEFAULT_SCOPES
    scopes = normalize_hubspot_oauth_scopes(scopes_raw)

    state_token = secrets.token_urlsafe(32)
    created_at = now_iso()
    expires_at = (datetime.utcnow() + timedelta(minutes=HUBSPOT_OAUTH_STATE_MINUTES)).isoformat()
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute("DELETE FROM HubSpotOAuthState WHERE expires_at <= ?", (now_iso(),))
        cur.execute(
            """
            INSERT INTO HubSpotOAuthState (id, state, redirect_uri, expires_at, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), state_token, redirect_uri, expires_at, created_at),
        )
        conn.commit()

    query = urlparse.urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scopes,
            "state": state_token,
        }
    )
    authorize_url = f"{HUBSPOT_OAUTH_AUTHORIZE_URL}?{query}"
    return HubSpotOAuthStartOut(authorize_url=authorize_url)


@app.get("/api/integrations/hubspot/oauth/callback")
def hubspot_oauth_callback(
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
) -> HTMLResponse:
    if error:
        detail = (error_description or error or "OAuth authorization was denied").strip()
        return hubspot_oauth_popup_response("error", detail)
    if not code or not state:
        return hubspot_oauth_popup_response("error", "Missing code or state")

    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM HubSpotOAuthState
            WHERE state = ? AND expires_at > ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (state, now_iso()),
        )
        state_row = cur.fetchone()
        if not state_row:
            return hubspot_oauth_popup_response("error", "OAuth session expired. Please try again.")
        cur.execute("DELETE FROM HubSpotOAuthState WHERE state = ?", (state,))
        conn.commit()

    client_id = os.getenv("HUBSPOT_CLIENT_ID", "").strip()
    client_secret = os.getenv("HUBSPOT_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        return hubspot_oauth_popup_response("error", "HubSpot OAuth client credentials are not configured")

    try:
        token_payload = exchange_hubspot_oauth_token(
            {
                "grant_type": "authorization_code",
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": state_row["redirect_uri"],
                "code": code,
            }
        )
        access_token = str(token_payload.get("access_token") or "").strip()
        refresh_token = str(token_payload.get("refresh_token") or "").strip()
        hub_id = str(token_payload.get("hub_id") or "").strip()
        expires_in = int(token_payload.get("expires_in") or 0)
        if not access_token or not refresh_token:
            return hubspot_oauth_popup_response("error", "HubSpot OAuth token response was incomplete")

        current = read_hubspot_settings(include_token=True)
        current["oauth_access_token"] = access_token
        current["oauth_refresh_token"] = refresh_token
        current["oauth_expires_at"] = (
            (datetime.utcnow() + timedelta(seconds=max(expires_in - 60, 60))).isoformat()
            if expires_in
            else ""
        )
        current["oauth_hub_id"] = hub_id
        current["oauth_redirect_uri"] = state_row["redirect_uri"]
        if hub_id:
            current["portal_id"] = hub_id
        persist_hubspot_settings(current)
        return hubspot_oauth_popup_response("success", "HubSpot connection complete")
    except Exception as exc:
        if isinstance(exc, HTTPException):
            return hubspot_oauth_popup_response("error", str(exc.detail))
        return hubspot_oauth_popup_response("error", str(exc))


@app.post("/api/integrations/hubspot/oauth/disconnect", response_model=HubSpotSettingsOut)
def disconnect_hubspot_oauth(request: Request) -> HubSpotSettingsOut:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    current = read_hubspot_settings(include_token=True)
    current["oauth_access_token"] = ""
    current["oauth_refresh_token"] = ""
    current["oauth_expires_at"] = ""
    current["oauth_hub_id"] = ""
    persist_hubspot_settings(current)
    return HubSpotSettingsOut(**read_hubspot_settings(include_token=False))


@app.post("/api/integrations/hubspot/test-connection", response_model=HubSpotTestResponse)
def test_hubspot_connection(request: Request) -> HubSpotTestResponse:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    settings = read_hubspot_settings(include_token=True)
    pipelines = list_hubspot_ticket_pipelines(settings)
    return HubSpotTestResponse(status="ok", pipelines_found=len(pipelines))


@app.get("/api/integrations/hubspot/pipelines", response_model=List[HubSpotPipelineOut])
def get_hubspot_ticket_pipelines(request: Request) -> List[HubSpotPipelineOut]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    settings = read_hubspot_settings(include_token=True)
    pipelines = list_hubspot_ticket_pipelines(settings)
    return [HubSpotPipelineOut(**row) for row in pipelines]


@app.get("/api/integrations/hubspot/ticket-properties", response_model=List[HubSpotTicketPropertyOut])
def get_hubspot_ticket_properties(request: Request) -> List[HubSpotTicketPropertyOut]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
    settings = read_hubspot_settings(include_token=True)
    properties = list_hubspot_ticket_properties(settings)
    return [HubSpotTicketPropertyOut(**row) for row in properties]


@app.post("/api/integrations/hubspot/sync-quote/{quote_id}", response_model=HubSpotSyncResponse)
def sync_quote_from_hubspot_endpoint(quote_id: str, request: Request) -> HubSpotSyncResponse:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        result = sync_quote_from_hubspot(conn, quote_id)
    return HubSpotSyncResponse(status="ok", **result)


@app.post("/api/integrations/hubspot/resync-all", response_model=HubSpotBulkResyncResponse)
def resync_all_quotes_to_hubspot(request: Request) -> HubSpotBulkResyncResponse:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        settings = read_hubspot_settings(include_token=True)
        cur = conn.cursor()
        cur.execute("SELECT id FROM Quote ORDER BY created_at DESC")
        quote_ids = [str(row["id"] or "").strip() for row in cur.fetchall() if str(row["id"] or "").strip()]

        attempted_quotes = 0
        can_sync = bool(settings.get("enabled")) and bool(settings.get("sync_quote_to_hubspot"))
        if can_sync:
            for quote_id in quote_ids:
                attempted_quotes += 1
                try:
                    sync_quote_to_hubspot(conn, quote_id, create_if_missing=True)
                except Exception as exc:
                    detail = str(exc)
                    if isinstance(exc, HTTPException):
                        detail = str(exc.detail)
                    update_quote_hubspot_sync_state(conn, quote_id, sync_error=detail)

        mismatches, buckets, clean_quotes = build_hubspot_bulk_mismatch_report(conn)
        status = "ok" if can_sync else "blocked"
        return HubSpotBulkResyncResponse(
            status=status,
            integration_enabled=bool(settings.get("enabled")),
            quote_to_hubspot_sync_enabled=bool(settings.get("sync_quote_to_hubspot")),
            total_quotes=len(quote_ids),
            attempted_quotes=attempted_quotes,
            clean_quotes=clean_quotes,
            mismatch_quotes=len(mismatches),
            buckets=[HubSpotBulkMismatchBucketOut(**bucket) for bucket in buckets],
            mismatches=[HubSpotBulkQuoteMismatchOut(**mismatch) for mismatch in mismatches],
        )


@app.post("/api/integrations/hubspot/card-data")
async def get_hubspot_card_data(request: Request) -> Dict[str, Any]:
    raw_body = await request.body()
    verify_hubspot_request_signature(request, raw_body)

    payload: Dict[str, Any] = {}
    if raw_body:
        try:
            loaded = json.loads(raw_body.decode("utf-8"))
        except Exception:
            raise HTTPException(status_code=400, detail="Request body must be valid JSON")
        if not isinstance(loaded, dict):
            raise HTTPException(status_code=400, detail="Request body must be a JSON object")
        payload = loaded

    properties = payload.get("properties")
    if not isinstance(properties, dict):
        properties = {}
    context = payload.get("context")
    if not isinstance(context, dict):
        context = {}
    crm_context = context.get("crm")
    if not isinstance(crm_context, dict):
        crm_context = {}
    crm_properties = crm_context.get("properties")
    if not isinstance(crm_properties, dict):
        crm_properties = {}

    quote_id = first_non_empty_string(
        payload.get("quote_id"),
        payload.get("level_health_quote_id"),
        payload.get("quoteId"),
        properties.get("level_health_quote_id"),
        crm_properties.get("level_health_quote_id"),
    )
    ticket_id = first_non_empty_string(
        payload.get("ticket_id"),
        payload.get("ticketId"),
        payload.get("objectId"),
        payload.get("object_id"),
        payload.get("hs_object_id"),
        properties.get("hs_ticket_id"),
        properties.get("hs_object_id"),
        crm_context.get("objectId"),
        crm_properties.get("hs_ticket_id"),
        crm_properties.get("hs_object_id"),
    )

    with get_db() as conn:
        quote_row, resolved_by = lookup_quote_for_hubspot_card(
            conn,
            quote_id=quote_id,
            hubspot_ticket_id=ticket_id,
        )
        if not quote_row:
            return {
                "status": "not_found",
                "request": {
                    "quote_id": quote_id,
                    "hubspot_ticket_id": ticket_id,
                },
            }
        return build_hubspot_card_data_for_quote(
            conn,
            quote_row,
            resolved_by=resolved_by or "unknown",
            request_quote_id=quote_id,
            request_ticket_id=ticket_id,
        )


@app.get("/api/quotes", response_model=List[QuoteListOut])
def list_quotes(
    request: Request, role: Optional[str] = None, email: Optional[str] = None
) -> List[QuoteListOut]:
    with get_db() as conn:
        cur = conn.cursor()
        scoped_role, scoped_email = resolve_access_scope(conn, request, role, email)
        where_clause, params = build_access_filter(
            conn,
            scoped_role,
            scoped_email,
            resource="quote",
        )
        cur.execute(f"SELECT * FROM Quote {where_clause} ORDER BY created_at DESC", params)
        rows = cur.fetchall()
        quotes: List[QuoteListOut] = []
        for row in rows:
            cur.execute(
                """
                SELECT recommendation, confidence
                FROM AssignmentRun
                WHERE quote_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (row["id"],),
            )
            assignment = cur.fetchone()
            latest_assignment = dict(assignment) if assignment else None
            quotes.append(
                QuoteListOut(
                    **{
                        **dict(row),
                        "include_specialty": bool(row["include_specialty"]),
                        "needs_action": bool(row["needs_action"]),
                        "agent_of_record": bool(row["agent_of_record"]) if row["agent_of_record"] is not None else None,
                        "latest_assignment": latest_assignment,
                    }
                )
            )
    return quotes


@app.post("/api/quotes", response_model=QuoteOut)
def create_quote(payload: QuoteCreate, request: Request) -> QuoteOut:
    quote_id = str(uuid.uuid4())
    created_at = now_iso()
    status = payload.status or "Draft"
    with get_db() as conn:
        session_user = require_session_user(conn, request)
        session_user_key = session_user_id(session_user)
        if not session_user_key:
            raise HTTPException(status_code=401, detail="Authentication required")
        session_email = normalize_user_email(session_user["email"])
        session_domain = email_domain(session_email)
        session_role = (session_user["role"] or "").strip().lower()
        org = fetch_org_by_domain(conn, "broker", session_domain)
        broker_org = (
            org["name"]
            if org
            else ((session_user["organization"] or "").strip() or broker_org_from_email(session_email))
        )
        cur = conn.cursor()
        normalized_employer_domain = payload.employer_domain.lower() if payload.employer_domain else None
        sponsor_domain = (payload.sponsor_domain or normalized_employer_domain or "").strip().lower() or None
        if session_role == "sponsor" and session_domain:
            sponsor_domain = session_domain
        cur.execute(
            """
            INSERT INTO Quote (
                id, company, employer_street, employer_city, state, employer_zip,
                employer_domain, quote_deadline, employer_sic, effective_date, current_enrolled,
                current_eligible, current_insurance_type, primary_network, secondary_network,
                tpa, stoploss, current_carrier, renewal_comparison, employees_eligible,
                expected_enrollees, broker_fee_pepm, include_specialty, notes,
                high_cost_info, broker_first_name, broker_last_name, broker_email,
                broker_phone, agent_of_record, broker_org, sponsor_domain, assigned_user_id, manual_network, proposal_url, status,
                version, needs_action, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                quote_id,
                payload.company,
                payload.employer_street,
                payload.employer_city,
                payload.state,
                payload.employer_zip,
                normalized_employer_domain,
                payload.quote_deadline,
                payload.employer_sic,
                payload.effective_date,
                payload.current_enrolled,
                payload.current_eligible,
                payload.current_insurance_type,
                payload.primary_network,
                payload.secondary_network,
                payload.tpa,
                payload.stoploss,
                payload.current_carrier,
                payload.renewal_comparison,
                payload.employees_eligible,
                payload.expected_enrollees,
                payload.broker_fee_pepm,
                1 if payload.include_specialty else 0,
                payload.notes,
                payload.high_cost_info,
                (session_user["first_name"] or "").strip() or None,
                (session_user["last_name"] or "").strip() or None,
                session_email,
                (session_user["phone"] or "").strip() or "",
                1 if payload.agent_of_record else 0 if payload.agent_of_record is not None else None,
                broker_org,
                sponsor_domain,
                session_user_key,
                payload.manual_network,
                payload.proposal_url,
                status,
                1,
                1,
                created_at,
                created_at,
            ),
        )
        conn.commit()
        recompute_needs_action(conn, quote_id)
        sync_quote_to_hubspot_async(quote_id, create_if_missing=True)
        row = fetch_quote(conn, quote_id)

    return QuoteOut(
        **{
            **dict(row),
            "include_specialty": bool(row["include_specialty"]),
            "needs_action": bool(row["needs_action"]),
            "agent_of_record": bool(row["agent_of_record"]) if row["agent_of_record"] is not None else None,
        }
    )


@app.get("/api/quotes/{quote_id}")
def get_quote_detail(
    quote_id: str, request: Request, role: Optional[str] = None, email: Optional[str] = None
) -> Dict[str, Any]:
    with get_db() as conn:
        scoped_role, scoped_email = resolve_access_scope(conn, request, role, email)
        if scoped_role != "admin":
            where_clause, params = build_access_filter(
                conn,
                scoped_role,
                scoped_email,
                resource="quote",
            )
            if where_clause:
                cur = conn.cursor()
                cur.execute(
                    f"SELECT id FROM Quote {where_clause} AND id = ? LIMIT 1",
                    [*params, quote_id],
                )
                scoped = cur.fetchone()
                if not scoped:
                    raise HTTPException(status_code=404, detail="Quote not found")
            else:
                raise HTTPException(status_code=404, detail="Quote not found")
        quote = fetch_quote(conn, quote_id)
        cur = conn.cursor()
        cur.execute("SELECT * FROM Upload WHERE quote_id = ? ORDER BY created_at DESC", (quote_id,))
        uploads = [dict(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT * FROM StandardizationRun
            WHERE quote_id = ?
            ORDER BY created_at DESC
            """,
            (quote_id,),
        )
        standardizations = [
            {
                **dict(row),
                "issues_json": json.loads(row["issues_json"]) if row["issues_json"] else [],
            }
            for row in cur.fetchall()
        ]
        cur.execute(
            """
            SELECT * FROM AssignmentRun
            WHERE quote_id = ?
            ORDER BY created_at DESC
            """,
            (quote_id,),
        )
        assignments = [
            {
                **dict(row),
                "result_json": json.loads(row["result_json"]) if row["result_json"] else {},
            }
            for row in cur.fetchall()
        ]
        cur.execute(
            """
            SELECT * FROM Proposal
            WHERE quote_id = ?
            ORDER BY created_at DESC
            """,
            (quote_id,),
        )
        proposals = [dict(row) for row in cur.fetchall()]

    return {
        "quote": {
            **dict(quote),
            "include_specialty": bool(quote["include_specialty"]),
            "needs_action": bool(quote["needs_action"]),
            "agent_of_record": bool(quote["agent_of_record"]) if quote["agent_of_record"] is not None else None,
        },
        "uploads": uploads,
        "standardizations": standardizations,
        "assignments": assignments,
        "proposals": proposals,
    }


@app.get("/api/organizations", response_model=List[OrganizationOut])
def list_organizations(request: Request, org_type: Optional[str] = None) -> List[OrganizationOut]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        sync_organizations_from_records(conn)
        cur = conn.cursor()
        if org_type:
            cur.execute(
                "SELECT * FROM Organization WHERE type = ? ORDER BY name ASC",
                (org_type,),
            )
        else:
            cur.execute("SELECT * FROM Organization ORDER BY name ASC")
        rows = cur.fetchall()
    return [OrganizationOut(**dict(row)) for row in rows]


@app.get("/api/organizations/{org_id}", response_model=OrganizationOut)
def get_organization(org_id: str, request: Request) -> OrganizationOut:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute("SELECT * FROM Organization WHERE id = ?", (org_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Organization not found")
    return OrganizationOut(**dict(row))


@app.post("/api/organizations", response_model=OrganizationOut)
def create_organization(payload: OrganizationIn, request: Request) -> OrganizationOut:
    org_id = str(uuid.uuid4())
    created_at = now_iso()
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO Organization (id, name, type, domain, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                org_id,
                payload.name,
                payload.type,
                payload.domain.lower(),
                created_at,
            ),
        )
        if payload.type == "broker":
            cur.execute(
                """
                UPDATE Quote
                SET broker_org = ?
                WHERE (broker_org IS NULL OR broker_org = '')
                  AND lower(substr(broker_email, instr(broker_email, '@') + 1)) = ?
                """,
                (payload.name, payload.domain.lower()),
            )
            cur.execute(
                """
                UPDATE Installation
                SET broker_org = ?
                WHERE (broker_org IS NULL OR broker_org = '')
                  AND quote_id IN (
                      SELECT id FROM Quote
                      WHERE lower(substr(broker_email, instr(broker_email, '@') + 1)) = ?
                  )
                """,
                (payload.name, payload.domain.lower()),
            )
        if payload.type == "sponsor":
            cur.execute(
                """
                UPDATE Quote
                SET sponsor_domain = ?
                WHERE (sponsor_domain IS NULL OR sponsor_domain = '')
                  AND lower(employer_domain) = ?
                """,
                (payload.domain.lower(), payload.domain.lower()),
            )
            cur.execute(
                """
                UPDATE Installation
                SET sponsor_domain = ?
                WHERE (sponsor_domain IS NULL OR sponsor_domain = '')
                  AND quote_id IN (
                      SELECT id FROM Quote WHERE lower(employer_domain) = ?
                  )
                """,
                (payload.domain.lower(), payload.domain.lower()),
            )
        conn.commit()
        cur.execute("SELECT * FROM Organization WHERE id = ?", (org_id,))
        row = cur.fetchone()
    return OrganizationOut(**dict(row))


@app.patch("/api/organizations/{org_id}", response_model=OrganizationOut)
def update_organization(org_id: str, payload: OrganizationUpdate, request: Request) -> OrganizationOut:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute("SELECT * FROM Organization WHERE id = ?", (org_id,))
        org = cur.fetchone()
        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")
        updates = payload.dict(exclude_unset=True)
        if "domain" in updates and updates["domain"]:
            updates["domain"] = updates["domain"].lower()
        data = {**dict(org), **updates}
        cur.execute(
            """
            UPDATE Organization
            SET name = ?, type = ?, domain = ?
            WHERE id = ?
            """,
            (data["name"], data["type"], data["domain"], org_id),
        )

        if org["type"] == "broker":
            if org["name"] != data["name"]:
                cur.execute(
                    "UPDATE Quote SET broker_org = ? WHERE broker_org = ?",
                    (data["name"], org["name"]),
                )
                cur.execute(
                    "UPDATE Installation SET broker_org = ? WHERE broker_org = ?",
                    (data["name"], org["name"]),
                )
        if data["type"] == "broker":
            cur.execute(
                """
                UPDATE Quote
                SET broker_org = ?
                WHERE (broker_org IS NULL OR broker_org = '' OR broker_org = ?)
                  AND lower(substr(broker_email, instr(broker_email, '@') + 1)) = ?
                """,
                (data["name"], org["name"], data["domain"]),
            )
            cur.execute(
                """
                UPDATE Installation
                SET broker_org = ?
                WHERE (broker_org IS NULL OR broker_org = '' OR broker_org = ?)
                  AND quote_id IN (
                      SELECT id FROM Quote
                      WHERE lower(substr(broker_email, instr(broker_email, '@') + 1)) = ?
                  )
                """,
                (data["name"], org["name"], data["domain"]),
            )
        if org["type"] == "sponsor" or data["type"] == "sponsor":
            if org["domain"] != data["domain"]:
                cur.execute(
                    "UPDATE Quote SET sponsor_domain = ? WHERE sponsor_domain = ?",
                    (data["domain"], org["domain"]),
                )
                cur.execute(
                    "UPDATE Installation SET sponsor_domain = ? WHERE sponsor_domain = ?",
                    (data["domain"], org["domain"]),
                )

        conn.commit()
        cur.execute("SELECT * FROM Organization WHERE id = ?", (org_id,))
        row = cur.fetchone()
    return OrganizationOut(**dict(row))


@app.delete("/api/organizations/{org_id}")
def delete_organization(org_id: str, request: Request) -> Dict[str, str]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute("SELECT * FROM Organization WHERE id = ?", (org_id,))
        org = cur.fetchone()
        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")
        if org["type"] == "broker":
            cur.execute(
                "UPDATE Quote SET broker_org = NULL WHERE broker_org = ?",
                (org["name"],),
            )
            cur.execute(
                "UPDATE Installation SET broker_org = NULL WHERE broker_org = ?",
                (org["name"],),
            )
        if org["type"] == "sponsor":
            cur.execute(
                "UPDATE Quote SET sponsor_domain = NULL WHERE sponsor_domain = ?",
                (org["domain"],),
            )
            cur.execute(
                "UPDATE Installation SET sponsor_domain = NULL WHERE sponsor_domain = ?",
                (org["domain"],),
            )
        cur.execute("DELETE FROM Organization WHERE id = ?", (org_id,))
        conn.commit()
    return {"status": "deleted"}


@app.post("/api/organizations/{org_id}/assign-quotes")
def assign_quotes_to_org(org_id: str, payload: OrganizationAssignIn, request: Request) -> Dict[str, str]:
    quote_ids = payload.quote_ids or []
    if not quote_ids:
        return {"status": "no-op"}
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute("SELECT * FROM Organization WHERE id = ?", (org_id,))
        org = cur.fetchone()
        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")
        placeholders = ",".join(["?"] * len(quote_ids))
        if org["type"] == "broker":
            cur.execute(
                f"""
                UPDATE Quote
                SET broker_org = ?
                WHERE id IN ({placeholders})
                """,
                [org["name"]] + quote_ids,
            )
            cur.execute(
                f"""
                UPDATE Installation
                SET broker_org = ?
                WHERE quote_id IN ({placeholders})
                """,
                [org["name"]] + quote_ids,
            )
        if org["type"] == "sponsor":
            cur.execute(
                f"""
                UPDATE Quote
                SET sponsor_domain = ?, employer_domain = COALESCE(employer_domain, ?)
                WHERE id IN ({placeholders})
                """,
                [org["domain"], org["domain"]] + quote_ids,
            )
            cur.execute(
                f"""
                UPDATE Installation
                SET sponsor_domain = ?
                WHERE quote_id IN ({placeholders})
                """,
                [org["domain"]] + quote_ids,
            )
        conn.commit()
    return {"status": "assigned"}


@app.get("/api/admin/access-requests", response_model=List[AccessRequestAdminOut])
def list_access_requests(request: Request, status: Optional[str] = None, limit: int = 100) -> List[AccessRequestAdminOut]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        query = "SELECT * FROM AccessRequest"
        params: List[Any] = []
        if status is not None:
            normalized_status = normalize_access_request_status(status)
            query += " WHERE status = ?"
            params.append(normalized_status)
        max_limit = max(1, min(500, int(limit)))
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(max_limit)
        cur.execute(query, params)
        rows = cur.fetchall()
    return [to_access_request_admin_out(row) for row in rows]


@app.post("/api/admin/access-requests/{access_request_id}/approve", response_model=AccessRequestAdminOut)
def approve_access_request(
    access_request_id: str,
    payload: AccessRequestDecisionIn,
    request: Request,
) -> AccessRequestAdminOut:
    with get_db() as conn:
        reviewer = require_session_role(conn, request, {"admin"})
        reviewer_id = str(reviewer["id"] or "").strip() if reviewer else None
        cur = conn.cursor()
        cur.execute("SELECT * FROM AccessRequest WHERE id = ?", (access_request_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Access request not found")
        if (row["status"] or "").strip().lower() != "pending":
            raise HTTPException(status_code=400, detail="Only pending access requests can be approved")

        approved_role = normalize_access_request_role(payload.role or row["requested_role"] or "broker")
        approved_email = normalize_user_email(row["email"])
        first_name = str(row["first_name"] or "").strip() or "User"
        last_name = str(row["last_name"] or "").strip() or "Request"
        requested_org = str(row["organization"] or "").strip()
        approved_org_input = (payload.organization or "").strip()
        email_domain_value = email_domain(approved_email)

        if approved_role == "broker":
            approved_org = approved_org_input or broker_org_name_for_domain(conn, email_domain_value) or requested_org
            if not approved_org:
                approved_org = (email_domain_value or "").strip()
        else:
            sponsor_domain = (
                normalize_domain_candidate(approved_org_input)
                or normalize_domain_candidate(row["requested_domain"])
                or normalize_domain_candidate(requested_org)
                or normalize_domain_candidate(email_domain_value)
            )
            if not sponsor_domain:
                raise HTTPException(status_code=400, detail="A sponsor domain is required to approve sponsor access")
            approved_org = sponsor_domain

        ensure_access_request_user(
            conn,
            first_name=first_name,
            last_name=last_name,
            email=approved_email,
            role=approved_role,
            organization=approved_org,
        )

        review_note = (payload.note or "").strip() or None
        cur.execute(
            """
            UPDATE AccessRequest
            SET status = ?, review_note = ?, reviewed_at = ?, reviewed_by_user_id = ?
            WHERE id = ?
            """,
            ("approved", review_note, now_iso(), reviewer_id or None, access_request_id),
        )
        sync_organizations_from_records(conn)
        conn.commit()
        cur.execute("SELECT * FROM AccessRequest WHERE id = ?", (access_request_id,))
        updated = cur.fetchone()
    return to_access_request_admin_out(updated)


@app.post("/api/admin/access-requests/{access_request_id}/reject", response_model=AccessRequestAdminOut)
def reject_access_request(
    access_request_id: str,
    payload: AccessRequestDecisionIn,
    request: Request,
) -> AccessRequestAdminOut:
    with get_db() as conn:
        reviewer = require_session_role(conn, request, {"admin"})
        reviewer_id = str(reviewer["id"] or "").strip() if reviewer else None
        cur = conn.cursor()
        cur.execute("SELECT * FROM AccessRequest WHERE id = ?", (access_request_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Access request not found")
        if (row["status"] or "").strip().lower() != "pending":
            raise HTTPException(status_code=400, detail="Only pending access requests can be rejected")

        review_note = (payload.note or "").strip() or None
        cur.execute(
            """
            UPDATE AccessRequest
            SET status = ?, review_note = ?, reviewed_at = ?, reviewed_by_user_id = ?
            WHERE id = ?
            """,
            ("rejected", review_note, now_iso(), reviewer_id or None, access_request_id),
        )
        conn.commit()
        cur.execute("SELECT * FROM AccessRequest WHERE id = ?", (access_request_id,))
        updated = cur.fetchone()
    return to_access_request_admin_out(updated)


@app.get("/api/users", response_model=List[UserOut])
def list_users(request: Request) -> List[UserOut]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute("SELECT * FROM User ORDER BY last_name ASC, first_name ASC")
        rows = cur.fetchall()
    return [to_user_out(row) for row in rows]


@app.post("/api/users", response_model=UserOut)
def create_user(payload: UserIn, request: Request) -> UserOut:
    user_id = str(uuid.uuid4())
    now = now_iso()
    raw_password = require_valid_password(payload.password, required=True)
    password_salt, password_hash = create_password_credentials(raw_password)
    email = normalize_user_email(payload.email)
    role = normalize_user_role(payload.role)
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO User (
                    id, first_name, last_name, email, phone, job_title, organization, role,
                    password_salt, password_hash, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    payload.first_name.strip(),
                    payload.last_name.strip(),
                    email,
                    payload.phone.strip(),
                    payload.job_title.strip(),
                    payload.organization.strip(),
                    role,
                    password_salt,
                    password_hash,
                    now,
                    now,
                ),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail="Email already exists")
        sync_organizations_from_records(conn)
        conn.commit()
        cur.execute("SELECT * FROM User WHERE id = ?", (user_id,))
        row = cur.fetchone()
    return to_user_out(row)


@app.patch("/api/users/{user_id}", response_model=UserOut)
def update_user(user_id: str, payload: UserUpdate, request: Request) -> UserOut:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        user = fetch_user(conn, user_id)
        updates = payload.dict(exclude_unset=True)
        data = dict(user)
        password_changed = False
        for key, value in updates.items():
            if isinstance(value, str):
                value = value.strip()
            if key == "email" and value:
                value = normalize_user_email(value)
            if key == "role" and value:
                value = normalize_user_role(value)
            data[key] = value
        if "password" in updates:
            password_value = require_valid_password(updates.get("password"), required=True)
            password_salt, password_hash = create_password_credentials(password_value)
            data["password_salt"] = password_salt
            data["password_hash"] = password_hash
            password_changed = True
        data["updated_at"] = now_iso()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE User
                SET first_name = ?, last_name = ?, email = ?, phone = ?, job_title = ?, organization = ?, role = ?,
                    password_salt = ?, password_hash = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    data["first_name"],
                    data["last_name"],
                    data["email"],
                    data.get("phone", ""),
                    data["job_title"],
                    data["organization"],
                    data["role"],
                    data.get("password_salt"),
                    data.get("password_hash"),
                    data["updated_at"],
                    user_id,
                ),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=400, detail="Email already exists")
        if password_changed:
            revoke_user_sessions(conn, user_id)
        sync_organizations_from_records(conn)
        conn.commit()
        cur.execute("SELECT * FROM User WHERE id = ?", (user_id,))
        row = cur.fetchone()
    return to_user_out(row)


@app.delete("/api/users/{user_id}")
def delete_user(user_id: str, request: Request) -> Dict[str, str]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        fetch_user(conn, user_id)
        cur = conn.cursor()
        cur.execute("UPDATE Quote SET assigned_user_id = NULL WHERE assigned_user_id = ?", (user_id,))
        cur.execute("UPDATE Task SET assigned_user_id = NULL WHERE assigned_user_id = ?", (user_id,))
        cur.execute("DELETE FROM Notification WHERE user_id = ?", (user_id,))
        cur.execute("DELETE FROM User WHERE id = ?", (user_id,))
        conn.commit()
    return {"status": "deleted"}


@app.post("/api/users/{user_id}/assign-quotes")
def assign_quotes_to_user(user_id: str, payload: UserAssignIn, request: Request) -> Dict[str, str]:
    quote_ids = payload.quote_ids or []
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        fetch_user(conn, user_id)
        cur = conn.cursor()
        cur.execute("UPDATE Quote SET assigned_user_id = NULL WHERE assigned_user_id = ?", (user_id,))
        if quote_ids:
            placeholders = ",".join(["?"] * len(quote_ids))
            cur.execute(
                f"UPDATE Quote SET assigned_user_id = ? WHERE id IN ({placeholders})",
                [user_id] + quote_ids,
            )
            cur.execute(
                f"SELECT id, company FROM Quote WHERE id IN ({placeholders})",
                quote_ids,
            )
            quote_name_by_id = {str(row["id"]): str(row["company"] or "Quote").strip() for row in cur.fetchall()}
            for quote_id in quote_ids:
                quote_name = quote_name_by_id.get(str(quote_id)) or "Quote"
                create_notification(
                    conn,
                    user_id,
                    kind="quote_assigned",
                    title="Quote assigned",
                    body=f"{quote_name} was assigned to you.",
                    entity_type="quote",
                    entity_id=str(quote_id),
                )
        conn.commit()
    return {"status": "assigned"}


@app.post("/api/users/{user_id}/assign-tasks")
def assign_tasks_to_user(user_id: str, payload: UserAssignIn, request: Request) -> Dict[str, str]:
    task_ids = payload.task_ids or []
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        fetch_user(conn, user_id)
        cur = conn.cursor()
        cur.execute("UPDATE Task SET assigned_user_id = NULL WHERE assigned_user_id = ?", (user_id,))
        if task_ids:
            placeholders = ",".join(["?"] * len(task_ids))
            cur.execute(
                f"UPDATE Task SET assigned_user_id = ? WHERE id IN ({placeholders})",
                [user_id] + task_ids,
            )
            cur.execute(
                f"""
                SELECT t.id, t.title, t.installation_id, i.company
                FROM Task t
                JOIN Installation i ON i.id = t.installation_id
                WHERE t.id IN ({placeholders})
                """,
                task_ids,
            )
            rows = cur.fetchall()
            task_meta = {
                str(row["id"]): {
                    "title": str(row["title"] or "Task").strip(),
                    "installation_id": str(row["installation_id"] or "").strip() or None,
                    "company": str(row["company"] or "Implementation").strip(),
                }
                for row in rows
            }
            for task_id in task_ids:
                task_data = task_meta.get(str(task_id))
                if not task_data:
                    continue
                create_notification(
                    conn,
                    user_id,
                    kind="task_assigned",
                    title="Task assigned",
                    body=f"{task_data['title']} for {task_data['company']} was assigned to you.",
                    entity_type="installation",
                    entity_id=task_data["installation_id"],
                )
        conn.commit()
    return {"status": "assigned"}


@app.get("/api/organizations/{org_id}/users", response_model=List[UserOut])
def list_organization_users(org_id: str, request: Request) -> List[UserOut]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute("SELECT * FROM Organization WHERE id = ?", (org_id,))
        org = cur.fetchone()
        if not org:
            raise HTTPException(status_code=404, detail="Organization not found")
        org_name = (org["name"] or "").strip().lower()
        org_domain = (org["domain"] or "").strip().lower()
        cur.execute(
            """
            SELECT * FROM User
            WHERE lower(trim(organization)) = ?
               OR lower(trim(organization)) = ?
            ORDER BY last_name ASC, first_name ASC
            """,
            (org_name, org_domain),
        )
        rows = cur.fetchall()
    return [to_user_out(row) for row in rows]


@app.get("/api/tasks", response_model=List[TaskListOut])
def list_tasks(request: Request, role: Optional[str] = None, email: Optional[str] = None) -> List[TaskListOut]:
    with get_db() as conn:
        cur = conn.cursor()
        scoped_role, scoped_email = resolve_access_scope(conn, request, role, email)
        where_clause, params = build_access_filter(
            conn,
            scoped_role,
            scoped_email,
            resource="task",
        )
        order_clause = "ORDER BY i.created_at DESC, t.title ASC"
        if scoped_role in {"broker", "sponsor"}:
            order_clause = """
            ORDER BY
                CASE
                    WHEN t.due_date IS NULL OR trim(t.due_date) = '' THEN 1
                    ELSE 0
                END ASC,
                t.due_date ASC,
                i.company ASC,
                t.title ASC
            """
        cur.execute(
            f"""
            SELECT
                t.*,
                i.company as installation_company
            FROM Task t
            JOIN Installation i ON i.id = t.installation_id
            {where_clause}
            {order_clause}
            """,
            params,
        )
        rows = cur.fetchall()
    return [TaskListOut(**dict(row)) for row in rows]


@app.get("/api/notifications", response_model=List[NotificationOut])
def list_notifications(request: Request, limit: int = 50) -> List[NotificationOut]:
    safe_limit = max(1, min(limit, 200))
    with get_db() as conn:
        session_user = require_session_user(conn, request)
        viewer_user_id = session_user_id(session_user)
        if not viewer_user_id:
            raise HTTPException(status_code=401, detail="Authentication required")
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM Notification
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (viewer_user_id, safe_limit),
        )
        rows = cur.fetchall()
    return [to_notification_out(row) for row in rows]


@app.get("/api/notifications/unread-count", response_model=NotificationUnreadCountOut)
def get_notification_unread_count(request: Request) -> NotificationUnreadCountOut:
    with get_db() as conn:
        session_user = require_session_user(conn, request)
        viewer_user_id = session_user_id(session_user)
        if not viewer_user_id:
            raise HTTPException(status_code=401, detail="Authentication required")
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM Notification
            WHERE user_id = ? AND COALESCE(is_read, 0) = 0
            """,
            (viewer_user_id,),
        )
        unread_count = int(cur.fetchone()["cnt"] or 0)
    return NotificationUnreadCountOut(unread_count=unread_count)


@app.post("/api/notifications/{notification_id}/read", response_model=NotificationOut)
def mark_notification_read(notification_id: str, request: Request) -> NotificationOut:
    with get_db() as conn:
        session_user = require_session_user(conn, request)
        viewer_user_id = session_user_id(session_user)
        if not viewer_user_id:
            raise HTTPException(status_code=401, detail="Authentication required")
        read_at = now_iso()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE Notification
            SET is_read = 1, read_at = ?
            WHERE id = ? AND user_id = ?
            """,
            (read_at, notification_id, viewer_user_id),
        )
        cur.execute(
            "SELECT * FROM Notification WHERE id = ? AND user_id = ?",
            (notification_id, viewer_user_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Notification not found")
        conn.commit()
    return to_notification_out(row)


@app.post("/api/notifications/read-all")
def mark_all_notifications_read(request: Request) -> Dict[str, Any]:
    with get_db() as conn:
        session_user = require_session_user(conn, request)
        viewer_user_id = session_user_id(session_user)
        if not viewer_user_id:
            raise HTTPException(status_code=401, detail="Authentication required")
        read_at = now_iso()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE Notification
            SET is_read = 1, read_at = ?
            WHERE user_id = ? AND COALESCE(is_read, 0) = 0
            """,
            (read_at, viewer_user_id),
        )
        updated_count = int(cur.rowcount or 0)
        conn.commit()
    return {"status": "ok", "updated_count": updated_count}


@app.patch("/api/quotes/{quote_id}", response_model=QuoteOut)
def update_quote(quote_id: str, payload: QuoteUpdate, request: Request) -> QuoteOut:
    with get_db() as conn:
        quote = fetch_quote(conn, quote_id)
        data = dict(quote)
        updates = payload.dict(exclude_unset=True)
        broker_info_fields = {
            "broker_first_name",
            "broker_last_name",
            "broker_email",
            "broker_phone",
            "agent_of_record",
            "broker_org",
            "broker_fee_pepm",
        }
        hubspot_detail_fields = {
            "primary_network",
            "secondary_network",
            "tpa",
            "stoploss",
            "current_carrier",
            "renewal_comparison",
        }
        if any(field in updates for field in (broker_info_fields | hubspot_detail_fields)):
            session_user = get_session_user(conn, request)
            role = str(session_user["role"] or "").strip().lower() if session_user else ""
            if role != "admin":
                raise HTTPException(
                    status_code=403,
                    detail="Only admin can edit broker or integration detail fields",
                )
        if "broker_email" in updates and "broker_org" not in updates:
            domain = email_domain(updates.get("broker_email"))
            org = fetch_org_by_domain(conn, "broker", domain)
            inferred_org = org["name"] if org else broker_org_from_email(updates.get("broker_email"))
            if inferred_org:
                updates["broker_org"] = inferred_org
        if "employer_domain" in updates and "sponsor_domain" not in updates:
            if updates.get("employer_domain"):
                updates["sponsor_domain"] = updates["employer_domain"].lower()
        if "manual_network" in updates and "primary_network" not in updates:
            manual_network = str(updates.get("manual_network") or "").strip()
            if manual_network:
                updates["manual_network"] = manual_network
                updates["primary_network"] = manual_network
            else:
                updates["manual_network"] = None
                updates["primary_network"] = latest_assignment_primary_network(conn, quote_id)
        for key, value in updates.items():
            if key == "include_specialty" and value is not None:
                value = 1 if value else 0
            if key == "needs_action" and value is not None:
                value = 1 if value else 0
            if key == "agent_of_record" and value is not None:
                value = 1 if value else 0
            data[key] = value
        data["updated_at"] = now_iso()
        columns = [
            "company",
            "employer_street",
            "employer_city",
            "state",
            "employer_zip",
            "employer_domain",
            "quote_deadline",
            "employer_sic",
            "effective_date",
            "current_enrolled",
            "current_eligible",
            "current_insurance_type",
            "primary_network",
            "secondary_network",
            "tpa",
            "stoploss",
            "current_carrier",
            "renewal_comparison",
            "employees_eligible",
            "expected_enrollees",
            "broker_fee_pepm",
            "include_specialty",
            "notes",
            "high_cost_info",
            "broker_first_name",
            "broker_last_name",
            "broker_email",
            "broker_phone",
            "agent_of_record",
            "broker_org",
            "sponsor_domain",
            "assigned_user_id",
            "manual_network",
            "proposal_url",
            "status",
            "version",
            "needs_action",
            "updated_at",
        ]
        cur = conn.cursor()
        cur.execute(
            f"""
            UPDATE Quote SET
                {", ".join([f"{c} = ?" for c in columns])}
            WHERE id = ?
            """,
            [data.get(c) for c in columns] + [quote_id],
        )
        install_updates: Dict[str, Optional[str]] = {}
        if "broker_org" in updates and data.get("broker_org") != quote["broker_org"]:
            install_updates["broker_org"] = data.get("broker_org")
        if "sponsor_domain" in updates and data.get("sponsor_domain") != quote["sponsor_domain"]:
            install_updates["sponsor_domain"] = data.get("sponsor_domain")
        if install_updates:
            install_columns = list(install_updates.keys())
            cur.execute(
                f"""
                UPDATE Installation SET
                    {", ".join([f"{c} = ?" for c in install_columns])},
                    updated_at = ?
                WHERE quote_id = ?
                """,
                [install_updates[c] for c in install_columns] + [data["updated_at"], quote_id],
            )
        previous_assigned_user_id = (quote["assigned_user_id"] or "").strip() or None
        updated_assigned_user_id = (data.get("assigned_user_id") or "").strip() or None
        if updated_assigned_user_id and updated_assigned_user_id != previous_assigned_user_id:
            quote_name = str(data.get("company") or quote["company"] or "Quote").strip() or "Quote"
            create_notification(
                conn,
                updated_assigned_user_id,
                kind="quote_assigned",
                title="Quote assigned",
                body=f"{quote_name} was assigned to you.",
                entity_type="quote",
                entity_id=quote_id,
            )
        conn.commit()
        sync_quote_to_hubspot_async(quote_id, create_if_missing=True)
        row = fetch_quote(conn, quote_id)

    return QuoteOut(
        **{
            **dict(row),
            "include_specialty": bool(row["include_specialty"]),
            "needs_action": bool(row["needs_action"]),
            "agent_of_record": bool(row["agent_of_record"]) if row["agent_of_record"] is not None else None,
        }
    )


@app.post("/api/quotes/{quote_id}/uploads", response_model=UploadOut)
def upload_quote_file(
    quote_id: str,
    type: str = Form(...),
    file: UploadFile = File(...),
) -> UploadOut:
    with get_db() as conn:
        fetch_quote(conn, quote_id)
    upload = save_upload(quote_id, type, file)
    sync_quote_to_hubspot_async(quote_id, create_if_missing=True)
    return upload


@app.get("/api/quotes/{quote_id}/uploads", response_model=List[UploadOut])
def list_quote_uploads(quote_id: str) -> List[UploadOut]:
    with get_db() as conn:
        fetch_quote(conn, quote_id)
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM Upload WHERE quote_id = ? ORDER BY created_at DESC",
            (quote_id,),
        )
        rows = cur.fetchall()
    return [UploadOut(**dict(row)) for row in rows]


@app.delete("/api/quotes/{quote_id}/uploads/{upload_id}")
def delete_quote_upload(quote_id: str, upload_id: str) -> Dict[str, str]:
    with get_db() as conn:
        fetch_quote(conn, quote_id)
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM Upload WHERE id = ? AND quote_id = ?",
            (upload_id, quote_id),
        )
        upload = cur.fetchone()
        if not upload:
            raise HTTPException(status_code=404, detail="Upload not found")
        try:
            Path(upload["path"]).unlink(missing_ok=True)
        except Exception:
            pass
        cur.execute("DELETE FROM Upload WHERE id = ?", (upload_id,))
        cur.execute("DELETE FROM HubSpotTicketAttachmentSync WHERE upload_id = ?", (upload_id,))
        conn.commit()
        recompute_needs_action(conn, quote_id)
        sync_quote_to_hubspot_async(quote_id, create_if_missing=True)
    return {"status": "deleted"}


@app.delete("/api/quotes/{quote_id}")
def delete_quote(quote_id: str, request: Request) -> Dict[str, str]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        fetch_quote(conn, quote_id)
        cleanup_result = delete_quote_with_dependencies(conn, quote_id)
        conn.commit()

    remove_quote_artifacts(
        quote_id,
        cleanup_result["installation_ids"],
        cleanup_result["files_to_remove"],
    )

    return {"status": "deleted"}


@app.post("/api/admin/cleanup-unassigned-records")
def cleanup_unassigned_records(request: Request) -> Dict[str, Any]:
    quote_ids: List[str] = []
    installation_ids_to_remove: List[str] = []
    files_to_remove: List[str] = []
    task_installation_ids_to_touch: List[str] = []
    deleted_task_count_from_quotes = 0
    deleted_task_count_direct = 0

    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()

        cur.execute(
            """
            SELECT q.id
            FROM Quote q
            LEFT JOIN User u ON u.id = q.assigned_user_id
            WHERE trim(COALESCE(q.assigned_user_id, '')) = '' OR u.id IS NULL
            ORDER BY q.created_at DESC
            """
        )
        quote_ids = [row["id"] for row in cur.fetchall()]

        for quote_id in quote_ids:
            cleanup_result = delete_quote_with_dependencies(conn, quote_id)
            files_to_remove.extend(cleanup_result["files_to_remove"])
            installation_ids_to_remove.extend(cleanup_result["installation_ids"])
            deleted_task_count_from_quotes += int(cleanup_result["deleted_task_count"] or 0)

        cur.execute(
            """
            SELECT t.id, t.installation_id
            FROM Task t
            LEFT JOIN User u ON u.id = t.assigned_user_id
            WHERE trim(COALESCE(t.assigned_user_id, '')) = '' OR u.id IS NULL
            """
        )
        task_rows = cur.fetchall()
        task_ids = [row["id"] for row in task_rows]
        task_installation_ids_to_touch = [
            row["installation_id"] for row in task_rows if row["installation_id"]
        ]
        deleted_task_count_direct = len(task_ids)

        if task_ids:
            placeholders = ",".join(["?"] * len(task_ids))
            cur.execute(f"DELETE FROM Task WHERE id IN ({placeholders})", task_ids)

        if task_installation_ids_to_touch:
            unique_installation_ids = sorted(set(task_installation_ids_to_touch))
            placeholders = ",".join(["?"] * len(unique_installation_ids))
            cur.execute(
                f"UPDATE Installation SET updated_at = ? WHERE id IN ({placeholders})",
                [now_iso(), *unique_installation_ids],
            )
            task_installation_ids_to_touch = unique_installation_ids

        conn.commit()

    for quote_id in dict.fromkeys(quote_ids):
        quote_dir = UPLOADS_DIR / quote_id
        try:
            if quote_dir.exists():
                shutil.rmtree(quote_dir, ignore_errors=True)
        except Exception:
            pass

    for path_value in dict.fromkeys(files_to_remove):
        remove_upload_file(path_value)

    for installation_id in dict.fromkeys(installation_ids_to_remove):
        install_dir = UPLOADS_DIR / f"installation-{installation_id}"
        try:
            if install_dir.exists():
                shutil.rmtree(install_dir, ignore_errors=True)
        except Exception:
            pass

    return {
        "status": "cleaned",
        "deleted_quote_count": len(quote_ids),
        "deleted_task_count": deleted_task_count_from_quotes + deleted_task_count_direct,
        "deleted_task_count_from_quotes": deleted_task_count_from_quotes,
        "deleted_task_count_direct": deleted_task_count_direct,
        "updated_installation_count": len(task_installation_ids_to_touch),
    }


@app.post("/api/admin/backfill-installation-orgs", response_model=InstallationOrgBackfillOut)
def backfill_installation_orgs(request: Request) -> InstallationOrgBackfillOut:
    scanned_installation_count = 0
    updated_installation_count = 0
    updated_broker_org_count = 0
    updated_sponsor_domain_count = 0
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                i.id AS installation_id,
                i.broker_org AS installation_broker_org,
                i.sponsor_domain AS installation_sponsor_domain,
                q.broker_org AS quote_broker_org,
                q.sponsor_domain AS quote_sponsor_domain
            FROM Installation i
            JOIN Quote q ON q.id = i.quote_id
            """
        )
        rows = cur.fetchall()
        scanned_installation_count = len(rows)
        for row in rows:
            sets: List[str] = []
            params: List[Optional[str]] = []
            if (row["installation_broker_org"] or "") != (row["quote_broker_org"] or ""):
                sets.append("broker_org = ?")
                params.append(row["quote_broker_org"])
                updated_broker_org_count += 1
            if (row["installation_sponsor_domain"] or "") != (row["quote_sponsor_domain"] or ""):
                sets.append("sponsor_domain = ?")
                params.append(row["quote_sponsor_domain"])
                updated_sponsor_domain_count += 1
            if sets:
                sets.append("updated_at = ?")
                params.append(now_iso())
                params.append(row["installation_id"])
                cur.execute(
                    f"UPDATE Installation SET {', '.join(sets)} WHERE id = ?",
                    params,
                )
                updated_installation_count += 1
        conn.commit()
    return InstallationOrgBackfillOut(
        status="backfilled",
        scanned_installation_count=scanned_installation_count,
        updated_installation_count=updated_installation_count,
        updated_broker_org_count=updated_broker_org_count,
        updated_sponsor_domain_count=updated_sponsor_domain_count,
    )


@app.post("/api/quotes/{quote_id}/standardize", response_model=StandardizationOut)
def run_standardization(
    quote_id: str, payload: Optional[StandardizationIn] = None
) -> StandardizationOut:
    payload = payload or StandardizationIn()
    with get_db() as conn:
        fetch_quote(conn, quote_id)
        census = latest_census_upload(conn, quote_id)
        if not census:
            raise HTTPException(status_code=400, detail="Census upload required before standardization")

        issues: List[Dict[str, Any]] = []
        file_path = Path(census["path"])
        if not file_path.exists():
            raise HTTPException(status_code=400, detail="Census file not found")

        gender_map = {k.lower(): v for k, v in (payload.gender_map or {}).items()}
        relationship_map = {k.lower(): v for k, v in (payload.relationship_map or {}).items()}
        tier_map = {k.lower(): v for k, v in (payload.tier_map or {}).items()}
        header_map = payload.header_map or {}

        required_fields = {
            "first_name": ["first name", "firstname", "first_name"],
            "last_name": ["last name", "lastname", "last_name"],
            "dob": [
                "dob",
                "date of birth",
                "birthdate",
                "mbr dob",
                "member dob",
                "mbr date of birth",
                "member date of birth",
            ],
            "zip": ["zip", "zipcode", "zip code", "postal code", "home zip code", "home zipcode"],
            "gender": ["gender", "sex"],
            "relationship": ["relationship", "rel", "relat"],
            "enrollment_tier": ["enrollment tier", "tier", "coverage tier"],
        }

        def normalize_header(value: str) -> str:
            return "".join(ch.lower() for ch in value.strip() if ch.isalnum())

        def find_header(headers: List[str], aliases: List[str]) -> Optional[str]:
            header_map = {normalize_header(h): h for h in headers}
            for alias in aliases:
                key = normalize_header(alias)
                if key in header_map:
                    return header_map[key]
            return None

        def add_issue(
            row_num: int,
            field: str,
            issue: str,
            value: Optional[str] = None,
            mapped_value: Optional[str] = None,
            rule: Optional[str] = None,
        ) -> None:
            entry: Dict[str, Any] = {"row": row_num, "field": field, "issue": issue}
            if value is not None:
                entry["value"] = value
            if mapped_value is not None:
                entry["mapped_value"] = mapped_value
            if rule:
                entry["rule"] = rule
            issues.append(entry)
            if row_num > 0:
                issue_row_set.add(row_num)

        detected_headers: List[str] = []
        sample_data: Dict[str, List[str]] = {}
        sample_rows: List[Dict[str, Any]] = []
        standardized_filename: Optional[str] = None
        standardized_path: Optional[str] = None
        total_rows = 0
        issue_row_set: set[int] = set()

        detected_headers, rows = load_census_rows(file_path)
        sample_data = {header: [] for header in detected_headers}

        header_lookup: Dict[str, Optional[str]] = {}
        for key, aliases in required_fields.items():
            if key in header_map:
                mapped_header = header_map[key]
                if mapped_header in detected_headers:
                    header_lookup[key] = mapped_header
                else:
                    header_lookup[key] = None
                    add_issue(1, key, f"Mapped column not found: {mapped_header}")
            else:
                header_lookup[key] = find_header(detected_headers, aliases)
            if not header_lookup[key]:
                add_issue(1, key, "Missing required column")

        allowed_gender = {"M", "F"}
        allowed_relationship = {"E", "S", "C"}
        allowed_tier = {"EE", "ES", "EC", "EF", "W"}
        today_date = datetime.utcnow().date()

        required_field_messages = {
            "first_name": "First Name is required",
            "last_name": "Last Name is required",
            "dob": "DOB is required",
            "zip": "Zip is required",
            "gender": "Gender must be M or F",
            "relationship": "Relationship must be E, S, or C",
            "enrollment_tier": "Enrollment Tier must be EE, ES, EC, EF, or W",
        }
        required_field_rule_ids = {
            "first_name": "F1",
            "last_name": "F2",
            "dob": "F3",
            "zip": "F4",
            "gender": "F5",
            "relationship": "F6",
            "enrollment_tier": "F7",
        }

        def age_on_date(dob_date: date, on_date: date) -> int:
            return on_date.year - dob_date.year - (
                (on_date.month, on_date.day) < (dob_date.month, dob_date.day)
            )

        header_to_required_field = {
            header: key for key, header in header_lookup.items() if header
        }

        standardized_rows: List[Dict[str, str]] = []
        validated_rows: List[Dict[str, Any]] = []
        for idx, row in enumerate(rows, start=2):
            # Skip completely empty rows (common in Excel exports)
            if all((str(value).strip() == "" for value in row.values())):
                continue
            standardized_row: Dict[str, str] = {}
            row_state: Dict[str, Any] = {"row": idx, "relationship": "", "enrollment_tier": "", "dob_date": None}
            total_rows += 1
            for header in detected_headers:
                if len(sample_data[header]) >= 3:
                    continue
                raw_value = (row.get(header) or "").strip()
                if raw_value:
                    sample_value = raw_value
                    if header_to_required_field.get(header) == "dob":
                        parsed, normalized_dob = normalize_census_dob(raw_value)
                        if parsed:
                            sample_value = normalized_dob
                    sample_data[header].append(sample_value)

            for key, header in header_lookup.items():
                if not header:
                    continue
                raw_value = str(row.get(header) or "").strip()
                if raw_value == "":
                    add_issue(
                        idx,
                        key,
                        required_field_messages[key],
                        value=raw_value,
                        rule=required_field_rule_ids[key],
                    )
                    standardized_row[key] = ""
                    continue

                if key == "first_name" or key == "last_name":
                    standardized_row[key] = raw_value
                    continue

                if key == "dob":
                    parsed, normalized_dob = normalize_census_dob(raw_value)
                    standardized_row[key] = normalized_dob
                    if not parsed:
                        add_issue(
                            idx,
                            key,
                            "DOB is invalid",
                            value=raw_value,
                            mapped_value=normalized_dob,
                            rule="F3",
                        )
                        continue
                    try:
                        dob_date = datetime.strptime(normalized_dob, "%Y-%m-%d").date()
                    except Exception:
                        dob_date = None
                    if dob_date is None:
                        add_issue(
                            idx,
                            key,
                            "DOB is invalid",
                            value=raw_value,
                            mapped_value=normalized_dob,
                            rule="F3",
                        )
                        continue
                    row_state["dob_date"] = dob_date
                    if dob_date > today_date:
                        add_issue(
                            idx,
                            key,
                            "DOB cannot be in the future",
                            value=raw_value,
                            mapped_value=normalized_dob,
                            rule="F3",
                        )
                    continue

                if key == "zip":
                    normalized_zip = normalize_member_zip(raw_value)
                    standardized_row[key] = normalized_zip or raw_value
                    if not normalized_zip:
                        add_issue(
                            idx,
                            key,
                            "Zip is invalid format",
                            value=raw_value,
                            rule="F4",
                        )
                    continue

                if key == "gender":
                    default_gender_map = {"m": "M", "male": "M", "f": "F", "female": "F"}
                    mapped = (
                        gender_map.get(raw_value.lower())
                        or default_gender_map.get(raw_value.lower())
                        or raw_value
                    ).upper()
                    standardized_row[key] = mapped
                    if mapped not in allowed_gender:
                        add_issue(
                            idx,
                            key,
                            "Gender must be M or F",
                            value=raw_value,
                            mapped_value=mapped,
                            rule="F5",
                        )
                    continue

                if key == "relationship":
                    mapped = relationship_map.get(raw_value.lower(), raw_value).upper()
                    standardized_row[key] = mapped
                    row_state["relationship"] = mapped
                    if mapped not in allowed_relationship:
                        add_issue(
                            idx,
                            key,
                            "Relationship must be E, S, or C",
                            value=raw_value,
                            mapped_value=mapped,
                            rule="F6",
                        )
                    continue

                if key == "enrollment_tier":
                    mapped = tier_map.get(raw_value.lower(), raw_value).upper()
                    standardized_row[key] = mapped
                    row_state["enrollment_tier"] = mapped
                    if mapped not in allowed_tier:
                        add_issue(
                            idx,
                            key,
                            "Enrollment Tier must be EE, ES, EC, EF, or W",
                            value=raw_value,
                            mapped_value=mapped,
                            rule="F7",
                        )
                    continue

                standardized_row[key] = raw_value

            if standardized_row:
                validated_rows.append(row_state)
                standardized_rows.append(standardized_row)
                if len(sample_rows) < 20:
                    sample_rows.append(
                        {
                            "row": idx,
                            **{field: standardized_row.get(field, "") for field in required_fields.keys()},
                        }
                    )

        employee_rows = [row for row in validated_rows if row.get("relationship") == "E"]
        if len(employee_rows) == 0:
            add_issue(1, "relationship", "No Employee rows found on census", rule="R1")

        for row in validated_rows:
            relationship = str(row.get("relationship") or "")
            tier = str(row.get("enrollment_tier") or "")
            row_num = int(row.get("row") or 0)
            dob_date = row.get("dob_date")
            if isinstance(dob_date, datetime):
                dob_date = dob_date.date()
            if relationship == "C" and isinstance(dob_date, date):
                if age_on_date(dob_date, today_date) >= 26:
                    add_issue(
                        row_num,
                        "relationship",
                        "Dependent age 26+ may not be eligible as a Child - verify",
                        value=str(dob_date),
                        rule="R2",
                    )
            if relationship == "E" and isinstance(dob_date, date):
                if age_on_date(dob_date, today_date) < 18:
                    add_issue(
                        row_num,
                        "relationship",
                        "Employee DOB indicates age under 18 - verify",
                        value=str(dob_date),
                        rule="R3",
                    )
            if relationship in {"S", "C"} and tier in {"EE", "W"}:
                add_issue(
                    row_num,
                    "enrollment_tier",
                    "Spouse/Child cannot have a tier of EE or W",
                    value=tier,
                    rule="R4",
                )

        ee_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "E" and row.get("enrollment_tier") == "EE"
        )
        es_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "E" and row.get("enrollment_tier") == "ES"
        )
        ec_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "E" and row.get("enrollment_tier") == "EC"
        )
        ef_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "E" and row.get("enrollment_tier") == "EF"
        )
        w_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "E" and row.get("enrollment_tier") == "W"
        )
        s_es_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "S" and row.get("enrollment_tier") == "ES"
        )
        s_ef_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "S" and row.get("enrollment_tier") == "EF"
        )
        c_ec_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "C" and row.get("enrollment_tier") == "EC"
        )
        c_ef_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "C" and row.get("enrollment_tier") == "EF"
        )
        spouse_ee_or_w_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "S" and row.get("enrollment_tier") in {"EE", "W"}
        )
        spouse_ec_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "S" and row.get("enrollment_tier") == "EC"
        )
        child_ee_or_w_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "C" and row.get("enrollment_tier") in {"EE", "W"}
        )
        child_es_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") == "C" and row.get("enrollment_tier") == "ES"
        )
        total_spouse_rows = sum(
            1 for row in validated_rows if row.get("relationship") == "S"
        )
        total_child_rows = sum(
            1 for row in validated_rows if row.get("relationship") == "C"
        )
        dependent_ee_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") in {"S", "C"} and row.get("enrollment_tier") == "EE"
        )
        dependent_w_count = sum(
            1
            for row in validated_rows
            if row.get("relationship") in {"S", "C"} and row.get("enrollment_tier") == "W"
        )

        if w_count > 0 and dependent_w_count > 0:
            add_issue(
                1,
                "enrollment_tier",
                "Dependents should not appear on census under a Waived employee",
                rule="R5",
            )
        if s_es_count != es_count:
            add_issue(
                1,
                "enrollment_tier",
                f"Mismatch: {es_count} ES employee(s) but {s_es_count} ES spouse(s) found - expected equal counts",
                rule="TC1",
            )
        if s_ef_count != ef_count:
            add_issue(
                1,
                "enrollment_tier",
                f"Mismatch: {ef_count} EF employee(s) but {s_ef_count} EF spouse(s) found - expected equal counts",
                rule="TC2",
            )
        if spouse_ee_or_w_count > 0:
            add_issue(
                1,
                "enrollment_tier",
                "Spouse row found with invalid tier EE or W",
                rule="TC3",
            )
        if spouse_ec_count > 0:
            add_issue(
                1,
                "enrollment_tier",
                "Spouse row with Tier EC found - EC does not include a spouse",
                rule="TC4",
            )
        if c_ec_count < ec_count:
            add_issue(
                1,
                "enrollment_tier",
                f"Mismatch: {ec_count} EC employee(s) but only {c_ec_count} EC child row(s) - each EC employee needs at least one child",
                rule="TC5",
            )
        if c_ef_count < ef_count:
            add_issue(
                1,
                "enrollment_tier",
                f"Mismatch: {ef_count} EF employee(s) but only {c_ef_count} EF child row(s) - each EF employee needs at least one child",
                rule="TC6",
            )
        if child_ee_or_w_count > 0:
            add_issue(
                1,
                "enrollment_tier",
                "Child row found with invalid tier EE or W",
                rule="TC7",
            )
        if child_es_count > 0:
            add_issue(
                1,
                "enrollment_tier",
                "Child row with Tier ES found - ES does not include a child",
                rule="TC8",
            )
        if total_spouse_rows != (s_es_count + s_ef_count):
            add_issue(
                1,
                "enrollment_tier",
                "Spouse row(s) found with a tier that does not correspond to any employee tier (ES or EF)",
                rule="TC9",
            )
        if total_child_rows != (c_ec_count + c_ef_count):
            add_issue(
                1,
                "enrollment_tier",
                "Child row(s) found with a tier that does not correspond to any employee tier (EC or EF)",
                rule="TC10",
            )
        if dependent_ee_count > 0:
            add_issue(
                1,
                "enrollment_tier",
                "Dependent rows found carrying EE tier - EE is Employee Only",
                rule="TC11",
            )
        if dependent_w_count > 0:
            add_issue(
                1,
                "enrollment_tier",
                "Dependent rows found carrying W (Waived) tier",
                rule="TC12",
            )

        if all(header_lookup.values()):
            quote_dir = UPLOADS_DIR / quote_id
            quote_dir.mkdir(parents=True, exist_ok=True)
            standardized_filename = f"standardized-{uuid.uuid4()}.csv"
            standardized_path = str(quote_dir / standardized_filename)
            with open(standardized_path, "w", newline="", encoding="utf-8") as out_f:
                fieldnames = list(required_fields.keys())
                writer = csv.DictWriter(out_f, fieldnames=fieldnames)
                writer.writeheader()
                for row in standardized_rows:
                    writer.writerow({field: row.get(field, "") for field in fieldnames})

        status = "Complete" if len(issues) == 0 else "Issues Found"
        run_id = str(uuid.uuid4())
        created_at = now_iso()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO StandardizationRun (
                id, quote_id, issues_json, issue_count, status,
                standardized_filename, standardized_path, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                quote_id,
                json.dumps(issues),
                len(issues),
                status,
                standardized_filename,
                standardized_path,
                created_at,
            ),
        )
        conn.commit()
        recompute_needs_action(conn, quote_id)

    return StandardizationOut(
        id=run_id,
        quote_id=quote_id,
        issues_json=issues,
        issue_count=len(issues),
        status=status,
        detected_headers=detected_headers,
        sample_data=sample_data,
        sample_rows=sample_rows,
        total_rows=total_rows,
        issue_rows=len(issue_row_set),
        standardized_filename=standardized_filename,
        standardized_path=standardized_path,
        created_at=created_at,
    )


@app.post("/api/quotes/{quote_id}/standardize/resolve", response_model=StandardizationOut)
def resolve_standardization(
    quote_id: str, payload: StandardizationResolveIn
) -> StandardizationOut:
    with get_db() as conn:
        fetch_quote(conn, quote_id)
        run_id = str(uuid.uuid4())
        created_at = now_iso()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO StandardizationRun (
                id, quote_id, issues_json, issue_count, status,
                standardized_filename, standardized_path, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                quote_id,
                json.dumps(payload.issues_json),
                0,
                "Resolved",
                None,
                None,
                created_at,
            ),
        )
        conn.commit()
        recompute_needs_action(conn, quote_id)

    return StandardizationOut(
        id=run_id,
        quote_id=quote_id,
        issues_json=payload.issues_json,
        issue_count=0,
        status="Resolved",
        detected_headers=[],
        sample_data={},
        sample_rows=[],
        total_rows=0,
        issue_rows=0,
        standardized_filename=None,
        standardized_path=None,
        created_at=created_at,
    )


@app.post("/api/quotes/{quote_id}/assign-network", response_model=AssignmentOut)
def run_assignment(quote_id: str) -> AssignmentOut:
    with get_db() as conn:
        fetch_quote(conn, quote_id)
        census = latest_census_upload(conn, quote_id)
        if not census:
            raise HTTPException(status_code=400, detail="Census upload required before network assignment")

        mapping_path = (BASE_DIR / "data" / "network_mappings.csv").resolve()
        mapping = load_network_mapping(mapping_path)
        settings = read_network_settings()
        DEFAULT_NETWORK = settings["default_network"]
        threshold = settings["coverage_threshold"]

        file_path = Path(census["path"])
        headers, rows = load_census_rows(file_path)

        zip_header = resolve_zip_header(headers)
        if not zip_header:
            raise HTTPException(status_code=400, detail="Census file missing ZIP column")

        result = compute_network_assignment(
            rows=rows,
            zip_header=zip_header,
            mapping=mapping,
            default_network=DEFAULT_NETWORK,
            coverage_threshold=threshold,
        )
        group_summary = result["group_summary"]
        primary_network = group_summary["primary_network"]
        coverage_percentage = group_summary["coverage_percentage"]
        fallback_used = group_summary["fallback_used"]
        review_required = group_summary["review_required"]
        census_incomplete = group_summary.get("census_incomplete", False)

        run_id = str(uuid.uuid4())
        created_at = now_iso()
        recommendation = primary_network
        confidence = round(coverage_percentage, 2)
        if review_required:
            if primary_network == "MIXED_NETWORK":
                rationale = "Mixed network coverage. Manual review required."
            elif census_incomplete:
                rationale = "Invalid ZIP values were excluded; census marked incomplete."
            else:
                rationale = "Assignment requires manual review."
        elif fallback_used:
            rationale = "Direct contract coverage below threshold; default network applied."
        else:
            rationale = f"Direct contract coverage meets threshold ({int(threshold*100)}%)."
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO AssignmentRun (id, quote_id, result_json, recommendation, confidence, rationale, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                quote_id,
                json.dumps(result),
                recommendation,
                confidence,
                rationale,
                created_at,
            ),
        )
        cur.execute(
            "UPDATE Quote SET primary_network = ?, manual_network = NULL, updated_at = ? WHERE id = ?",
            (primary_network, now_iso(), quote_id),
        )
        conn.commit()
        recompute_needs_action(conn, quote_id)
        sync_quote_to_hubspot_async(quote_id, create_if_missing=False)

    return AssignmentOut(
        id=run_id,
        quote_id=quote_id,
        result_json=result,
        recommendation=recommendation,
        confidence=confidence,
        rationale=rationale,
        created_at=created_at,
    )


@app.post("/api/quotes/{quote_id}/proposal", response_model=ProposalOut)
def generate_proposal(quote_id: str) -> ProposalOut:
    with get_db() as conn:
        quote = fetch_quote(conn, quote_id)
        quote_dir = UPLOADS_DIR / quote_id
        quote_dir.mkdir(parents=True, exist_ok=True)
        proposal_id = str(uuid.uuid4())
        filename = f"proposal-{proposal_id}.txt"
        path = quote_dir / filename
        path.write_text(
            f"Proposal for {quote['company']}\nGenerated at {now_iso()}\n",
            encoding="utf-8",
        )
        created_at = now_iso()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO Proposal (id, quote_id, filename, path, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (proposal_id, quote_id, filename, str(path), "Ready", created_at),
        )
        cur.execute(
            "UPDATE Quote SET status = ?, updated_at = ? WHERE id = ?",
            ("Proposal", now_iso(), quote_id),
        )
        conn.commit()
        recompute_needs_action(conn, quote_id)

    return ProposalOut(
        id=proposal_id,
        quote_id=quote_id,
        filename=filename,
        path=str(path),
        status="Ready",
        created_at=created_at,
    )


@app.post("/api/quotes/{quote_id}/mark-signed")
def mark_proposal_signed(quote_id: str) -> Dict[str, str]:
    with get_db() as conn:
        fetch_quote(conn, quote_id)
        cur = conn.cursor()
        cur.execute(
            "UPDATE Quote SET status = ?, updated_at = ? WHERE id = ?",
            ("Sold", now_iso(), quote_id),
        )
        cur.execute(
            "UPDATE Proposal SET status = ? WHERE quote_id = ?",
            ("Signed", quote_id),
        )
        conn.commit()
    return {"status": "Sold"}


@app.post("/api/quotes/{quote_id}/convert-to-installation", response_model=InstallationOut)
def convert_to_installation(
    quote_id: str, request: Request, role: Optional[str] = None, email: Optional[str] = None
) -> InstallationOut:
    with get_db() as conn:
        scoped_role, _ = resolve_access_scope(conn, request, role, email)
    if scoped_role == "sponsor":
        raise HTTPException(status_code=403, detail="Only broker/admin can mark sold")
    with get_db() as conn:
        quote = fetch_quote(conn, quote_id)
        installation_id = str(uuid.uuid4())
        now = now_iso()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO Installation (
                id, quote_id, company, broker_org, sponsor_domain, effective_date, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                installation_id,
                quote_id,
                quote["company"],
                quote["broker_org"],
                quote["sponsor_domain"],
                quote["effective_date"],
                "In Progress",
                now,
                now,
            ),
        )
        tasks: List[tuple[Any, ...]] = []
        for title in IMPLEMENTATION_TASK_TITLES:
            tasks.append(
                (
                    str(uuid.uuid4()),
                    installation_id,
                    title,
                    default_installation_task_owner(title),
                    default_installation_task_assigned_user_id(
                        conn,
                        title=title,
                        sponsor_domain=quote["sponsor_domain"],
                    ),
                    None,
                    "Not Started",
                    default_installation_task_url(title),
                )
            )
        cur.executemany(
            """
            INSERT INTO Task (id, installation_id, title, owner, assigned_user_id, due_date, state, task_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tasks,
        )
        cur.execute(
            "UPDATE Quote SET status = ?, updated_at = ? WHERE id = ?",
            ("Sold", now_iso(), quote_id),
        )
        conn.commit()

    return InstallationOut(
        id=installation_id,
        quote_id=quote_id,
        company=quote["company"],
        broker_org=quote["broker_org"],
        sponsor_domain=quote["sponsor_domain"],
        effective_date=quote["effective_date"],
        status="In Progress",
        created_at=now,
        updated_at=now,
    )


@app.get("/api/installations", response_model=List[InstallationOut])
def list_installations(
    request: Request, role: Optional[str] = None, email: Optional[str] = None
) -> List[InstallationOut]:
    with get_db() as conn:
        cur = conn.cursor()
        scoped_role, scoped_email = resolve_access_scope(conn, request, role, email)
        if scoped_role == "sponsor":
            where_clause, params = build_sponsor_installation_filter(
                conn,
                scoped_email,
                include_assigned_user=True,
            )
        else:
            where_clause, params = build_access_filter(
                conn,
                scoped_role,
                scoped_email,
                include_assigned_user=False,
                resource="quote",
            )
        cur.execute(
            f"SELECT * FROM Installation {where_clause} ORDER BY created_at DESC",
            params,
        )
        rows = cur.fetchall()
    return [InstallationOut(**dict(row)) for row in rows]


def require_installation_access(
    conn: sqlite3.Connection,
    installation_id: str,
    request: Request,
    role: Optional[str] = None,
    email: Optional[str] = None,
) -> tuple[sqlite3.Row, Optional[str], Optional[str]]:
    scoped_role, scoped_email = resolve_access_scope(conn, request, role, email)
    cur = conn.cursor()
    cur.execute("SELECT * FROM Installation WHERE id = ?", (installation_id,))
    installation = cur.fetchone()
    if not installation:
        raise HTTPException(status_code=404, detail="Installation not found")
    if scoped_role != "admin":
        if scoped_role == "sponsor":
            where_clause, params = build_sponsor_installation_filter(
                conn,
                scoped_email,
                include_assigned_user=True,
            )
        else:
            where_clause, params = build_access_filter(
                conn,
                scoped_role,
                scoped_email,
                include_assigned_user=False,
                resource="quote",
            )
        if where_clause:
            cur.execute(
                f"SELECT id FROM Installation {where_clause} AND id = ? LIMIT 1",
                [*params, installation_id],
            )
            scoped = cur.fetchone()
            if not scoped:
                raise HTTPException(status_code=404, detail="Installation not found")
        else:
            raise HTTPException(status_code=404, detail="Installation not found")
    return installation, scoped_role, scoped_email


@app.get("/api/installations/{installation_id}")
def get_installation_detail(
    installation_id: str,
    request: Request,
    role: Optional[str] = None,
    email: Optional[str] = None,
) -> Dict[str, Any]:
    with get_db() as conn:
        installation, scoped_role, scoped_email = require_installation_access(
            conn, installation_id, request, role, email
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM Task WHERE installation_id = ? ORDER BY title",
            (installation_id,),
        )
        tasks = [dict(row) for row in cur.fetchall()]
        if scoped_role == "sponsor":
            tasks = [task for task in tasks if task["title"] not in BROKER_ADMIN_ONLY_TASKS]
        cur.execute(
            "SELECT * FROM InstallationDocument WHERE installation_id = ? ORDER BY created_at DESC",
            (installation_id,),
        )
        documents = [dict(row) for row in cur.fetchall()]

    return {
        "installation": dict(installation),
        "tasks": tasks,
        "documents": documents,
    }


@app.delete("/api/installations/{installation_id}")
def delete_installation(installation_id: str, request: Request) -> Dict[str, Any]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cleanup = delete_installation_with_dependencies(conn, installation_id)
        conn.commit()

    for path_value in cleanup["files_to_remove"]:
        remove_upload_file(path_value)
    install_dir = UPLOADS_DIR / f"installation-{installation_id}"
    try:
        if install_dir.exists():
            shutil.rmtree(install_dir, ignore_errors=True)
    except Exception:
        pass

    return {
        "status": "deleted",
        "installation_id": installation_id,
        "quote_id": cleanup["quote_id"],
    }


@app.post(
    "/api/installations/{installation_id}/regress-to-quote",
    response_model=InstallationRegressOut,
)
def regress_installation_to_quote(
    installation_id: str,
    request: Request,
) -> InstallationRegressOut:
    target_quote_status = "Quote Submitted"
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cleanup = delete_installation_with_dependencies(conn, installation_id)
        quote_id = cleanup["quote_id"]
        if not quote_id:
            raise HTTPException(status_code=400, detail="Installation is not linked to a quote")
        fetch_quote(conn, quote_id)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE Quote
            SET status = ?, updated_at = ?
            WHERE id = ?
            """,
            (target_quote_status, now_iso(), quote_id),
        )
        conn.commit()

    for path_value in cleanup["files_to_remove"]:
        remove_upload_file(path_value)
    install_dir = UPLOADS_DIR / f"installation-{installation_id}"
    try:
        if install_dir.exists():
            shutil.rmtree(install_dir, ignore_errors=True)
    except Exception:
        pass

    return InstallationRegressOut(
        status="regressed",
        installation_id=installation_id,
        quote_id=quote_id,
        quote_status=target_quote_status,
    )


@app.post("/api/installations/{installation_id}/tasks/{task_id}/advance", response_model=TaskOut)
def advance_task(
    installation_id: str, task_id: str, request: Request
) -> TaskOut:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM Task WHERE id = ? AND installation_id = ?",
            (task_id, installation_id),
        )
        task = cur.fetchone()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        current_state = normalize_task_state(task["state"]) or "Not Started"
        next_state = current_state
        if current_state == "Not Started":
            next_state = "In Progress"
        elif current_state == "In Progress":
            next_state = "Complete"

        cur.execute(
            "UPDATE Task SET state = ? WHERE id = ?",
            (next_state, task_id),
        )
        cur.execute(
            "UPDATE Installation SET updated_at = ? WHERE id = ?",
            (now_iso(), installation_id),
        )
        conn.commit()

        cur.execute("SELECT * FROM Task WHERE id = ?", (task_id,))
        updated = cur.fetchone()

    return TaskOut(**dict(updated))


@app.post(
    "/api/installations/{installation_id}/tasks/{task_id}/complete-implementation-form",
    response_model=TaskOut,
)
def complete_implementation_forms_task(
    installation_id: str,
    task_id: str,
    request: Request,
    role: Optional[str] = None,
    email: Optional[str] = None,
) -> TaskOut:
    with get_db() as conn:
        require_installation_access(conn, installation_id, request, role, email)
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM Task WHERE id = ? AND installation_id = ?",
            (task_id, installation_id),
        )
        task = cur.fetchone()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        if str(task["title"] or "").strip().lower() != "implementation forms":
            raise HTTPException(
                status_code=400,
                detail="Only the Implementation Forms task can be auto-completed",
            )

        cur.execute(
            "UPDATE Task SET state = ? WHERE id = ?",
            ("Complete", task_id),
        )
        cur.execute(
            "UPDATE Installation SET updated_at = ? WHERE id = ?",
            (now_iso(), installation_id),
        )
        conn.commit()

        cur.execute("SELECT * FROM Task WHERE id = ?", (task_id,))
        updated = cur.fetchone()
        if not updated:
            raise HTTPException(status_code=404, detail="Task not found")
    return TaskOut(**dict(updated))


@app.post(
    "/api/installations/{installation_id}/tasks/{task_id}/launch-stoploss-disclosure",
    response_model=StoplossDisclosureLaunchOut,
)
def launch_stoploss_disclosure_task(
    installation_id: str,
    task_id: str,
    payload: StoplossDisclosureLaunchIn,
    request: Request,
    role: Optional[str] = None,
    email: Optional[str] = None,
) -> StoplossDisclosureLaunchOut:
    selected_url = str(payload.selected_url or "").strip()
    if not selected_url:
        raise HTTPException(status_code=400, detail="selected_url is required")

    with get_db() as conn:
        installation, _, _ = require_installation_access(
            conn, installation_id, request, role, email
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM Task WHERE id = ? AND installation_id = ?",
            (task_id, installation_id),
        )
        task = cur.fetchone()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        if str(task["title"] or "").strip().lower() != "stoploss disclosure":
            raise HTTPException(
                status_code=400,
                detail="Only Stoploss Disclosure can use this launch action",
            )

        options = parse_pandadoc_dropdown_task_options(task["task_url"])
        allowed_map = {option_url: label for label, option_url in options}
        if allowed_map and selected_url not in allowed_map:
            raise HTTPException(
                status_code=400,
                detail="selected_url is not allowed for this task",
            )

        target = parse_pandadoc_app_target(selected_url)
        if not target:
            raise HTTPException(
                status_code=400,
                detail="selected_url must be a PandaDoc template or document URL",
            )

        if target["type"] == "templates":
            open_url = create_stoploss_disclosure_document_from_template(
                conn,
                installation=installation,
                task=task,
                template_id=target["id"],
                option_label=allowed_map.get(selected_url),
            )
            return StoplossDisclosureLaunchOut(
                status="created",
                open_url=open_url,
                created_via_api=True,
            )

        return StoplossDisclosureLaunchOut(
            status="opened",
            open_url=selected_url,
            created_via_api=False,
        )


@app.patch("/api/installations/{installation_id}/tasks/{task_id}", response_model=TaskOut)
def update_task(
    installation_id: str,
    task_id: str,
    payload: TaskUpdateIn,
    request: Request,
) -> TaskOut:
    updates = payload.dict(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No task updates provided")

    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM Task WHERE id = ? AND installation_id = ?",
            (task_id, installation_id),
        )
        task = cur.fetchone()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")

        assignments: List[str] = []
        params: List[Any] = []

        if "state" in updates:
            normalized_state = normalize_task_state(updates.get("state"))
            if not normalized_state:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid state. Use Not Started, In Progress, or Complete.",
                )
            assignments.append("state = ?")
            params.append(normalized_state)

        if "task_url" in updates:
            url = (updates.get("task_url") or "").strip()
            assignments.append("task_url = ?")
            params.append(url or None)

        if "due_date" in updates:
            due_date_raw = (updates.get("due_date") or "").strip()
            if due_date_raw:
                try:
                    datetime.strptime(due_date_raw, "%Y-%m-%d")
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail="Invalid due_date. Use YYYY-MM-DD.",
                    )
                assignments.append("due_date = ?")
                params.append(due_date_raw)
            else:
                assignments.append("due_date = ?")
                params.append(None)

        if "assigned_user_id" in updates:
            assigned_user_id = (updates.get("assigned_user_id") or "").strip()
            if assigned_user_id:
                cur.execute("SELECT id FROM User WHERE id = ?", (assigned_user_id,))
                if not cur.fetchone():
                    raise HTTPException(status_code=400, detail="Assigned user not found")
                assignments.append("assigned_user_id = ?")
                params.append(assigned_user_id)
            else:
                assignments.append("assigned_user_id = ?")
                params.append(None)

        if not assignments:
            raise HTTPException(status_code=400, detail="No valid task fields to update")

        params.append(task_id)
        cur.execute(
            f"UPDATE Task SET {', '.join(assignments)} WHERE id = ?",
            params,
        )
        previous_assigned_user_id = (task["assigned_user_id"] or "").strip() or None
        updated_assigned_user_id = previous_assigned_user_id
        if "assigned_user_id" in updates:
            updated_assigned_user_id = (updates.get("assigned_user_id") or "").strip() or None
        if updated_assigned_user_id and updated_assigned_user_id != previous_assigned_user_id:
            cur.execute("SELECT company FROM Installation WHERE id = ?", (installation_id,))
            installation = cur.fetchone()
            installation_name = (
                str(installation["company"] or "Implementation").strip()
                if installation
                else "Implementation"
            )
            task_title = str(task["title"] or "Task").strip() or "Task"
            create_notification(
                conn,
                updated_assigned_user_id,
                kind="task_assigned",
                title="Task assigned",
                body=f"{task_title} for {installation_name} was assigned to you.",
                entity_type="installation",
                entity_id=installation_id,
            )
        cur.execute(
            "UPDATE Installation SET updated_at = ? WHERE id = ?",
            (now_iso(), installation_id),
        )
        conn.commit()
        cur.execute("SELECT * FROM Task WHERE id = ?", (task_id,))
        updated = cur.fetchone()

    return TaskOut(**dict(updated))


@app.delete("/api/tasks/{task_id}")
def delete_task(task_id: str, request: Request) -> Dict[str, str]:
    with get_db() as conn:
        require_session_role(conn, request, {"admin"})
        cur = conn.cursor()
        cur.execute("SELECT * FROM Task WHERE id = ?", (task_id,))
        task = cur.fetchone()
        if not task:
            raise HTTPException(status_code=404, detail="Task not found")
        cur.execute("DELETE FROM Task WHERE id = ?", (task_id,))
        cur.execute(
            "UPDATE Installation SET updated_at = ? WHERE id = ?",
            (now_iso(), task["installation_id"]),
        )
        conn.commit()
    return {"status": "deleted"}


@app.post("/api/installations/{installation_id}/documents", response_model=InstallationDocumentOut)
def upload_installation_document(
    installation_id: str,
    file: UploadFile = File(...),
) -> InstallationDocumentOut:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM Installation WHERE id = ?", (installation_id,))
        installation = cur.fetchone()
        if not installation:
            raise HTTPException(status_code=404, detail="Installation not found")

    doc_id = str(uuid.uuid4())
    install_dir = UPLOADS_DIR / f"installation-{installation_id}"
    install_dir.mkdir(parents=True, exist_ok=True)
    safe_name = file.filename or f"document-{doc_id}"
    target_path = install_dir / f"{doc_id}-{safe_name}"
    with target_path.open("wb") as f:
        f.write(file.file.read())
    created_at = now_iso()
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO InstallationDocument (id, installation_id, filename, path, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (doc_id, installation_id, safe_name, str(target_path), created_at),
        )
        conn.commit()

    return InstallationDocumentOut(
        id=doc_id,
        installation_id=installation_id,
        filename=safe_name,
        path=str(target_path),
        created_at=created_at,
    )


@app.delete("/api/installations/{installation_id}/documents/{document_id}")
def delete_installation_document(
    installation_id: str, document_id: str
) -> Dict[str, str]:
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM Installation WHERE id = ?",
            (installation_id,),
        )
        installation = cur.fetchone()
        if not installation:
            raise HTTPException(status_code=404, detail="Installation not found")
        cur.execute(
            "SELECT * FROM InstallationDocument WHERE id = ? AND installation_id = ?",
            (document_id, installation_id),
        )
        doc = cur.fetchone()
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")
        try:
            Path(doc["path"]).unlink(missing_ok=True)
        except Exception:
            pass
        cur.execute("DELETE FROM InstallationDocument WHERE id = ?", (document_id,))
        conn.commit()
    return {"status": "deleted"}


app.mount("/uploads", StaticFiles(directory=str(UPLOADS_DIR)), name="uploads")


@app.get("/api/uploads/{quote_id}/{filename}")
def get_upload_file(quote_id: str, filename: str) -> FileResponse:
    file_path = UPLOADS_DIR / quote_id / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=str(file_path), filename=filename)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
