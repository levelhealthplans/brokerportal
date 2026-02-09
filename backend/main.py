from __future__ import annotations

import csv
import hashlib
import json
import os
import secrets
import shutil
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta
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
HUBSPOT_SETTINGS_PATH = (BASE_DIR / "data" / "hubspot_settings.json").resolve()

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

BROKER_ADMIN_ONLY_TASKS = {"Vendors Notified", "Ventegra vZip"}
TASK_STATE_CANONICAL = {
    "not started": "Not Started",
    "in progress": "In Progress",
    "complete": "Complete",
    "done": "Complete",
}
ALLOWED_USER_ROLES = {"admin", "broker", "sponsor"}
PASSWORD_MIN_LENGTH = 8
HUBSPOT_API_BASE = "https://api.hubapi.com"
HUBSPOT_OAUTH_AUTHORIZE_URL = "https://app.hubspot.com/oauth/authorize"
HUBSPOT_OAUTH_TOKEN_URL = "https://api.hubapi.com/oauth/v1/token"
HUBSPOT_OAUTH_STATE_MINUTES = 15
HUBSPOT_OAUTH_DEFAULT_SCOPES = (
    "oauth tickets "
    "crm.objects.contacts.read crm.objects.contacts.write "
    "crm.objects.companies.read crm.objects.companies.write "
    "crm.objects.deals.read crm.objects.deals.write "
    "crm.schemas.companies.read crm.schemas.companies.write "
    "crm.schemas.contacts.read crm.schemas.contacts.write "
    "crm.schemas.deals.read crm.schemas.deals.write"
)

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


def build_access_filter(
    conn: sqlite3.Connection, role: Optional[str], email: Optional[str]
) -> tuple[str, List[Any]]:
    if not role or role == "admin":
        return "", []
    if role == "broker":
        normalized_email = (email or "").strip().lower()
        domain = email_domain(normalized_email)
        org = fetch_org_by_domain(conn, "broker", domain)
        broker_org = org["name"] if org else broker_org_from_email(normalized_email)

        user_id = None
        if normalized_email:
            cur = conn.cursor()
            cur.execute("SELECT id, organization FROM User WHERE email = ?", (normalized_email,))
            user = cur.fetchone()
            if user:
                user_id = (user["id"] or "").strip() or None
                if not broker_org:
                    broker_org = (user["organization"] or "").strip() or None

        clauses: List[str] = []
        params: List[Any] = []
        if broker_org:
            clauses.append("broker_org = ?")
            params.append(broker_org)
        if user_id:
            clauses.append("assigned_user_id = ?")
            params.append(user_id)
        if not clauses:
            return "WHERE 1 = 0", []
        return f"WHERE ({' OR '.join(clauses)})", params
    if role == "sponsor":
        domain = email_domain((email or "").strip().lower())
        if not domain:
            return "WHERE 1 = 0", []
        return "WHERE sponsor_domain = ?", [domain]
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

        conn.commit()

        cur.execute("SELECT COUNT(*) as cnt FROM Quote")
        if cur.fetchone()["cnt"] == 0:
            seed_data(conn)

        cur.execute("SELECT COUNT(*) as cnt FROM Organization")
        if cur.fetchone()["cnt"] == 0:
            seed_organizations(conn)
        ensure_default_admin_user(conn)


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


class TaskListOut(TaskOut):
    installation_company: Optional[str] = None


class InstallationDocumentOut(BaseModel):
    id: str
    installation_id: str
    filename: str
    path: str
    created_at: str


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


class AuthLoginIn(BaseModel):
    email: str
    password: str


class AuthVerifyOut(BaseModel):
    email: str
    role: str
    first_name: str
    last_name: str
    organization: str


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


def auth_user_payload(row: sqlite3.Row) -> AuthVerifyOut:
    return AuthVerifyOut(
        email=row["email"],
        role=row["role"],
        first_name=row["first_name"],
        last_name=row["last_name"],
        organization=row["organization"],
    )


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


def send_resend_magic_link(to_email: str, link: str) -> bool:
    api_key = os.getenv("RESEND_API_KEY", "").strip()
    from_email = os.getenv("RESEND_FROM_EMAIL", "").strip()
    if not api_key or not from_email:
        return False
    payload = {
        "from": from_email,
        "to": [to_email],
        "subject": "Your Level Health sign-in link",
        "html": f"<p>Use this secure sign-in link:</p><p><a href=\"{link}\">{link}</a></p><p>This link expires in {MAGIC_LINK_DURATION_MINUTES} minutes.</p>",
    }
    req = urlrequest.Request(
        "https://api.resend.com/emails",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=10) as resp:
            if resp.status >= 300:
                raise HTTPException(status_code=502, detail="Failed to send magic link email.")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=502, detail="Failed to send magic link email.")
    return True


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


def normalize_zip(value: str) -> Optional[str]:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) != 5:
        return None
    return digits


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
        normalized = normalize_zip(raw_zip)
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


def delete_quote_with_dependencies(conn: sqlite3.Connection, quote_id: str) -> Dict[str, Any]:
    cur = conn.cursor()
    files_to_remove: List[str] = []
    installation_ids: List[str] = []
    deleted_task_count = 0

    cur.execute("SELECT path FROM Upload WHERE quote_id = ?", (quote_id,))
    files_to_remove.extend(
        [row["path"] for row in cur.fetchall() if row["path"]]
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


def default_hubspot_settings() -> Dict[str, Any]:
    return {
        "enabled": False,
        "portal_id": (os.getenv("HUBSPOT_PORTAL_ID", "7106327") or "7106327").strip(),
        "pipeline_id": (os.getenv("HUBSPOT_PIPELINE_ID", "98238573") or "98238573").strip(),
        "default_stage_id": (os.getenv("HUBSPOT_DEFAULT_STAGE_ID", "") or "").strip(),
        "sync_quote_to_hubspot": True,
        "sync_hubspot_to_quote": True,
        "ticket_subject_template": "Quote {{company}} ({{quote_id}})",
        "ticket_content_template": "Company: {{company}}\nQuote ID: {{quote_id}}\nStatus: {{status}}\nEffective Date: {{effective_date}}\nBroker Org: {{broker_org}}",
        "property_mappings": {
            "id": "level_health_quote_id",
            "company": "level_health_company",
            "status": "level_health_quote_status",
            "effective_date": "level_health_effective_date",
            "broker_org": "level_health_broker_org",
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
        "property_mappings": normalize_mapping_dict(settings.get("property_mappings")),
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


def read_hubspot_settings(*, include_token: bool = False) -> Dict[str, Any]:
    defaults = default_hubspot_settings()
    raw: Dict[str, Any] = {}
    if HUBSPOT_SETTINGS_PATH.exists():
        try:
            loaded = json.loads(HUBSPOT_SETTINGS_PATH.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                raw = loaded
        except Exception:
            raw = {}

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
        "property_mappings": normalize_mapping_dict(
            raw.get("property_mappings") or defaults["property_mappings"]
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
        "property_mappings": normalize_mapping_dict(
            payload.property_mappings
            if payload.property_mappings is not None
            else current["property_mappings"]
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
        try:
            parsed = json.loads(detail)
            parsed_message = str(parsed.get("message") or parsed.get("detail") or detail)
        except Exception:
            parsed_message = detail or str(exc)
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


def build_hubspot_ticket_properties(quote: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, str]:
    status_key = str(quote.get("status") or "").strip()
    mapped_stage = settings["quote_status_to_stage"].get(status_key)
    stage_id = (mapped_stage or settings.get("default_stage_id") or "").strip()

    properties: Dict[str, str] = {
        "subject": render_hubspot_template(settings["ticket_subject_template"], quote),
        "content": render_hubspot_template(settings["ticket_content_template"], quote),
    }
    if settings.get("pipeline_id"):
        properties["hs_pipeline"] = settings["pipeline_id"]
    if stage_id:
        properties["hs_pipeline_stage"] = stage_id

    property_mappings = settings.get("property_mappings") or {}
    for local_key, hubspot_property in property_mappings.items():
        if not hubspot_property:
            continue
        if local_key in quote:
            properties[hubspot_property] = to_hubspot_property_value(quote.get(local_key))
    return {key: value for key, value in properties.items() if value is not None}


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
        try:
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
    quote = dict(quote_row)
    properties = build_hubspot_ticket_properties(quote, settings)
    ticket_id = (quote.get("hubspot_ticket_id") or "").strip()
    if not ticket_id and not create_if_missing:
        return

    try:
        if ticket_id:
            hubspot_api_request(
                token,
                "PATCH",
                f"/crm/v3/objects/tickets/{ticket_id}",
                body={"properties": properties},
            )
            association_warning = sync_hubspot_ticket_associations(conn, token, quote, ticket_id)
            ticket_url = build_hubspot_ticket_url(settings["portal_id"], ticket_id)
            update_quote_hubspot_sync_state(
                conn,
                quote_id,
                ticket_id=ticket_id,
                ticket_url=ticket_url,
                sync_error=association_warning,
            )
            return

        if not settings.get("pipeline_id") or not properties.get("hs_pipeline_stage"):
            update_quote_hubspot_sync_state(
                conn,
                quote_id,
                sync_error="HubSpot pipeline/stage is not configured",
            )
            return

        created = hubspot_api_request(
            token,
            "POST",
            "/crm/v3/objects/tickets",
            body={"properties": properties},
        )
        new_ticket_id = str(created.get("id") or "").strip()
        association_warning = None
        if new_ticket_id:
            association_warning = sync_hubspot_ticket_associations(conn, token, quote, new_ticket_id)
        ticket_url = build_hubspot_ticket_url(settings["portal_id"], new_ticket_id)
        update_quote_hubspot_sync_state(
            conn,
            quote_id,
            ticket_id=new_ticket_id or None,
            ticket_url=ticket_url,
            sync_error=association_warning,
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
        if name:
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

    ticket = hubspot_api_request(
        token,
        "GET",
        f"/crm/v3/objects/tickets/{ticket_id}",
        query={"properties": ["subject", "hs_pipeline", "hs_pipeline_stage"]},
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
            for row in existing:
                try:
                    Path(row["path"]).unlink(missing_ok=True)
                except Exception:
                    pass
            cur.execute(
                "DELETE FROM Upload WHERE quote_id = ? AND type = 'census'",
                (quote_id,),
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


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/api/auth/request-link")
def request_magic_link(payload: AuthRequestIn) -> Dict[str, str]:
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

    link = f"{FRONTEND_BASE_URL}/auth/verify?token={token}"
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

    scopes = (os.getenv("HUBSPOT_OAUTH_SCOPES", HUBSPOT_OAUTH_DEFAULT_SCOPES) or "").strip()
    if not scopes:
        scopes = HUBSPOT_OAUTH_DEFAULT_SCOPES

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


@app.get("/api/quotes", response_model=List[QuoteListOut])
def list_quotes(
    request: Request, role: Optional[str] = None, email: Optional[str] = None
) -> List[QuoteListOut]:
    with get_db() as conn:
        cur = conn.cursor()
        scoped_role, scoped_email = resolve_access_scope(conn, request, role, email)
        where_clause, params = build_access_filter(conn, scoped_role, scoped_email)
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
                current_eligible, current_insurance_type, employees_eligible,
                expected_enrollees, broker_fee_pepm, include_specialty, notes,
                high_cost_info, broker_first_name, broker_last_name, broker_email,
                broker_phone, agent_of_record, broker_org, sponsor_domain, assigned_user_id, manual_network, proposal_url, status,
                version, needs_action, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                session_user["id"],
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
            where_clause, params = build_access_filter(conn, scoped_role, scoped_email)
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
        where_clause, params = build_access_filter(conn, scoped_role, scoped_email)
        cur.execute(
            f"""
            SELECT
                t.*,
                i.company as installation_company
            FROM Task t
            JOIN Installation i ON i.id = t.installation_id
            {where_clause}
            ORDER BY i.created_at DESC, t.title ASC
            """,
            params,
        )
        rows = cur.fetchall()
    return [TaskListOut(**dict(row)) for row in rows]


@app.patch("/api/quotes/{quote_id}", response_model=QuoteOut)
def update_quote(quote_id: str, payload: QuoteUpdate) -> QuoteOut:
    with get_db() as conn:
        quote = fetch_quote(conn, quote_id)
        data = dict(quote)
        updates = payload.dict(exclude_unset=True)
        if "broker_email" in updates and "broker_org" not in updates:
            domain = email_domain(updates.get("broker_email"))
            org = fetch_org_by_domain(conn, "broker", domain)
            inferred_org = org["name"] if org else broker_org_from_email(updates.get("broker_email"))
            if inferred_org:
                updates["broker_org"] = inferred_org
        if "employer_domain" in updates and "sponsor_domain" not in updates:
            if updates.get("employer_domain"):
                updates["sponsor_domain"] = updates["employer_domain"].lower()
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
    return save_upload(quote_id, type, file)


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
        conn.commit()
        recompute_needs_action(conn, quote_id)
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


@app.post("/api/quotes/{quote_id}/standardize", response_model=StandardizationOut)
def run_standardization(
    quote_id: str, payload: Optional[StandardizationIn] = None
) -> StandardizationOut:
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
            "dob": ["dob", "date of birth", "birthdate"],
            "zip": ["zip", "zipcode", "zip code", "postal code"],
            "gender": ["gender", "sex"],
            "relationship": ["relationship", "rel"],
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
        ) -> None:
            entry: Dict[str, Any] = {"row": row_num, "field": field, "issue": issue}
            if value is not None:
                entry["value"] = value
            if mapped_value is not None:
                entry["mapped_value"] = mapped_value
            issues.append(entry)

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

        standardized_rows: List[Dict[str, str]] = []
        for idx, row in enumerate(rows, start=2):
            # Skip completely empty rows (common in Excel exports)
            if all((str(value).strip() == "" for value in row.values())):
                continue
            standardized_row: Dict[str, str] = {}
            total_rows += 1
            for header in detected_headers:
                if len(sample_data[header]) >= 3:
                    continue
                raw_value = (row.get(header) or "").strip()
                if raw_value:
                    sample_data[header].append(raw_value)
            for key, header in header_lookup.items():
                if not header:
                    continue
                value = (row.get(header) or "").strip()
                if value == "":
                    add_issue(idx, key, "Missing value")
                    issue_row_set.add(idx)
                    standardized_row[key] = ""
                    continue

                if key == "gender":
                    mapped = gender_map.get(value.lower(), value).upper()
                    standardized_row[key] = mapped
                    if mapped not in allowed_gender:
                        add_issue(
                            idx,
                            key,
                            "Invalid gender. Allowed: M or F",
                            value,
                            mapped,
                        )
                        issue_row_set.add(idx)
                elif key == "relationship":
                    mapped = relationship_map.get(value.lower(), value).upper()
                    standardized_row[key] = mapped
                    if mapped not in allowed_relationship:
                        add_issue(
                            idx,
                            key,
                            "Invalid relationship. Allowed: E, S, or C",
                            value,
                            mapped,
                        )
                        issue_row_set.add(idx)
                elif key == "enrollment_tier":
                    mapped = tier_map.get(value.lower(), value).upper()
                    standardized_row[key] = mapped
                    if mapped not in allowed_tier:
                        add_issue(
                            idx,
                            key,
                            "Invalid enrollment tier. Allowed: EE, ES, EC, EF, or W",
                            value,
                            mapped,
                        )
                        issue_row_set.add(idx)
                elif key == "zip":
                    digits = "".join(ch for ch in value if ch.isdigit())
                    standardized_row[key] = digits
                    if len(digits) != 5:
                        add_issue(idx, key, "Invalid ZIP code", value)
                        issue_row_set.add(idx)
                elif key == "dob":
                    parsed = False
                    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y/%m/%d"):
                        try:
                            datetime.strptime(value, fmt)
                            parsed = True
                            break
                        except ValueError:
                            continue
                    standardized_row[key] = value
                    if not parsed:
                        add_issue(idx, key, "Invalid date format", value)
                        issue_row_set.add(idx)
                else:
                    standardized_row[key] = value

            if standardized_row:
                standardized_rows.append(standardized_row)
                if len(sample_rows) < 20:
                    sample_rows.append(
                        {
                            "row": idx,
                            **{field: standardized_row.get(field, "") for field in required_fields.keys()},
                        }
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
            "UPDATE Quote SET manual_network = NULL, updated_at = ? WHERE id = ?",
            (now_iso(), quote_id),
        )
        conn.commit()
        recompute_needs_action(conn, quote_id)

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
        tasks = [
            (str(uuid.uuid4()), installation_id, title, "Level Health", None, "Not Started", None)
            for title in IMPLEMENTATION_TASK_TITLES
        ]
        cur.executemany(
            """
            INSERT INTO Task (id, installation_id, title, owner, due_date, state, task_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
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
        where_clause, params = build_access_filter(conn, scoped_role, scoped_email)
        cur.execute(
            f"SELECT * FROM Installation {where_clause} ORDER BY created_at DESC",
            params,
        )
        rows = cur.fetchall()
    return [InstallationOut(**dict(row)) for row in rows]


@app.get("/api/installations/{installation_id}")
def get_installation_detail(
    installation_id: str,
    request: Request,
    role: Optional[str] = None,
    email: Optional[str] = None,
) -> Dict[str, Any]:
    with get_db() as conn:
        scoped_role, scoped_email = resolve_access_scope(conn, request, role, email)
        cur = conn.cursor()
        cur.execute("SELECT * FROM Installation WHERE id = ?", (installation_id,))
        installation = cur.fetchone()
        if not installation:
            raise HTTPException(status_code=404, detail="Installation not found")
        if scoped_role != "admin":
            where_clause, params = build_access_filter(conn, scoped_role, scoped_email)
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
