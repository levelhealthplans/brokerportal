import asyncio
import json
import shutil
import tempfile
import unittest
import uuid
from datetime import datetime
from pathlib import Path
import sys
from unittest.mock import patch

from fastapi import HTTPException
from starlette.requests import Request

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class HubspotCardDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.original_db_path = main.DB_PATH
        cls.original_uploads_dir = main.UPLOADS_DIR
        cls.tempdir = tempfile.TemporaryDirectory()
        cls.temp_root = Path(cls.tempdir.name)
        cls.test_db_path = cls.temp_root / "test.db"
        cls.test_uploads_dir = cls.temp_root / "uploads"

    @classmethod
    def tearDownClass(cls) -> None:
        main.DB_PATH = cls.original_db_path
        main.UPLOADS_DIR = cls.original_uploads_dir
        cls.tempdir.cleanup()

    def setUp(self) -> None:
        if self.test_db_path.exists():
            self.test_db_path.unlink()
        shutil.rmtree(self.test_uploads_dir, ignore_errors=True)
        self.test_uploads_dir.mkdir(parents=True, exist_ok=True)
        main.DB_PATH = self.test_db_path
        main.UPLOADS_DIR = self.test_uploads_dir
        main.init_db()
        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM Quote")
            quote_ids = [str(row["id"] or "").strip() for row in cur.fetchall() if str(row["id"] or "").strip()]
            for quote_id in quote_ids:
                main.delete_quote_with_dependencies(conn, quote_id)
            conn.commit()

    def _create_quote(self, company: str) -> main.QuoteOut:
        payload = main.QuoteCreate(
            company=company,
            employer_street="1 Main St",
            employer_city="St. Louis",
            state="MO",
            employer_zip="63101",
            employer_domain=f"{company.lower().replace(' ', '-')}.example.com",
            quote_deadline="2026-03-01",
            employer_sic="1234",
            effective_date="2026-04-01",
            current_enrolled=10,
            current_eligible=12,
            current_insurance_type="Level Funded",
            employees_eligible=12,
            expected_enrollees=10,
            broker_fee_pepm=35.0,
            include_specialty=False,
            notes="",
            high_cost_info="",
            status="Draft",
        )
        session_user = {
            "id": "session-user-1",
            "email": "jake@legacybrokerskc.com",
            "role": "broker",
            "first_name": "Jake",
            "last_name": "Page",
            "phone": "555-1111",
            "organization": "Legacy Brokers KC",
        }
        with patch.object(main, "require_session_user", return_value=session_user), patch.object(
            main, "sync_quote_to_hubspot_async", return_value=None
        ):
            return main.create_quote(payload, request=object())

    def _signed_request(self, payload: dict, *, secret: str, signature: str | None = None) -> Request:
        body_bytes = json.dumps(payload).encode("utf-8")
        timestamp = str(int(datetime.utcnow().timestamp() * 1000))
        url = "https://portal.example.com/api/integrations/hubspot/card-data"
        normalized_url = main.normalize_hubspot_signature_uri(url)
        next_signature = signature or main.build_hubspot_signature_v3(
            secret,
            method="POST",
            uri=normalized_url,
            body=body_bytes.decode("utf-8"),
            timestamp=timestamp,
        )
        headers = [
            (b"host", b"portal.example.com"),
            (b"x-hubspot-request-timestamp", timestamp.encode("utf-8")),
            (b"x-hubspot-signature-v3", next_signature.encode("utf-8")),
            (b"content-type", b"application/json"),
        ]
        scope = {
            "type": "http",
            "method": "POST",
            "scheme": "https",
            "path": "/api/integrations/hubspot/card-data",
            "raw_path": b"/api/integrations/hubspot/card-data",
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 44321),
            "server": ("portal.example.com", 443),
            "root_path": "",
        }
        sent = False

        async def receive() -> dict:
            nonlocal sent
            if sent:
                return {"type": "http.request", "body": b"", "more_body": False}
            sent = True
            return {"type": "http.request", "body": body_bytes, "more_body": False}

        return Request(scope, receive)

    def test_get_hubspot_card_data_resolves_quote_from_property(self) -> None:
        quote = self._create_quote("Card Property Group")
        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE Quote SET hubspot_ticket_id = ?, hubspot_ticket_url = ? WHERE id = ?",
                ("ticket-001", "https://app.hubspot.com/contacts/7106327/record/0-5/ticket-001", quote.id),
            )
            cur.execute(
                """
                INSERT INTO AssignmentRun (
                    id, quote_id, result_json, recommendation, confidence, rationale, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    quote.id,
                    json.dumps({"coverage_percentage": 0.93, "fallback_used": False}),
                    "Mercy_MO",
                    0.88,
                    "Strong direct coverage",
                    main.now_iso(),
                ),
            )
            conn.commit()

        payload = {"properties": {"level_health_quote_id": quote.id}}
        request = self._signed_request(payload, secret="test-card-secret")

        with patch.dict(main.os.environ, {"HUBSPOT_APP_CLIENT_SECRET": "test-card-secret"}, clear=False):
            response = asyncio.run(main.get_hubspot_card_data(request))

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["resolved_by"], "quote_id")
        self.assertEqual(response["quote"]["id"], quote.id)
        self.assertEqual(response["quote"]["hubspot_ticket_id"], "ticket-001")
        self.assertEqual(response["assignment"]["coverage_percentage"], 0.93)
        self.assertFalse(response["assignment"]["fallback_used"])
        self.assertEqual(response["assignment"]["recommendation"], "Mercy_MO")

    def test_get_hubspot_card_data_resolves_quote_from_ticket_id(self) -> None:
        quote = self._create_quote("Card Ticket Group")
        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE Quote SET hubspot_ticket_id = ? WHERE id = ?",
                ("ticket-lookup-7", quote.id),
            )
            conn.commit()

        payload = {"objectId": "ticket-lookup-7"}
        request = self._signed_request(payload, secret="test-card-secret")

        with patch.dict(main.os.environ, {"HUBSPOT_APP_CLIENT_SECRET": "test-card-secret"}, clear=False):
            response = asyncio.run(main.get_hubspot_card_data(request))

        self.assertEqual(response["status"], "ok")
        self.assertEqual(response["resolved_by"], "hubspot_ticket_id")
        self.assertEqual(response["quote"]["id"], quote.id)
        self.assertEqual(response["request"]["hubspot_ticket_id"], "ticket-lookup-7")

    def test_get_hubspot_card_data_rejects_invalid_signature(self) -> None:
        payload = {"objectId": "ticket-lookup-7"}
        request = self._signed_request(
            payload,
            secret="test-card-secret",
            signature="not-a-valid-signature",
        )
        with patch.dict(main.os.environ, {"HUBSPOT_APP_CLIENT_SECRET": "test-card-secret"}, clear=False):
            with self.assertRaises(HTTPException) as raised:
                asyncio.run(main.get_hubspot_card_data(request))
        self.assertEqual(raised.exception.status_code, 401)


if __name__ == "__main__":
    unittest.main()
