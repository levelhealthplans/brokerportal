import shutil
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class HubspotBulkResyncTests(unittest.TestCase):
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

    def test_bulk_resync_returns_grouped_mismatch_report(self) -> None:
        quote_ok = self._create_quote("Group A")
        quote_err_one = self._create_quote("Group B")
        quote_err_two = self._create_quote("Group C")

        mismatch_message = "Dropped invalid ticket properties: level_health_state"

        def fake_sync(conn: object, quote_id: str, *, create_if_missing: bool) -> None:
            self.assertTrue(create_if_missing)
            if quote_id == quote_ok.id:
                main.update_quote_hubspot_sync_state(
                    conn,
                    quote_id,
                    ticket_id="1001",
                    ticket_url="https://app.hubspot.com/contacts/7106327/record/0-5/1001",
                    sync_error=None,
                )
                return
            main.update_quote_hubspot_sync_state(conn, quote_id, sync_error=mismatch_message)

        with patch.object(main, "require_session_role", return_value=None), patch.object(
            main,
            "read_hubspot_settings",
            return_value={"enabled": True, "sync_quote_to_hubspot": True},
        ), patch.object(main, "sync_quote_to_hubspot", side_effect=fake_sync) as sync_mock:
            report = main.resync_all_quotes_to_hubspot(request=object())

        self.assertEqual(report.status, "ok")
        self.assertEqual(report.total_quotes, 3)
        self.assertEqual(report.attempted_quotes, 3)
        self.assertEqual(sync_mock.call_count, 3)
        self.assertEqual(report.clean_quotes, 1)
        self.assertEqual(report.mismatch_quotes, 2)
        self.assertEqual(len(report.buckets), 1)
        self.assertEqual(report.buckets[0].message, mismatch_message)
        self.assertEqual(report.buckets[0].count, 2)
        self.assertEqual(set(report.buckets[0].quote_ids), {quote_err_one.id, quote_err_two.id})
        self.assertEqual(len(report.mismatches), 2)
        self.assertEqual(set(row.quote_id for row in report.mismatches), {quote_err_one.id, quote_err_two.id})

    def test_bulk_resync_reports_blocked_when_outbound_sync_disabled(self) -> None:
        quote = self._create_quote("Group Disabled")
        with main.get_db() as conn:
            main.update_quote_hubspot_sync_state(
                conn,
                quote.id,
                sync_error="HubSpot pipeline/stage is not configured",
            )

        with patch.object(main, "require_session_role", return_value=None), patch.object(
            main,
            "read_hubspot_settings",
            return_value={"enabled": False, "sync_quote_to_hubspot": False},
        ), patch.object(main, "sync_quote_to_hubspot", return_value=None) as sync_mock:
            report = main.resync_all_quotes_to_hubspot(request=object())

        sync_mock.assert_not_called()
        self.assertEqual(report.status, "blocked")
        self.assertFalse(report.integration_enabled)
        self.assertFalse(report.quote_to_hubspot_sync_enabled)
        self.assertEqual(report.attempted_quotes, 0)
        self.assertEqual(report.total_quotes, 1)
        self.assertEqual(report.clean_quotes, 0)
        self.assertEqual(report.mismatch_quotes, 1)


if __name__ == "__main__":
    unittest.main()
