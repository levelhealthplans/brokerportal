import shutil
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

from fastapi import HTTPException

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class InstallationRegressionTests(unittest.TestCase):
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

    def _create_quote(self) -> main.QuoteOut:
        payload = main.QuoteCreate(
            company="Regression Group",
            employer_street="1 Main St",
            employer_city="St. Louis",
            state="MO",
            employer_zip="63101",
            employer_domain="regression.example.com",
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

    def _create_installation(self, quote_id: str) -> main.InstallationOut:
        with patch.object(main, "resolve_access_scope", return_value=("admin", "admin@example.com")):
            return main.convert_to_installation(quote_id, request=object())

    def test_broker_fields_require_admin_on_quote_update(self) -> None:
        quote = self._create_quote()

        with patch.object(main, "get_session_user", return_value={"role": "broker"}):
            with self.assertRaises(HTTPException) as exc:
                main.update_quote(
                    quote.id,
                    main.QuoteUpdate(broker_phone="555-2222"),
                    request=object(),
                )
        self.assertEqual(exc.exception.status_code, 403)
        self.assertIn("Only admin can edit broker information", str(exc.exception.detail))

        with patch.object(main, "get_session_user", return_value={"role": "admin"}), patch.object(
            main, "sync_quote_to_hubspot_async", return_value=None
        ):
            updated = main.update_quote(
                quote.id,
                main.QuoteUpdate(broker_phone="555-3333"),
                request=object(),
            )
        self.assertEqual(updated.broker_phone, "555-3333")

    def test_quote_broker_org_and_sponsor_domain_propagate_to_installation(self) -> None:
        quote = self._create_quote()
        installation = self._create_installation(quote.id)

        with patch.object(main, "get_session_user", return_value={"role": "admin"}), patch.object(
            main, "sync_quote_to_hubspot_async", return_value=None
        ):
            updated = main.update_quote(
                quote.id,
                main.QuoteUpdate(
                    broker_org="TWS",
                    sponsor_domain="twsbenefits.com",
                ),
                request=object(),
            )

        self.assertEqual(updated.broker_org, "TWS")
        self.assertEqual(updated.sponsor_domain, "twsbenefits.com")

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT broker_org, sponsor_domain FROM Installation WHERE id = ?", (installation.id,))
            install_row = cur.fetchone()
            self.assertIsNotNone(install_row)
            self.assertEqual(install_row["broker_org"], "TWS")
            self.assertEqual(install_row["sponsor_domain"], "twsbenefits.com")

    def test_convert_to_installation_sets_implementation_forms_url_from_env(self) -> None:
        quote = self._create_quote()
        form_url = "https://share.hsforms.com/1-example-form"
        with patch.dict(main.os.environ, {"HUBSPOT_IMPLEMENTATION_FORM_URL": form_url}, clear=False):
            installation = self._create_installation(quote.id)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT task_url
                FROM Task
                WHERE installation_id = ? AND title = ?
                LIMIT 1
                """,
                (installation.id, "Implementation Forms"),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["task_url"], form_url)

    def test_convert_to_installation_sets_implementation_forms_popup_url(self) -> None:
        quote = self._create_quote()
        with patch.dict(
            main.os.environ,
            {
                "HUBSPOT_IMPLEMENTATION_FORM_URL": "",
                "HUBSPOT_IMPLEMENTATION_FORM_PORTAL_ID": "7106327",
                "HUBSPOT_IMPLEMENTATION_FORM_ID": "f215c8d6-451d-4b7b-826f-fdab43b80369",
                "HUBSPOT_IMPLEMENTATION_FORM_REGION": "na1",
            },
            clear=False,
        ):
            installation = self._create_installation(quote.id)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT task_url
                FROM Task
                WHERE installation_id = ? AND title = ?
                LIMIT 1
                """,
                (installation.id, "Implementation Forms"),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(
                row["task_url"],
                "hubspot-form://popup?portal_id=7106327&form_id=f215c8d6-451d-4b7b-826f-fdab43b80369&region=na1",
            )

    def test_backfill_installation_orgs_syncs_stale_values(self) -> None:
        quote = self._create_quote()
        installation = self._create_installation(quote.id)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE Installation
                SET broker_org = ?, sponsor_domain = ?
                WHERE id = ?
                """,
                ("Legacy Brokers KC", "legacybrokerskc.com", installation.id),
            )
            cur.execute(
                """
                UPDATE Quote
                SET broker_org = ?, sponsor_domain = ?, updated_at = ?
                WHERE id = ?
                """,
                ("TWS", "twsbenefits.com", main.now_iso(), quote.id),
            )
            conn.commit()
            cur.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM Installation i
                JOIN Quote q ON q.id = i.quote_id
                """
            )
            scanned_count = int(cur.fetchone()["cnt"] or 0)

        with patch.object(main, "require_session_role", return_value=None):
            result = main.backfill_installation_orgs(request=object())

        self.assertEqual(result.status, "backfilled")
        self.assertEqual(result.scanned_installation_count, scanned_count)
        self.assertEqual(result.updated_installation_count, 1)
        self.assertEqual(result.updated_broker_org_count, 1)
        self.assertEqual(result.updated_sponsor_domain_count, 1)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT broker_org, sponsor_domain FROM Installation WHERE id = ?", (installation.id,))
            install_row = cur.fetchone()
            self.assertIsNotNone(install_row)
            self.assertEqual(install_row["broker_org"], "TWS")
            self.assertEqual(install_row["sponsor_domain"], "twsbenefits.com")

    def test_regress_installation_to_quote_removes_installation(self) -> None:
        quote = self._create_quote()
        installation = self._create_installation(quote.id)

        with patch.object(main, "require_session_role", return_value=None):
            result = main.regress_installation_to_quote(installation.id, request=object())

        self.assertEqual(result.status, "regressed")
        self.assertEqual(result.quote_id, quote.id)
        self.assertEqual(result.quote_status, "Quote Submitted")

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM Installation WHERE id = ?", (installation.id,))
            self.assertIsNone(cur.fetchone())
            cur.execute("SELECT COUNT(*) AS cnt FROM Task WHERE installation_id = ?", (installation.id,))
            self.assertEqual(int(cur.fetchone()["cnt"] or 0), 0)
            row = main.fetch_quote(conn, quote.id)
            self.assertEqual(row["status"], "Quote Submitted")

    def test_complete_implementation_forms_task_marks_complete(self) -> None:
        quote = self._create_quote()
        installation = self._create_installation(quote.id)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id
                FROM Task
                WHERE installation_id = ? AND title = ?
                LIMIT 1
                """,
                (installation.id, "Implementation Forms"),
            )
            task = cur.fetchone()
            self.assertIsNotNone(task)
            task_id = task["id"]

        with patch.object(
            main,
            "resolve_access_scope",
            return_value=("sponsor", "owner@regression.example.com"),
        ):
            updated = main.complete_implementation_forms_task(
                installation.id,
                task_id,
                request=object(),
            )

        self.assertEqual(updated.state, "Complete")

    def test_complete_implementation_forms_task_rejects_other_titles(self) -> None:
        quote = self._create_quote()
        installation = self._create_installation(quote.id)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id
                FROM Task
                WHERE installation_id = ? AND title = ?
                LIMIT 1
                """,
                (installation.id, "Program Agreement"),
            )
            task = cur.fetchone()
            self.assertIsNotNone(task)
            task_id = task["id"]

        with patch.object(main, "resolve_access_scope", return_value=("admin", "admin@example.com")):
            with self.assertRaises(HTTPException) as exc:
                main.complete_implementation_forms_task(
                    installation.id,
                    task_id,
                    request=object(),
                )
        self.assertEqual(exc.exception.status_code, 400)

    def test_delete_installation_removes_tasks_and_docs(self) -> None:
        quote = self._create_quote()
        installation = self._create_installation(quote.id)

        install_dir = self.test_uploads_dir / f"installation-{installation.id}"
        install_dir.mkdir(parents=True, exist_ok=True)
        doc_path = install_dir / "doc.txt"
        doc_path.write_text("hello", encoding="utf-8")
        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO InstallationDocument (id, installation_id, filename, path, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("doc-1", installation.id, "doc.txt", str(doc_path), main.now_iso()),
            )
            conn.commit()

        with patch.object(main, "require_session_role", return_value=None):
            response = main.delete_installation(installation.id, request=object())
        self.assertEqual(response["status"], "deleted")
        self.assertEqual(response["installation_id"], installation.id)
        self.assertEqual(response["quote_id"], quote.id)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM Installation WHERE id = ?", (installation.id,))
            self.assertIsNone(cur.fetchone())
            cur.execute("SELECT COUNT(*) AS cnt FROM Task WHERE installation_id = ?", (installation.id,))
            self.assertEqual(int(cur.fetchone()["cnt"] or 0), 0)
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM InstallationDocument WHERE installation_id = ?",
                (installation.id,),
            )
            self.assertEqual(int(cur.fetchone()["cnt"] or 0), 0)


if __name__ == "__main__":
    unittest.main()
