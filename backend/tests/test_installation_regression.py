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

    def _create_sponsor_user(
        self,
        *,
        email: str,
        organization: str,
    ) -> str:
        user_id = "sponsor-user-1"
        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO User (
                    id, first_name, last_name, email, phone, job_title, organization, role,
                    password_salt, password_hash, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    "Plan",
                    "Sponsor",
                    email,
                    "",
                    "Admin",
                    organization,
                    "sponsor",
                    "",
                    "",
                    main.now_iso(),
                    main.now_iso(),
                ),
            )
            conn.commit()
        return user_id

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
        self.assertIn("Only admin can edit broker", str(exc.exception.detail))

        with patch.object(main, "get_session_user", return_value={"role": "admin"}), patch.object(
            main, "sync_quote_to_hubspot_async", return_value=None
        ):
            updated = main.update_quote(
                quote.id,
                main.QuoteUpdate(broker_phone="555-3333"),
                request=object(),
            )
        self.assertEqual(updated.broker_phone, "555-3333")

    def test_update_quote_manual_network_syncs_primary_network(self) -> None:
        quote = self._create_quote()

        with patch.object(main, "get_session_user", return_value={"role": "admin"}), patch.object(
            main, "sync_quote_to_hubspot_async", return_value=None
        ):
            updated = main.update_quote(
                quote.id,
                main.QuoteUpdate(manual_network="Mercy_MO"),
                request=object(),
            )

        self.assertEqual(updated.manual_network, "Mercy_MO")
        self.assertEqual(updated.primary_network, "Mercy_MO")

    def test_update_quote_clearing_manual_network_uses_latest_assignment_primary(self) -> None:
        quote = self._create_quote()
        created_at = main.now_iso()
        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO AssignmentRun (
                    id, quote_id, result_json, recommendation, confidence, rationale, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "assignment-1",
                    quote.id,
                    main.json.dumps(
                        {
                            "group_summary": {
                                "primary_network": "Mercy_MO",
                                "coverage_percentage": 0.95,
                                "fallback_used": False,
                                "review_required": False,
                            }
                        }
                    ),
                    "Mercy_MO",
                    0.95,
                    "Direct contract coverage meets threshold.",
                    created_at,
                ),
            )
            conn.commit()

        with patch.object(main, "get_session_user", return_value={"role": "admin"}), patch.object(
            main, "sync_quote_to_hubspot_async", return_value=None
        ):
            updated = main.update_quote(
                quote.id,
                main.QuoteUpdate(manual_network=""),
                request=object(),
            )

        self.assertIsNone(updated.manual_network)
        self.assertEqual(updated.primary_network, "Mercy_MO")

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

    def test_convert_to_installation_sets_stoploss_disclosure_dropdown_url(self) -> None:
        quote = self._create_quote()
        url_one = "https://app.pandadoc.com/a/#/templates/bpN5tuyuHD7qzkr5t64PtQ"
        url_two = "https://app.pandadoc.com/a/#/templates/wFdvAHTyGU6REL7Mn4bby5"
        with patch.dict(
            main.os.environ,
            {
                "PANDADOC_STOPLOSS_DISCLOSURE_URLS": f"{url_one}\n{url_two};{url_one}",
                "PANDADOC_STOPLOSS_DISCLOSURE_URL": "",
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
                (installation.id, "Stoploss Disclosure"),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(
                row["task_url"],
                main.build_pandadoc_dropdown_task_url([url_one, url_two]),
            )

    def test_convert_to_installation_sets_stoploss_disclosure_labeled_dropdown_url(self) -> None:
        quote = self._create_quote()
        url_one = "https://app.pandadoc.com/a/#/templates/bpN5tuyuHD7qzkr5t64PtQ"
        url_two = "https://app.pandadoc.com/a/#/documents/MXBYyGs4H4ERZRrfjqxatN?new=true"
        with patch.dict(
            main.os.environ,
            {
                "PANDADOC_STOPLOSS_DISCLOSURE_OPTIONS": (
                    f"Arlo|{url_one}\nRyan Specialty|{url_two}"
                ),
                "PANDADOC_STOPLOSS_DISCLOSURE_URLS": "",
                "PANDADOC_STOPLOSS_DISCLOSURE_URL": "",
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
                (installation.id, "Stoploss Disclosure"),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            expected = main.build_pandadoc_dropdown_task_url_with_labels(
                [("Arlo", url_one), ("Ryan Specialty", url_two)]
            )
            self.assertEqual(row["task_url"], expected)

    def test_convert_to_installation_assigns_stoploss_disclosure_to_sponsor(self) -> None:
        quote = self._create_quote()
        sponsor_user_id = self._create_sponsor_user(
            email="owner@regression.example.com",
            organization="regression.example.com",
        )
        installation = self._create_installation(quote.id)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT owner, assigned_user_id
                FROM Task
                WHERE installation_id = ? AND title = ?
                LIMIT 1
                """,
                (installation.id, "Stoploss Disclosure"),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["owner"], "Plan Sponsor")
            self.assertEqual(row["assigned_user_id"], sponsor_user_id)

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

    def test_launch_stoploss_disclosure_creates_document_from_template(self) -> None:
        quote = self._create_quote()
        sponsor_user_id = self._create_sponsor_user(
            email="owner@regression.example.com",
            organization="regression.example.com",
        )
        installation = self._create_installation(quote.id)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, task_url, assigned_user_id
                FROM Task
                WHERE installation_id = ? AND title = ?
                LIMIT 1
                """,
                (installation.id, "Stoploss Disclosure"),
            )
            task = cur.fetchone()
            self.assertIsNotNone(task)
            task_id = task["id"]
            self.assertEqual(task["assigned_user_id"], sponsor_user_id)
            task_url = str(task["task_url"] or "")
            options = main.parse_pandadoc_dropdown_task_options(task_url)
            self.assertGreaterEqual(len(options), 1)
            selected_url = options[0][1]

        with patch.dict(main.os.environ, {"PANDADOC_API_KEY": "test-key"}, clear=False), patch.object(
            main, "resolve_access_scope", return_value=("sponsor", "owner@regression.example.com")
        ), patch.object(main, "pandadoc_api_request", return_value={"id": "doc-123"}) as mocked_api:
            launched = main.launch_stoploss_disclosure_task(
                installation.id,
                task_id,
                main.StoplossDisclosureLaunchIn(selected_url=selected_url),
                request=object(),
            )

        self.assertEqual(launched.status, "created")
        self.assertTrue(launched.created_via_api)
        self.assertEqual(launched.open_url, "https://app.pandadoc.com/a/#/documents/doc-123")
        mocked_api.assert_called_once()

    def test_launch_stoploss_disclosure_opens_document_new_link_without_api(self) -> None:
        quote = self._create_quote()
        self._create_sponsor_user(
            email="owner@regression.example.com",
            organization="regression.example.com",
        )
        with patch.dict(
            main.os.environ,
            {
                "PANDADOC_STOPLOSS_DISCLOSURE_OPTIONS": (
                    "Ryan Specialty|https://app.pandadoc.com/a/#/documents/"
                    "MXBYyGs4H4ERZRrfjqxatN?new=true"
                ),
                "PANDADOC_STOPLOSS_DISCLOSURE_URLS": "",
                "PANDADOC_STOPLOSS_DISCLOSURE_URL": "",
            },
            clear=False,
        ):
            installation = self._create_installation(quote.id)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, task_url
                FROM Task
                WHERE installation_id = ? AND title = ?
                LIMIT 1
                """,
                (installation.id, "Stoploss Disclosure"),
            )
            task = cur.fetchone()
            self.assertIsNotNone(task)
            task_id = task["id"]
            selected_url = str(task["task_url"] or "")

        with patch.object(
            main, "resolve_access_scope", return_value=("sponsor", "owner@regression.example.com")
        ), patch.object(main, "pandadoc_api_request") as mocked_api:
            launched = main.launch_stoploss_disclosure_task(
                installation.id,
                task_id,
                main.StoplossDisclosureLaunchIn(selected_url=selected_url),
                request=object(),
            )

        self.assertEqual(launched.status, "opened")
        self.assertFalse(launched.created_via_api)
        self.assertEqual(launched.open_url, selected_url)
        mocked_api.assert_not_called()

    def test_sponsor_with_assigned_task_can_see_tasks_installations_and_quote(self) -> None:
        quote = self._create_quote()
        installation = self._create_installation(quote.id)
        sponsor_user_id = self._create_sponsor_user(
            email="sponsor.viewer@gmail.com",
            organization="External Sponsor",
        )

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id
                FROM Task
                WHERE installation_id = ? AND title = ?
                LIMIT 1
                """,
                (installation.id, "Stoploss Disclosure"),
            )
            task = cur.fetchone()
            self.assertIsNotNone(task)
            stoploss_task_id = task["id"]
            cur.execute(
                """
                UPDATE Task
                SET assigned_user_id = ?
                WHERE id = ?
                """,
                (sponsor_user_id, stoploss_task_id),
            )
            conn.commit()

        with patch.object(
            main,
            "resolve_access_scope",
            return_value=("sponsor", "sponsor.viewer@gmail.com"),
        ):
            sponsor_tasks = main.list_tasks(request=object())
            sponsor_installations = main.list_installations(request=object())
            sponsor_quotes = main.list_quotes(request=object())
            detail = main.get_installation_detail(installation.id, request=object())

        self.assertTrue(any(task.id == stoploss_task_id for task in sponsor_tasks))
        self.assertTrue(any(item.id == installation.id for item in sponsor_installations))
        self.assertTrue(any(item.id == quote.id for item in sponsor_quotes))
        self.assertEqual(detail["installation"]["id"], installation.id)

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
