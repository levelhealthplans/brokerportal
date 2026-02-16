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


class NotificationTests(unittest.TestCase):
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

    def _insert_user(self, user_id: str, email: str, role: str = "broker") -> None:
        now = main.now_iso()
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
                    "Test",
                    "User",
                    email,
                    "",
                    "Broker",
                    "Legacy Brokers KC",
                    role,
                    None,
                    None,
                    now,
                    now,
                ),
            )
            conn.commit()

    def _create_quote(self) -> main.QuoteOut:
        payload = main.QuoteCreate(
            company="Notification Group",
            employer_street="1 Main St",
            employer_city="St. Louis",
            state="MO",
            employer_zip="63101",
            employer_domain="notify.example.com",
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

    def test_update_quote_assignment_creates_notification(self) -> None:
        self._insert_user("target-user-1", "target1@example.com")
        quote = self._create_quote()

        with patch.object(main, "get_session_user", return_value={"role": "admin"}), patch.object(
            main, "sync_quote_to_hubspot_async", return_value=None
        ):
            main.update_quote(
                quote.id,
                main.QuoteUpdate(assigned_user_id="target-user-1"),
                request=object(),
            )

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM Notification
                WHERE user_id = ? AND kind = ? AND entity_type = ? AND entity_id = ?
                """,
                ("target-user-1", "quote_assigned", "quote", quote.id),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertIn("Notification Group", row["body"])
            self.assertEqual(int(row["is_read"] or 0), 0)

    def test_assign_tasks_to_user_creates_notification(self) -> None:
        self._insert_user("target-user-2", "target2@example.com")
        quote = self._create_quote()
        installation = self._create_installation(quote.id)

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM Task WHERE installation_id = ? ORDER BY title ASC LIMIT 1",
                (installation.id,),
            )
            task_row = cur.fetchone()
            self.assertIsNotNone(task_row)
            task_id = task_row["id"]

        with patch.object(main, "require_session_role", return_value=None):
            main.assign_tasks_to_user(
                "target-user-2",
                main.UserAssignIn(task_ids=[task_id]),
                request=object(),
            )

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM Notification
                WHERE user_id = ? AND kind = ? AND entity_type = ? AND entity_id = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                ("target-user-2", "task_assigned", "installation", installation.id),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertIn("was assigned to you", row["body"])
            self.assertEqual(int(row["is_read"] or 0), 0)

    def test_notification_read_endpoints(self) -> None:
        self._insert_user("target-user-3", "target3@example.com")
        with main.get_db() as conn:
            main.create_notification(
                conn,
                "target-user-3",
                kind="quote_assigned",
                title="Quote assigned",
                body="Acme was assigned to you.",
                entity_type="quote",
                entity_id="quote-1",
            )
            main.create_notification(
                conn,
                "target-user-3",
                kind="task_assigned",
                title="Task assigned",
                body="Task was assigned to you.",
                entity_type="installation",
                entity_id="install-1",
            )
            conn.commit()

        session_user = {"id": "target-user-3", "role": "broker", "email": "target3@example.com"}
        with patch.object(main, "require_session_user", return_value=session_user):
            listed = main.list_notifications(request=object(), limit=10)
            self.assertEqual(len(listed), 2)
            unread = main.get_notification_unread_count(request=object())
            self.assertEqual(unread.unread_count, 2)

            first = listed[0]
            marked = main.mark_notification_read(first.id, request=object())
            self.assertTrue(marked.is_read)

            unread_after_one = main.get_notification_unread_count(request=object())
            self.assertEqual(unread_after_one.unread_count, 1)

            bulk = main.mark_all_notifications_read(request=object())
            self.assertEqual(bulk["status"], "ok")
            self.assertEqual(int(bulk["updated_count"]), 1)

            unread_after_all = main.get_notification_unread_count(request=object())
            self.assertEqual(unread_after_all.unread_count, 0)

    def test_create_notification_sends_resend_email(self) -> None:
        self._insert_user("target-user-4", "target4@example.com")

        with main.get_db() as conn, patch.object(
            main,
            "send_resend_notification_email",
            return_value=True,
        ) as send_mock:
            main.create_notification(
                conn,
                "target-user-4",
                kind="quote_assigned",
                title="Quote assigned",
                body="Acme was assigned to you.",
                entity_type="quote",
                entity_id="quote-123",
            )
            conn.commit()

        send_mock.assert_called_once_with(
            "target4@example.com",
            title="Quote assigned",
            body="Acme was assigned to you.",
            entity_type="quote",
            entity_id="quote-123",
        )

    def test_create_notification_skips_resend_without_user_email(self) -> None:
        self._insert_user("target-user-5", "")

        with main.get_db() as conn, patch.object(
            main,
            "send_resend_notification_email",
            return_value=True,
        ) as send_mock:
            main.create_notification(
                conn,
                "target-user-5",
                kind="task_assigned",
                title="Task assigned",
                body="Task was assigned to you.",
                entity_type="installation",
                entity_id="install-123",
            )
            conn.commit()

        send_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
