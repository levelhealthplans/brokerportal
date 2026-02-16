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


class AccessRequestTests(unittest.TestCase):
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

    def test_broker_domain_request_auto_approves(self) -> None:
        result = main.request_access(
            main.AccessRequestIn(
                first_name="Alyssa",
                last_name="Nguyen",
                email="alyssa@legacybrokerskc.com",
                requested_role="broker",
                organization="Legacy Brokers KC",
            )
        )
        self.assertEqual(result.status, "approved")

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM User WHERE email = ?", ("alyssa@legacybrokerskc.com",))
            user = cur.fetchone()
            self.assertIsNotNone(user)
            self.assertEqual(user["role"], "broker")
            self.assertEqual(user["organization"], "Legacy Brokers KC")
            cur.execute("SELECT COUNT(*) AS cnt FROM AccessRequest WHERE email = ?", ("alyssa@legacybrokerskc.com",))
            self.assertEqual(cur.fetchone()["cnt"], 0)

    def test_sponsor_request_creates_pending_review(self) -> None:
        result = main.request_access(
            main.AccessRequestIn(
                first_name="Sam",
                last_name="Sponsor",
                email="sam@sponsorco.com",
                requested_role="sponsor",
            )
        )
        self.assertEqual(result.status, "pending_review")

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM AccessRequest WHERE email = ?", ("sam@sponsorco.com",))
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["status"], "pending")
            self.assertEqual(row["requested_role"], "sponsor")
            self.assertEqual(row["requested_domain"], "sponsorco.com")

    def test_existing_user_request_returns_existing_user_status(self) -> None:
        with patch.object(main, "require_session_role", return_value=None):
            main.create_user(
                main.UserIn(
                    first_name="Existing",
                    last_name="Broker",
                    email="existing@legacybrokerskc.com",
                    phone="",
                    job_title="Broker",
                    organization="Legacy Brokers KC",
                    role="broker",
                    password="BrokerPass123!",
                ),
                request=object(),
            )

        result = main.request_access(
            main.AccessRequestIn(
                first_name="Existing",
                last_name="Broker",
                email="existing@legacybrokerskc.com",
                requested_role="broker",
                organization="Legacy Brokers KC",
            )
        )
        self.assertEqual(result.status, "existing_user")

    def test_admin_approve_pending_request_creates_user(self) -> None:
        main.request_access(
            main.AccessRequestIn(
                first_name="Pat",
                last_name="Plan",
                email="pat@clientco.com",
                requested_role="sponsor",
                organization="clientco.com",
            )
        )
        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM AccessRequest WHERE email = ? ORDER BY created_at DESC LIMIT 1",
                ("pat@clientco.com",),
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            access_request_id = row["id"]

        reviewer = {"id": "admin-reviewer-1", "role": "admin", "email": "admin@example.com"}
        with patch.object(main, "require_session_role", return_value=reviewer):
            approved = main.approve_access_request(
                access_request_id,
                main.AccessRequestDecisionIn(role="sponsor", organization="clientco.com"),
                request=object(),
            )
        self.assertEqual(approved.status, "approved")

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT * FROM User WHERE email = ?", ("pat@clientco.com",))
            user = cur.fetchone()
            self.assertIsNotNone(user)
            self.assertEqual(user["role"], "sponsor")
            self.assertEqual(user["organization"], "clientco.com")


if __name__ == "__main__":
    unittest.main()
