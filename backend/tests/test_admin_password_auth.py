import shutil
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

from fastapi import HTTPException, Response

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class AdminPasswordAuthTests(unittest.TestCase):
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

    def test_admin_can_create_user_and_login_with_email_password(self) -> None:
        payload = main.UserIn(
            first_name="Demo",
            last_name="Broker",
            email="demo.broker@example.com",
            phone="",
            job_title="Broker",
            organization="Legacy Brokers KC",
            role="broker",
            password="BrokerPass123!",
        )
        with patch.object(main, "require_session_role", return_value=None):
            created = main.create_user(payload, request=object())

        self.assertEqual(created.email, "demo.broker@example.com")
        self.assertEqual(created.role, "broker")

        auth = main.login_with_password(
            main.AuthLoginIn(email="demo.broker@example.com", password="BrokerPass123!"),
            response=Response(),
        )
        self.assertEqual(auth.email, "demo.broker@example.com")
        self.assertEqual(auth.role, "broker")

    def test_admin_password_reset_changes_login_and_revokes_sessions(self) -> None:
        with patch.object(main, "require_session_role", return_value=None):
            created = main.create_user(
                main.UserIn(
                    first_name="Ops",
                    last_name="User",
                    email="ops.user@example.com",
                    phone="",
                    job_title="Ops",
                    organization="Level Health",
                    role="sponsor",
                    password="InitialPass123!",
                ),
                request=object(),
            )

        auth = main.login_with_password(
            main.AuthLoginIn(email="ops.user@example.com", password="InitialPass123!"),
            response=Response(),
        )
        self.assertEqual(auth.role, "sponsor")

        with main.get_db() as conn:
            session_token = main.create_auth_session(conn, created.id)
            self.assertTrue(session_token)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS cnt FROM AuthSession WHERE user_id = ?", (created.id,))
            self.assertEqual(cur.fetchone()["cnt"], 2)

        with patch.object(main, "require_session_role", return_value=None):
            updated = main.update_user(
                created.id,
                main.UserUpdate(password="ResetPass456!"),
                request=object(),
            )
        self.assertEqual(updated.email, "ops.user@example.com")

        with self.assertRaises(HTTPException) as old_pw_exc:
            main.login_with_password(
                main.AuthLoginIn(email="ops.user@example.com", password="InitialPass123!"),
                response=Response(),
            )
        self.assertEqual(old_pw_exc.exception.status_code, 401)

        new_auth = main.login_with_password(
            main.AuthLoginIn(email="ops.user@example.com", password="ResetPass456!"),
            response=Response(),
        )
        self.assertEqual(new_auth.email, "ops.user@example.com")

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS cnt FROM AuthSession WHERE user_id = ?", (created.id,))
            self.assertEqual(cur.fetchone()["cnt"], 1)

    def test_create_user_requires_password(self) -> None:
        payload = main.UserIn(
            first_name="No",
            last_name="Password",
            email="nopass@example.com",
            phone="",
            job_title="Broker",
            organization="Legacy Brokers KC",
            role="broker",
            password="",
        )
        with patch.object(main, "require_session_role", return_value=None):
            with self.assertRaises(HTTPException) as exc:
                main.create_user(payload, request=object())
        self.assertEqual(exc.exception.status_code, 400)
        self.assertIn("Password is required", str(exc.exception.detail))

    def test_create_user_rejects_duplicate_email(self) -> None:
        with patch.object(main, "require_session_role", return_value=None):
            main.create_user(
                main.UserIn(
                    first_name="First",
                    last_name="User",
                    email="dup@example.com",
                    phone="",
                    job_title="Broker",
                    organization="Legacy Brokers KC",
                    role="broker",
                    password="BrokerPass123!",
                ),
                request=object(),
            )
            with self.assertRaises(HTTPException) as exc:
                main.create_user(
                    main.UserIn(
                        first_name="Second",
                        last_name="User",
                        email="dup@example.com",
                        phone="",
                        job_title="Broker",
                        organization="Legacy Brokers KC",
                        role="broker",
                        password="AnotherPass123!",
                    ),
                    request=object(),
                )
        self.assertEqual(exc.exception.status_code, 400)
        self.assertIn("Email already exists", str(exc.exception.detail))


if __name__ == "__main__":
    unittest.main()
