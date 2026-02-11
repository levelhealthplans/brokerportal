import shutil
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

from fastapi import HTTPException
from fastapi import Response

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class AuthProfileTests(unittest.TestCase):
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

    def _create_user(self) -> main.UserOut:
        with patch.object(main, "require_session_role", return_value=None):
            return main.create_user(
                main.UserIn(
                    first_name="Profile",
                    last_name="User",
                    email="profile.user@example.com",
                    phone="555-0000",
                    job_title="Broker",
                    organization="Legacy Brokers KC",
                    role="broker",
                    password="ProfilePass123!",
                ),
                request=object(),
            )

    def test_get_auth_profile_returns_current_user_fields(self) -> None:
        created = self._create_user()
        session_user = {
            "id": "session-1",
            "user_id": created.id,
            "email": created.email,
            "role": created.role,
            "first_name": created.first_name,
            "last_name": created.last_name,
            "organization": created.organization,
            "phone": created.phone,
            "job_title": created.job_title,
        }
        with patch.object(main, "require_session_user", return_value=session_user):
            profile = main.get_auth_profile(request=object())

        self.assertEqual(profile.email, "profile.user@example.com")
        self.assertEqual(profile.organization, "Legacy Brokers KC")
        self.assertEqual(profile.phone, "555-0000")
        self.assertEqual(profile.job_title, "Broker")

    def test_update_auth_profile_allows_name_phone_title_and_password_only(self) -> None:
        created = self._create_user()
        session_user = {
            "id": "session-2",
            "user_id": created.id,
            "email": created.email,
            "role": created.role,
            "first_name": created.first_name,
            "last_name": created.last_name,
            "organization": created.organization,
            "phone": created.phone,
            "job_title": created.job_title,
        }
        with patch.object(main, "require_session_user", return_value=session_user):
            updated = main.update_auth_profile(
                main.AuthProfileUpdateIn(
                    first_name="Updated",
                    last_name="Person",
                    phone="555-1111",
                    job_title="Senior Broker",
                    password="ProfilePass456!",
                ),
                request=object(),
            )

        self.assertEqual(updated.first_name, "Updated")
        self.assertEqual(updated.last_name, "Person")
        self.assertEqual(updated.phone, "555-1111")
        self.assertEqual(updated.job_title, "Senior Broker")
        self.assertEqual(updated.email, "profile.user@example.com")
        self.assertEqual(updated.organization, "Legacy Brokers KC")

        with self.assertRaises(HTTPException) as old_pw_exc:
            main.login_with_password(
                main.AuthLoginIn(email="profile.user@example.com", password="ProfilePass123!"),
                response=Response(),
            )
        self.assertEqual(old_pw_exc.exception.status_code, 401)
        auth = main.login_with_password(
            main.AuthLoginIn(email="profile.user@example.com", password="ProfilePass456!"),
            response=Response(),
        )
        self.assertEqual(auth.email, "profile.user@example.com")

    def test_update_auth_profile_uses_session_user_id_when_session_id_differs(self) -> None:
        created = self._create_user()
        session_user = {
            "id": "auth-session-id",
            "user_id": created.id,
            "email": created.email,
            "role": created.role,
            "first_name": created.first_name,
            "last_name": created.last_name,
            "organization": created.organization,
            "phone": created.phone,
            "job_title": created.job_title,
        }
        with patch.object(main, "require_session_user", return_value=session_user):
            main.update_auth_profile(
                main.AuthProfileUpdateIn(first_name="SessionScoped"),
                request=object(),
            )

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT first_name FROM User WHERE id = ?", (created.id,))
            row = cur.fetchone()
            self.assertEqual(row["first_name"], "SessionScoped")


if __name__ == "__main__":
    unittest.main()
