import shutil
import tempfile
import unittest
import io
from pathlib import Path
import sys
from urllib import error as urlerror
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

    def test_request_magic_link_sends_email_with_resend(self) -> None:
        with patch.object(main, "require_session_role", return_value=None):
            main.create_user(
                main.UserIn(
                    first_name="Magic",
                    last_name="User",
                    email="magic.user@example.com",
                    phone="",
                    job_title="Broker",
                    organization="Legacy Brokers KC",
                    role="broker",
                    password="MagicPass123!",
                ),
                request=object(),
            )

        with patch.object(main, "send_resend_magic_link", return_value=True) as send_mock:
            result = main.request_magic_link(main.AuthRequestIn(email="magic.user@example.com"))

        self.assertEqual(result["status"], "sent")
        send_mock.assert_called_once()
        sent_args = send_mock.call_args.args
        self.assertEqual(sent_args[0], "magic.user@example.com")
        self.assertIn("/auth/verify?token=", sent_args[1])

        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS cnt FROM AuthMagicLink WHERE email = ?", ("magic.user@example.com",))
            self.assertEqual(cur.fetchone()["cnt"], 1)

    def test_request_magic_link_returns_dev_link_when_delivery_fails_and_fallback_enabled(self) -> None:
        with patch.object(main, "require_session_role", return_value=None):
            main.create_user(
                main.UserIn(
                    first_name="Dev",
                    last_name="User",
                    email="dev.magic@example.com",
                    phone="",
                    job_title="Broker",
                    organization="Legacy Brokers KC",
                    role="broker",
                    password="MagicPass123!",
                ),
                request=object(),
            )

        with patch.object(
            main,
            "send_resend_magic_link",
            side_effect=HTTPException(status_code=502, detail="Failed to send email."),
        ), patch.object(main, "ALLOW_DEV_MAGIC_LINK_FALLBACK", True), patch.object(
            main,
            "FRONTEND_BASE_URL",
            "http://localhost:5173",
        ):
            result = main.request_magic_link(main.AuthRequestIn(email="dev.magic@example.com"))

        self.assertEqual(result["status"], "dev_link")
        self.assertIn("/auth/verify?token=", result["link"])

    def test_send_resend_magic_link_surfaces_provider_error_message(self) -> None:
        http_error = urlerror.HTTPError(
            url="https://api.resend.com/emails",
            code=403,
            msg="Forbidden",
            hdrs=None,
            fp=io.BytesIO(b'{"message":"The from address is not verified."}'),
        )
        with patch.dict(
            "os.environ",
            {
                "RESEND_API_KEY": "test-key",
                "RESEND_FROM_EMAIL": "no-reply@example.com",
            },
            clear=False,
        ), patch.object(main.urlrequest, "urlopen", side_effect=http_error):
            with self.assertRaises(HTTPException) as exc:
                main.send_resend_magic_link(
                    "recipient@example.com",
                    "http://localhost:5173/auth/verify?token=abc",
                )
        self.assertEqual(exc.exception.status_code, 502)
        self.assertIn("from address is not verified", str(exc.exception.detail).lower())

    def test_create_quote_uses_signed_in_user_identity(self) -> None:
        payload = main.QuoteCreate(
            company="Session Bound Group",
            employer_street="1 Main St",
            employer_city="St. Louis",
            state="MO",
            employer_zip="63101",
            employer_domain="group.example.com",
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
            broker_first_name="Ignored",
            broker_last_name="Ignored",
            broker_email="ignored@example.com",
            broker_phone="555-9999",
            agent_of_record=True,
            broker_org="Ignored Org",
            sponsor_domain="ignored.example.com",
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
        with patch.object(main, "require_session_user", return_value=session_user):
            quote = main.create_quote(payload, request=object())

        self.assertEqual(quote.broker_email, "jake@legacybrokerskc.com")
        self.assertEqual(quote.broker_first_name, "Jake")
        self.assertEqual(quote.broker_last_name, "Page")
        self.assertEqual(quote.broker_phone, "555-1111")
        self.assertEqual(quote.assigned_user_id, "session-user-1")
        self.assertEqual(quote.broker_org, "Legacy Brokers KC")

    def test_non_admin_cannot_edit_hubspot_detail_fields(self) -> None:
        payload = main.QuoteCreate(
            company="HubSpot Detail Guard Group",
            employer_street="1 Main St",
            employer_city="St. Louis",
            state="MO",
            employer_zip="63101",
            employer_domain="group.example.com",
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
            quote = main.create_quote(payload, request=object())

        with patch.object(main, "get_session_user", return_value={"role": "broker"}), patch.object(
            main, "sync_quote_to_hubspot_async", return_value=None
        ):
            with self.assertRaises(HTTPException) as exc:
                main.update_quote(
                    quote.id,
                    main.QuoteUpdate(primary_network="Mercy_MO"),
                    request=object(),
                )
        self.assertEqual(exc.exception.status_code, 403)

    def test_list_installations_broker_without_org_mapping_does_not_500(self) -> None:
        with main.get_db() as conn:
            cur = conn.cursor()
            now = main.now_iso()
            cur.execute(
                """
                INSERT INTO User (
                    id, first_name, last_name, email, phone, job_title, organization, role,
                    password_salt, password_hash, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "broker-user-1",
                    "Jake",
                    "Simpara",
                    "jake@simparahr.com",
                    "",
                    "Broker",
                    "Simpara HR",
                    "broker",
                    "salt",
                    "hash",
                    now,
                    now,
                ),
            )
            conn.commit()

        with patch.object(
            main,
            "resolve_access_scope",
            return_value=("broker", "jake@simparahr.com"),
        ):
            installations = main.list_installations(request=object())
        self.assertEqual(installations, [])


if __name__ == "__main__":
    unittest.main()
