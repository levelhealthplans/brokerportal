import os
import tempfile
import unittest
from pathlib import Path
from urllib import parse as urlparse
import sys
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class HubspotOAuthScopeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.original_db_path = main.DB_PATH
        cls.original_settings_path = main.HUBSPOT_SETTINGS_PATH
        cls.original_legacy_settings_path = main.LEGACY_HUBSPOT_SETTINGS_PATH
        cls.tempdir = tempfile.TemporaryDirectory()
        cls.temp_root = Path(cls.tempdir.name)
        cls.test_db_path = cls.temp_root / "test.db"
        cls.test_settings_path = cls.temp_root / "hubspot_settings.json"
        cls.test_legacy_settings_path = cls.temp_root / "legacy_hubspot_settings.json"

    @classmethod
    def tearDownClass(cls) -> None:
        main.DB_PATH = cls.original_db_path
        main.HUBSPOT_SETTINGS_PATH = cls.original_settings_path
        main.LEGACY_HUBSPOT_SETTINGS_PATH = cls.original_legacy_settings_path
        cls.tempdir.cleanup()

    def setUp(self) -> None:
        if self.test_db_path.exists():
            self.test_db_path.unlink()
        if self.test_settings_path.exists():
            self.test_settings_path.unlink()
        if self.test_legacy_settings_path.exists():
            self.test_legacy_settings_path.unlink()
        main.DB_PATH = self.test_db_path
        main.HUBSPOT_SETTINGS_PATH = self.test_settings_path
        main.LEGACY_HUBSPOT_SETTINGS_PATH = self.test_legacy_settings_path
        main.init_db()

    def test_start_oauth_always_includes_required_scopes(self) -> None:
        with patch.dict(
            os.environ,
            {
                "HUBSPOT_CLIENT_ID": "client-1",
                "HUBSPOT_CLIENT_SECRET": "secret-1",
                "HUBSPOT_OAUTH_SCOPES": "crm.objects.contacts.read oauth",
            },
            clear=False,
        ), patch.object(main, "require_session_role", return_value=None):
            payload = main.HubSpotOAuthStartIn(
                redirect_uri="https://portal.example/api/integrations/hubspot/oauth/callback"
            )
            result = main.start_hubspot_oauth(payload, request=object())

        parsed = urlparse.urlparse(result.authorize_url)
        scope_value = urlparse.parse_qs(parsed.query).get("scope", [""])[0]
        scopes = scope_value.split()

        self.assertIn("oauth", scopes)
        self.assertIn("tickets", scopes)
        self.assertIn("files", scopes)
        self.assertIn("crm.objects.contacts.read", scopes)
        self.assertEqual(scopes.count("files"), 1)


if __name__ == "__main__":
    unittest.main()
