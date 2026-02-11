import json
import tempfile
import unittest
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class HubspotSettingsStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.temp_root = Path(self.tempdir.name)
        self.original_settings_path = main.HUBSPOT_SETTINGS_PATH
        self.original_legacy_path = main.LEGACY_HUBSPOT_SETTINGS_PATH
        self.settings_path = self.temp_root / "persisted" / "hubspot_settings.json"
        self.legacy_path = self.temp_root / "legacy" / "hubspot_settings.json"
        main.HUBSPOT_SETTINGS_PATH = self.settings_path
        main.LEGACY_HUBSPOT_SETTINGS_PATH = self.legacy_path

    def tearDown(self) -> None:
        main.HUBSPOT_SETTINGS_PATH = self.original_settings_path
        main.LEGACY_HUBSPOT_SETTINGS_PATH = self.original_legacy_path
        self.tempdir.cleanup()

    def test_read_hubspot_settings_migrates_legacy_file(self) -> None:
        legacy_payload = {
            "enabled": True,
            "portal_id": "7106327",
            "pipeline_id": "98238573",
            "default_stage_id": "",
            "sync_quote_to_hubspot": True,
            "sync_hubspot_to_quote": True,
            "ticket_subject_template": "Quote {{company}} ({{quote_id}})",
            "ticket_content_template": "Company: {{company}}",
            "property_mappings": {"company": "level_health_company_cached"},
            "quote_status_to_stage": {"Draft": "101"},
            "stage_to_quote_status": {"101": "Draft"},
            "oauth_redirect_uri": "https://example.com/callback",
            "private_app_token": "",
            "oauth_access_token": "access-1",
            "oauth_refresh_token": "refresh-1",
            "oauth_expires_at": "",
            "oauth_hub_id": "7106327",
        }
        self.legacy_path.parent.mkdir(parents=True, exist_ok=True)
        self.legacy_path.write_text(json.dumps(legacy_payload), encoding="utf-8")

        loaded = main.read_hubspot_settings(include_token=True)

        self.assertTrue(self.settings_path.exists())
        migrated = json.loads(self.settings_path.read_text(encoding="utf-8"))
        self.assertEqual(
            migrated["property_mappings"]["company"],
            "level_health_company_cached",
        )
        self.assertEqual(loaded["property_mappings"]["company"], "level_health_company_cached")
        self.assertEqual(loaded["oauth_hub_id"], "7106327")
        self.assertTrue(loaded["oauth_connected"])

    def test_write_hubspot_settings_persists_to_configured_path(self) -> None:
        update = main.HubSpotSettingsUpdate(
            enabled=True,
            portal_id="7106327",
            pipeline_id="98238573",
            default_stage_id="",
            sync_quote_to_hubspot=True,
            sync_hubspot_to_quote=True,
            property_mappings={"company": "level_health_company_cached"},
            quote_status_to_stage={"Draft": "101"},
            stage_to_quote_status={"101": "Draft"},
        )

        main.write_hubspot_settings(update, existing_token=None)

        self.assertTrue(self.settings_path.exists())
        stored = json.loads(self.settings_path.read_text(encoding="utf-8"))
        self.assertEqual(stored["portal_id"], "7106327")
        self.assertEqual(stored["property_mappings"]["company"], "level_health_company_cached")
        self.assertEqual(stored["quote_status_to_stage"]["Draft"], "101")


if __name__ == "__main__":
    unittest.main()
