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


class OrganizationSyncTests(unittest.TestCase):
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

    def test_level_health_org_backfilled_from_default_admin(self) -> None:
        with patch.object(main, "require_session_role", return_value=None):
            rows = main.list_organizations(request=object())
        self.assertTrue(
            any(
                row.name == "Level Health"
                and row.type == "broker"
                and row.domain == "levelhealthplans.com"
                for row in rows
            )
        )

    def test_broker_user_org_backfilled_for_display(self) -> None:
        payload = main.UserIn(
            first_name="Amy",
            last_name="Stone",
            email="amy@acmebenefits.com",
            phone="",
            job_title="Broker",
            organization="Acme Benefits",
            role="broker",
            password="BrokerPass123!",
        )
        with patch.object(main, "require_session_role", return_value=None):
            main.create_user(payload, request=object())
            rows = main.list_organizations(request=object())
        self.assertTrue(
            any(
                row.name == "Acme Benefits"
                and row.type == "broker"
                and row.domain == "acmebenefits.com"
                for row in rows
            )
        )


if __name__ == "__main__":
    unittest.main()
