import io
import shutil
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import patch

from fastapi import UploadFile

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class StandardizationDobTests(unittest.TestCase):
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
            company="DOB Formatting Group",
            employer_street="1 Main St",
            employer_city="St. Louis",
            state="MO",
            employer_zip="63101",
            employer_domain="dob-formatting.example.com",
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

    def test_standardization_accepts_datetime_dob_and_normalizes_to_date_only(self) -> None:
        quote = self._create_quote()
        census_csv = (
            "first_name,last_name,dob,zip,gender,relationship,enrollment_tier\n"
            "John,Doe,1968-01-26 00:00:00,63101,M,E,EE\n"
        )
        with patch.object(main, "sync_quote_to_hubspot_async", return_value=None):
            main.upload_quote_file(
                quote.id,
                type="census",
                file=UploadFile(filename="members.csv", file=io.BytesIO(census_csv.encode("utf-8"))),
            )

        result = main.run_standardization(quote.id, main.StandardizationIn())

        self.assertEqual(result.issue_count, 0)
        self.assertTrue(result.standardized_path)
        standardized = Path(result.standardized_path or "")
        self.assertTrue(standardized.exists())
        body = standardized.read_text(encoding="utf-8")
        self.assertIn("1968-01-26", body)
        self.assertNotIn("1968-01-26 00:00:00", body)


if __name__ == "__main__":
    unittest.main()
