import io
import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
import sys
from unittest.mock import patch

from fastapi import UploadFile

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class StandardizationRulesTests(unittest.TestCase):
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
            company="Validation Rules Group",
            employer_street="1 Main St",
            employer_city="St. Louis",
            state="MO",
            employer_zip="63101",
            employer_domain="validation-rules.example.com",
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

    def _upload_census(self, quote_id: str, csv_text: str) -> None:
        with patch.object(main, "sync_quote_to_hubspot_async", return_value=None):
            main.upload_quote_file(
                quote_id,
                type="census",
                file=UploadFile(filename="members.csv", file=io.BytesIO(csv_text.encode("utf-8"))),
            )

    def test_required_field_rules_report_expected_messages(self) -> None:
        quote = self._create_quote()
        census_csv = (
            "first_name,last_name,dob,zip,gender,relationship,enrollment_tier\n"
            ",Doe,2999-01-01,ABCDE,X,Z,ZZ\n"
        )
        self._upload_census(quote.id, census_csv)

        result = main.run_standardization(quote.id, None)
        messages = {str(issue.get("issue") or "") for issue in result.issues_json}
        rules = {str(issue.get("rule") or "") for issue in result.issues_json}

        self.assertIn("First Name is required", messages)
        self.assertIn("DOB cannot be in the future", messages)
        self.assertIn("Zip is invalid format", messages)
        self.assertIn("Gender must be M or F", messages)
        self.assertIn("Relationship must be E, S, or C", messages)
        self.assertIn("Enrollment Tier must be EE, ES, EC, EF, or W", messages)
        self.assertIn("F1", rules)
        self.assertIn("F3", rules)
        self.assertIn("F4", rules)
        self.assertIn("F5", rules)
        self.assertIn("F6", rules)
        self.assertIn("F7", rules)

    def test_zip_plus4_is_accepted_and_normalized(self) -> None:
        quote = self._create_quote()
        census_csv = (
            "first_name,last_name,dob,zip,gender,relationship,enrollment_tier\n"
            "Jane,Doe,1988-02-14,63101-1234,F,E,EE\n"
        )
        self._upload_census(quote.id, census_csv)

        result = main.run_standardization(quote.id, None)

        self.assertEqual(result.issue_count, 0)
        self.assertTrue(result.standardized_path)
        body = Path(result.standardized_path or "").read_text(encoding="utf-8")
        self.assertIn(",63101,", body)

    def test_tier_count_reconciliation_rules_fire(self) -> None:
        quote = self._create_quote()
        census_csv = (
            "first_name,last_name,dob,zip,gender,relationship,enrollment_tier\n"
            "Erin,One,1980-01-01,63101,F,E,ES\n"
            "Evan,Two,1981-01-01,63101,M,E,EC\n"
            "Sam,Spouse,1982-01-01,63101,F,S,EF\n"
            "Chris,Child,2010-01-01,63101,M,C,ES\n"
        )
        self._upload_census(quote.id, census_csv)

        result = main.run_standardization(quote.id, None)
        rules = {str(issue.get("rule") or "") for issue in result.issues_json}

        self.assertIn("TC1", rules)
        self.assertIn("TC5", rules)
        self.assertIn("TC8", rules)

    def test_relationship_age_and_waived_dependent_rules_fire(self) -> None:
        quote = self._create_quote()
        under_18_year = datetime.utcnow().year - 10
        over_26_year = datetime.utcnow().year - 30
        census_csv = (
            "first_name,last_name,dob,zip,gender,relationship,enrollment_tier\n"
            f"Young,Employee,{under_18_year}-01-01,63101,M,E,W\n"
            f"Older,Child,{over_26_year}-01-01,63101,F,C,W\n"
        )
        self._upload_census(quote.id, census_csv)

        result = main.run_standardization(quote.id, None)
        rules = {str(issue.get("rule") or "") for issue in result.issues_json}

        self.assertIn("R2", rules)
        self.assertIn("R3", rules)
        self.assertIn("R5", rules)
        self.assertIn("TC12", rules)


if __name__ == "__main__":
    unittest.main()
