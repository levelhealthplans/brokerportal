import io
import shutil
import tempfile
import unittest
import uuid
from pathlib import Path
import sys
from unittest.mock import patch

from fastapi import UploadFile

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class HubspotUploadMappingTests(unittest.TestCase):
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
            company="Upload Mapping Group",
            employer_street="1 Main St",
            employer_city="St. Louis",
            state="MO",
            employer_zip="63101",
            employer_domain="upload-mapping.example.com",
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

    def test_quote_hubspot_context_includes_upload_fields(self) -> None:
        quote = self._create_quote()
        now = main.now_iso()
        with main.get_db() as conn:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO Upload (id, quote_id, type, filename, path, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        str(uuid.uuid4()),
                        quote.id,
                        "census",
                        "members.csv",
                        str(self.test_uploads_dir / quote.id / "members.csv"),
                        now,
                    ),
                    (
                        str(uuid.uuid4()),
                        quote.id,
                        "sbc",
                        "sbc.pdf",
                        str(self.test_uploads_dir / quote.id / "sbc.pdf"),
                        now,
                    ),
                ],
            )
            conn.commit()

            row = main.fetch_quote(conn, quote.id)
            context = main.build_quote_hubspot_context(conn, dict(row))

        self.assertTrue(context["census_uploaded"])
        self.assertTrue(context["sbc_uploaded"])
        self.assertFalse(context["renewal_uploaded"])
        self.assertFalse(context["current_pricing_uploaded"])
        self.assertFalse(context["aggregate_report_uploaded"])
        self.assertFalse(context["high_cost_claimant_report_uploaded"])
        self.assertFalse(context["other_claims_data_uploaded"])
        self.assertFalse(context["other_files_uploaded"])
        self.assertEqual(context["census_latest_filename"], "members.csv")
        self.assertEqual(context["census_latest_uploaded_at"], now)
        self.assertEqual(context["upload_count"], 2)
        self.assertIn("members.csv:", context["upload_files"])
        self.assertIn("sbc.pdf:", context["upload_files"])
        self.assertIn("/uploads/", context["upload_files"])

    def test_build_ticket_properties_maps_upload_fields(self) -> None:
        quote_context = {
            "id": "q-123",
            "company": "Trademark",
            "status": "Draft",
            "census_uploaded": True,
            "census_latest_filename": "members.csv",
            "sbc_uploaded": False,
            "upload_files": "members.csv: https://portal.example/uploads/q-123/members.csv",
        }
        settings = {
            "quote_status_to_stage": {},
            "default_stage_id": "",
            "ticket_subject_template": "Quote {{id}}",
            "ticket_content_template": "Census uploaded: {{census_uploaded}}",
            "pipeline_id": "",
            "property_mappings": {
                "census_uploaded": "level_health_census_uploaded",
                "census_latest_filename": "level_health_census_filename",
                "sbc_uploaded": "level_health_sbc_uploaded",
            },
        }

        properties = main.build_hubspot_ticket_properties(quote_context, settings)

        self.assertEqual(properties["subject"], "Trademark")
        self.assertEqual(properties["level_health_census_uploaded"], "true")
        self.assertEqual(properties["level_health_census_filename"], "members.csv")
        self.assertEqual(properties["level_health_sbc_uploaded"], "false")
        self.assertIn("Uploads:", properties["content"])
        self.assertIn("members.csv:", properties["content"])

    def test_build_ticket_properties_keeps_non_numeric_stage_ids(self) -> None:
        quote_context = {
            "id": "q-999",
            "status": "Draft",
        }
        settings = {
            "quote_status_to_stage": {"Draft": "appointmentscheduled"},
            "default_stage_id": "",
            "ticket_subject_template": "Quote {{id}}",
            "ticket_content_template": "Status: {{status}}",
            "pipeline_id": "default",
            "property_mappings": {},
        }

        properties = main.build_hubspot_ticket_properties(quote_context, settings)
        sanitized, removed = main.sanitize_hubspot_ticket_properties(properties)

        self.assertEqual(properties["hs_pipeline_stage"], "appointmentscheduled")
        self.assertEqual(sanitized["hs_pipeline_stage"], "appointmentscheduled")
        self.assertEqual(removed, [])

    def test_sanitize_hubspot_ticket_properties_keeps_writable_reserved_fields(self) -> None:
        input_properties = {
            "subject": "Quote ACME",
            "content": "Body",
            "hs_pipeline": "default",
            "hs_pipeline_stage": "appointmentscheduled",
            "hs_ticket_id": "12345",
            "hs_primary_company": "read-only-value",
            "level_health_company": "ACME",
        }

        sanitized, removed = main.sanitize_hubspot_ticket_properties(input_properties)

        self.assertEqual(sanitized["subject"], "Quote ACME")
        self.assertEqual(sanitized["content"], "Body")
        self.assertEqual(sanitized["hs_pipeline"], "default")
        self.assertEqual(sanitized["hs_pipeline_stage"], "appointmentscheduled")
        self.assertEqual(sanitized["level_health_company"], "ACME")
        self.assertNotIn("hs_ticket_id", sanitized)
        self.assertNotIn("hs_primary_company", sanitized)
        self.assertIn("hs_ticket_id", removed)
        self.assertIn("hs_primary_company", removed)

    def test_find_existing_hubspot_ticket_for_quote_uses_quote_id_mapping(self) -> None:
        quote = {"id": "quote-123"}
        settings = {"property_mappings": {"id": "level_health_quote_id"}}
        with patch.object(main, "hubspot_search_object_id", return_value="ticket-999") as search_mock:
            found = main.find_existing_hubspot_ticket_for_quote("token-1", quote, settings)
        self.assertEqual(found, "ticket-999")
        search_mock.assert_called_once_with(
            "token-1",
            "tickets",
            "level_health_quote_id",
            "quote-123",
            properties=["level_health_quote_id", "subject", "hs_pipeline_stage"],
        )

    def test_sync_quote_from_hubspot_updates_network_detail_fields(self) -> None:
        quote = self._create_quote()
        with main.get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE Quote SET hubspot_ticket_id = ? WHERE id = ?",
                ("ticket-123", quote.id),
            )
            conn.commit()

        settings = {
            "enabled": True,
            "sync_hubspot_to_quote": True,
            "portal_id": "7106327",
            "stage_to_quote_status": {},
            "property_mappings": {
                "primary_network": "level_health_primary_network",
                "secondary_network": "level_health_secondary_network",
                "tpa": "level_health_tpa",
                "stoploss": "level_health_stoploss",
                "current_carrier": "level_health_current_carrier",
                "renewal_comparison": "level_health_renewal_comparison",
            },
        }
        ticket = {
            "properties": {
                "hs_pipeline_stage": "",
                "level_health_primary_network": "Mercy_MO",
                "level_health_secondary_network": "Cigna_PPO",
                "level_health_tpa": "UHC",
                "level_health_stoploss": "Sun Life",
                "level_health_current_carrier": "Aetna",
                "level_health_renewal_comparison": "12% increase",
            }
        }

        with main.get_db() as conn, patch.object(
            main, "read_hubspot_settings", return_value=settings
        ), patch.object(
            main, "resolve_hubspot_api_token", return_value="token-1"
        ), patch.object(
            main, "hubspot_api_request", return_value=ticket
        ):
            main.sync_quote_from_hubspot(conn, quote.id)
            refreshed = dict(main.fetch_quote(conn, quote.id))

        self.assertEqual(refreshed["primary_network"], "Mercy_MO")
        self.assertEqual(refreshed["secondary_network"], "Cigna_PPO")
        self.assertEqual(refreshed["tpa"], "UHC")
        self.assertEqual(refreshed["stoploss"], "Sun Life")
        self.assertEqual(refreshed["current_carrier"], "Aetna")
        self.assertEqual(refreshed["renewal_comparison"], "12% increase")

    def test_sync_hubspot_ticket_file_attachments_is_idempotent(self) -> None:
        quote = self._create_quote()
        with patch.object(main, "sync_quote_to_hubspot_async", return_value=None):
            upload = main.upload_quote_file(
                quote.id,
                type="other_files",
                file=UploadFile(filename="supporting.pdf", file=io.BytesIO(b"pdf-bytes")),
            )

        with main.get_db() as conn, patch.object(
            main, "upload_file_to_hubspot", return_value="file-1"
        ) as upload_mock, patch.object(
            main, "create_hubspot_note_with_attachment", return_value="note-1"
        ) as note_mock:
            warning_one = main.sync_hubspot_ticket_file_attachments(
                conn,
                "token-1",
                quote_id=quote.id,
                ticket_id="ticket-1",
            )
            warning_two = main.sync_hubspot_ticket_file_attachments(
                conn,
                "token-1",
                quote_id=quote.id,
                ticket_id="ticket-1",
            )
            cur = conn.cursor()
            cur.execute(
                """
                SELECT hubspot_file_id, hubspot_note_id
                FROM HubSpotTicketAttachmentSync
                WHERE upload_id = ? AND ticket_id = ?
                """,
                (upload.id, "ticket-1"),
            )
            row = cur.fetchone()

        self.assertIsNone(warning_one)
        self.assertIsNone(warning_two)
        self.assertEqual(upload_mock.call_count, 1)
        self.assertEqual(note_mock.call_count, 1)
        self.assertEqual(row["hubspot_file_id"], "file-1")
        self.assertEqual(row["hubspot_note_id"], "note-1")

    def test_upload_and_delete_trigger_hubspot_resync(self) -> None:
        quote = self._create_quote()

        with patch.object(main, "sync_quote_to_hubspot_async", return_value=None) as sync_mock:
            upload = main.upload_quote_file(
                quote.id,
                type="census",
                file=UploadFile(filename="members.csv", file=io.BytesIO(b"zip\n63101\n")),
            )
            sync_mock.assert_called_with(quote.id, create_if_missing=False)

            sync_mock.reset_mock()
            result = main.delete_quote_upload(quote.id, upload.id)
            self.assertEqual(result["status"], "deleted")
            sync_mock.assert_called_with(quote.id, create_if_missing=False)

    def test_associate_hubspot_note_to_ticket_prefers_v4_default(self) -> None:
        with patch.object(main, "associate_hubspot_records_default", return_value=None) as assoc_mock, patch.object(
            main, "hubspot_api_request"
        ) as v3_mock:
            main.associate_hubspot_note_to_ticket("token-1", note_id="note-1", ticket_id="ticket-1")

        assoc_mock.assert_called_once_with(
            "token-1",
            from_object_type="note",
            from_object_id="note-1",
            to_object_type="ticket",
            to_object_id="ticket-1",
        )
        v3_mock.assert_not_called()

    def test_associate_hubspot_note_to_ticket_falls_back_to_v3_variants(self) -> None:
        with patch.object(
            main,
            "associate_hubspot_records_default",
            side_effect=Exception("v4 unavailable"),
        ) as assoc_mock, patch.object(
            main,
            "hubspot_api_request",
            side_effect=[
                Exception("first v3 path failed"),
                {},
            ],
        ) as v3_mock:
            main.associate_hubspot_note_to_ticket("token-1", note_id="note-2", ticket_id="ticket-2")

        assoc_mock.assert_called_once()
        self.assertEqual(v3_mock.call_count, 2)
        first_call = v3_mock.call_args_list[0]
        second_call = v3_mock.call_args_list[1]
        self.assertIn("/associations/ticket/ticket-2/note_to_ticket", first_call.args[2])
        self.assertIn("/associations/ticket/ticket-2/228", second_call.args[2])


if __name__ == "__main__":
    unittest.main()
