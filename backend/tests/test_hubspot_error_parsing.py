import unittest
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class HubspotErrorParsingTests(unittest.TestCase):
    def test_extract_missing_required_properties_from_context(self) -> None:
        payload = {
            "status": "error",
            "message": "Error creating TICKET. Some required properties were not set.",
            "context": {
                "properties": ["level_health_quote_status", "level_health_company"],
            },
        }
        names = main.extract_hubspot_missing_required_properties(
            payload,
            error_message=payload["message"],
        )
        self.assertEqual(
            names,
            ["level_health_quote_status", "level_health_company"],
        )

    def test_extract_missing_required_properties_from_nested_errors(self) -> None:
        payload = {
            "status": "error",
            "message": "Validation failed",
            "errors": [
                {
                    "message": "Missing required properties",
                    "context": {"missingRequiredProperties": ["foo_prop", "bar_prop"]},
                }
            ],
        }
        names = main.extract_hubspot_missing_required_properties(
            payload,
            error_message=payload["message"],
        )
        self.assertEqual(names, ["foo_prop", "bar_prop"])

    def test_extract_missing_required_properties_from_message_brackets(self) -> None:
        message = "Error creating TICKET. Required properties were not set: [foo, bar, baz]"
        names = main.extract_hubspot_missing_required_properties(
            {},
            error_message=message,
        )
        self.assertEqual(names, ["foo", "bar", "baz"])

    def test_extract_missing_required_properties_dedupes(self) -> None:
        payload = {
            "status": "error",
            "message": "Error creating TICKET. Some required properties were not set.",
            "context": {"requiredProperties": ["foo", "bar"]},
            "errors": [{"context": {"missingRequiredProperties": ["bar", "baz"]}}],
        }
        names = main.extract_hubspot_missing_required_properties(
            payload,
            error_message=payload["message"],
        )
        self.assertEqual(names, ["foo", "bar", "baz"])


if __name__ == "__main__":
    unittest.main()
