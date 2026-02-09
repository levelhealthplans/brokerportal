import unittest
from pathlib import Path
import sys

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main  # noqa: E402


class NetworkAssignmentTests(unittest.TestCase):
    def test_known_mapping_examples_and_default_fallback(self) -> None:
        mapping = main.load_network_mapping(BACKEND_DIR / "data" / "network_mappings.csv")
        rows = [
            {"zip": "63011"},
            {"zip": "45202"},
            {"zip": "99999"},
        ]
        result = main.compute_network_assignment(
            rows=rows,
            zip_header="zip",
            mapping=mapping,
            default_network="Cigna_PPO",
            coverage_threshold=0.90,
        )
        assigned = {row["zip"]: row["assigned_network"] for row in result["member_assignments"]}
        self.assertEqual(assigned["63011"], "Mercy_MO")
        self.assertEqual(assigned["45202"], "H2B_OH")
        self.assertEqual(assigned["99999"], "Cigna_PPO")

    def test_single_direct_contract_meets_threshold(self) -> None:
        mapping = {"63011": "Mercy_MO"}
        rows = [{"zip": "63011"} for _ in range(9)] + [{"zip": "99999"}]
        result = main.compute_network_assignment(
            rows=rows,
            zip_header="zip",
            mapping=mapping,
            default_network="Cigna_PPO",
            coverage_threshold=0.90,
        )
        summary = result["group_summary"]
        self.assertEqual(summary["primary_network"], "Mercy_MO")
        self.assertAlmostEqual(summary["coverage_percentage"], 0.9)
        self.assertFalse(summary["fallback_used"])
        self.assertFalse(summary["review_required"])

    def test_mixed_network_requires_manual_review(self) -> None:
        mapping = {"11111": "Mercy_MO", "22222": "H2B_OH"}
        rows = [{"zip": "11111"} for _ in range(5)] + [{"zip": "22222"} for _ in range(5)]
        result = main.compute_network_assignment(
            rows=rows,
            zip_header="zip",
            mapping=mapping,
            default_network="Cigna_PPO",
            coverage_threshold=0.90,
        )
        summary = result["group_summary"]
        self.assertEqual(summary["primary_network"], "MIXED_NETWORK")
        self.assertTrue(summary["review_required"])
        self.assertEqual(summary["coverage_percentage"], 0)

    def test_exactly_forty_percent_each_is_not_mixed(self) -> None:
        mapping = {"11111": "Mercy_MO", "22222": "H2B_OH"}
        rows = (
            [{"zip": "11111"} for _ in range(4)]
            + [{"zip": "22222"} for _ in range(4)]
            + [{"zip": "99999"} for _ in range(2)]
        )
        result = main.compute_network_assignment(
            rows=rows,
            zip_header="zip",
            mapping=mapping,
            default_network="Cigna_PPO",
            coverage_threshold=0.90,
        )
        summary = result["group_summary"]
        self.assertEqual(summary["primary_network"], "Cigna_PPO")
        self.assertFalse(summary["review_required"])
        self.assertTrue(summary["fallback_used"])

    def test_invalid_zips_are_excluded_and_flagged_incomplete(self) -> None:
        mapping = {"12345": "Mercy_MO"}
        rows = [
            {"zip": "12345"},
            {"zip": "", "member_id": "2"},
            {"zip": "ABCDE", "member_id": "3"},
            {"zip": "1234", "member_id": "4"},
        ]
        result = main.compute_network_assignment(
            rows=rows,
            zip_header="zip",
            mapping=mapping,
            default_network="Cigna_PPO",
            coverage_threshold=0.90,
        )
        summary = result["group_summary"]
        self.assertEqual(summary["total_members"], 1)
        self.assertTrue(summary["census_incomplete"])
        self.assertTrue(summary["review_required"])
        self.assertEqual(len(summary["invalid_rows"]), 3)


if __name__ == "__main__":
    unittest.main()
