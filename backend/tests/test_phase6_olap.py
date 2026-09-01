"""
Phase 6 OLAP Engine & Data Cube Unit Tests
"""

import unittest
from backend.app.services.olap_service import OLAPEngine

class TestPhase6OLAPEngine(unittest.TestCase):

    def test_01_cube_metadata(self):
        meta = OLAPEngine.get_metadata()
        self.assertEqual(meta["cube_name"], "cloud_cost_cube")
        self.assertIn("rollup", meta["operations"])
        self.assertIn("drilldown", meta["operations"])
        self.assertIn("slice", meta["operations"])
        self.assertIn("dice", meta["operations"])
        self.assertIn("pivot", meta["operations"])

    def test_02_rollup_day_to_month(self):
        res = OLAPEngine.rollup(dimension="time", level="month", measure="net_cost")
        self.assertEqual(res["operation"], "ROLLUP")
        self.assertGreaterEqual(len(res["data"]), 1)
        self.assertIn("interpretation", res)

    def test_03_rollup_month_to_year(self):
        res = OLAPEngine.rollup(dimension="time", level="year", measure="net_cost")
        self.assertEqual(res["operation"], "ROLLUP")
        self.assertGreaterEqual(len(res["data"]), 1)

    def test_04_drilldown_year_to_month(self):
        res = OLAPEngine.drilldown(hierarchy="time", current_level="year", next_level="month_name")
        self.assertEqual(res["operation"], "DRILLDOWN")
        self.assertGreaterEqual(len(res["data"]), 1)

    def test_05_drilldown_provider_account_project(self):
        res = OLAPEngine.drilldown(hierarchy="cloud", current_level="AWS", next_level="account_id")
        self.assertEqual(res["operation"], "DRILLDOWN")
        self.assertGreaterEqual(len(res["data"]), 1)

    def test_06_slice_aws(self):
        res = OLAPEngine.slice(dimension="cloud_provider", value="AWS", measure="net_cost")
        self.assertEqual(res["operation"], "SLICE")
        self.assertEqual(res["value"], "AWS")
        self.assertGreaterEqual(res["record_count"], 1)

    def test_07_slice_engineering(self):
        res = OLAPEngine.slice(dimension="department", value="Engineering", measure="net_cost")
        self.assertEqual(res["operation"], "SLICE")
        self.assertEqual(res["value"], "Engineering")

    def test_08_dice_multi_filter(self):
        filters = {
            "cloud_provider": ["AWS", "Azure"],
            "environment": ["production"]
        }
        res = OLAPEngine.dice(filters=filters)
        self.assertEqual(res["operation"], "DICE")
        self.assertIn("measures_summary", res)

    def test_09_pivot_matrix(self):
        res = OLAPEngine.pivot(rows="department", columns="cloud_provider", measure="net_cost")
        self.assertEqual(res["operation"], "PIVOT")
        self.assertIn("matrix", res)

    def test_10_top_n_projects(self):
        res = OLAPEngine.get_top_n(category="projects", n=5)
        self.assertEqual(res["category"], "projects")
        self.assertLessEqual(len(res["data"]), 5)

    def test_11_time_series_monthly(self):
        res = OLAPEngine.get_time_series(granularity="monthly")
        self.assertEqual(res["granularity"], "monthly")
        self.assertGreaterEqual(len(res["data"]), 1)

    def test_12_budget_analysis(self):
        res = OLAPEngine.get_budget_analysis()
        self.assertIn("total_budget", res)
        self.assertIn("actual_cost", res)
        self.assertIn("department_budget_breakdown", res)

    def test_13_savings_analysis(self):
        res = OLAPEngine.get_savings_analysis()
        self.assertIn("total_savings", res)
        self.assertIn("effective_discount_pct", res)

    def test_14_anomaly_analysis(self):
        res = OLAPEngine.get_anomaly_analysis()
        self.assertIn("total_records", res)
        self.assertIn("anomalous_records", res)

    def test_15_security_injection_rejection(self):
        with self.assertRaises(ValueError):
            OLAPEngine.rollup(dimension="SELECT * FROM users", level="month")
        with self.assertRaises(ValueError):
            OLAPEngine.rollup(dimension="time", level="month", measure="DROP TABLE fact_cloud_cost")

if __name__ == '__main__':
    unittest.main()
