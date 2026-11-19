import unittest
from unittest.mock import Mock, patch

from scripts.smoke import assert_masked_customer_events, latency_summary, order_matches, wait_for


class SmokeContracts(unittest.TestCase):
    def test_order_comparison_rejects_a_wrong_status_even_when_amount_matches(self):
        row = {"customer_id": 4, "product": "Test", "amount": "9.50",
               "status": "PENDING", "is_deleted": 0}
        self.assertTrue(order_matches(row, 4, "Test", "9.50", "PENDING", 0))
        self.assertFalse(order_matches(row, 4, "Test", "9.50", "SHIPPED", 0))
        self.assertFalse(order_matches(row, 4, "Test", "9.50", "PENDING", 1))

    def test_customer_event_verifies_before_and_after_and_requires_all_operations(self):
        masked = {"id": 4, "email": "********"}
        unmasked = {"id": 4, "email": "test@example.invalid"}
        events = [
            {"op": "c", "before": None, "after": masked},
            {"op": "u", "before": masked, "after": masked},
            {"op": "d", "before": masked, "after": None},
        ]
        assert_masked_customer_events(events, 4)
        with self.assertRaises(AssertionError):
            assert_masked_customer_events(events[:-1], 4)
        events[1] = {"op": "u", "before": unmasked, "after": masked}
        with self.assertRaises(AssertionError):
            assert_masked_customer_events(events, 4)

    def test_latency_summary_uses_nearest_rank(self):
        self.assertEqual(latency_summary([3, 1, 5, 2, 4]),
                         {"samples": 5, "p50_ms": 3, "p95_ms": 5})

    def test_wait_retries_a_tcp_reset_during_container_startup(self):
        probe = Mock(side_effect=[ConnectionResetError(), True])
        with patch("scripts.smoke.time.sleep"):
            self.assertTrue(wait_for("Connect", probe, bool, timeout=1))
        self.assertEqual(probe.call_count, 2)


if __name__ == "__main__":
    unittest.main()
