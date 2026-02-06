import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import lambda_function


class ParseDataTests(unittest.TestCase):
    def test_parse_data_returns_float(self):
        data = {"Global Quote": {"05. price": "123.45"}}
        self.assertEqual(lambda_function.parse_data(data), 123.45)

    def test_parse_data_returns_none_for_invalid(self):
        data = {"Global Quote": {"05. price": "not-a-number"}}
        self.assertIsNone(lambda_function.parse_data(data))


class QueryNotionDatabaseTests(unittest.TestCase):
    def test_query_notion_database_handles_pagination(self):
        first_page = {
            "results": [{"id": "1"}, {"id": "2"}],
            "has_more": True,
            "next_cursor": "cursor-1",
        }
        second_page = {
            "results": [{"id": "3"}],
            "has_more": False,
            "next_cursor": None,
        }
        with mock.patch(
            "lambda_function.request_json", side_effect=[first_page, second_page]
        ) as request_mock:
            results = lambda_function.query_notion_database(
                "db-id", {"header": "x"}, mock.Mock()
            )

        self.assertEqual([item["id"] for item in results], ["1", "2", "3"])
        self.assertEqual(request_mock.call_count, 2)


class UpdateTotalAssetsCalloutTests(unittest.TestCase):
    def test_update_total_assets_callout_sets_text(self):
        block_response = {
            "callout": {"rich_text": [{"type": "text", "text": {"content": "Total"}}]}
        }
        with mock.patch(
            "lambda_function.request_json", return_value=block_response
        ), mock.patch("lambda_function.request_status", return_value=True) as status_mock:
            lambda_function.update_total_assets_callout(
                "block-id", 42.5, {"h": "v"}, mock.Mock()
            )

        status_mock.assert_called_once()
        payload = status_mock.call_args.kwargs["payload"]
        self.assertEqual(
            payload["callout"]["rich_text"][1]["text"]["content"], ": $42.50"
        )


class FetchCryptoPricesTests(unittest.TestCase):
    def test_fetch_crypto_prices_empty_list(self):
        prices = lambda_function.fetch_crypto_prices([], mock.Mock())
        self.assertEqual(prices, {})


class SelectHelpersTests(unittest.TestCase):
    def test_get_select_name_returns_none_for_missing(self):
        result = {"id": "page-1", "properties": {"Coin": {"select": None}}}
        self.assertIsNone(lambda_function.get_select_name(result, "Coin"))

    def test_update_notion_prices_skips_missing_coin(self):
        result = {
            "id": "page-2",
            "properties": {
                "Coin": {"select": None},
                "Price": {"number": 1.23},
            },
        }
        with mock.patch("lambda_function.request_status") as status_mock:
            lambda_function.update_notion_prices(
                "crypto", [result], {"BTC": 100.0}, {"h": "v"}, mock.Mock()
            )

        status_mock.assert_not_called()


class BuildUpdateJobsTests(unittest.TestCase):
    def test_build_update_jobs_skips_missing_coin(self):
        results = [
            {
                "id": "page-1",
                "properties": {"Coin": {"select": None}, "Price": {"number": 1.0}},
            }
        ]
        jobs = lambda_function.build_update_jobs("crypto", results, {"BTC": 2.0})
        self.assertEqual(jobs, [])

    def test_build_update_jobs_stock_payload(self):
        results = [
            {
                "id": "page-2",
                "properties": {
                    "Stock": {"select": {"name": "AAPL"}},
                    "Price": {"number": 100.0},
                },
            }
        ]
        jobs = lambda_function.build_update_jobs("stock", results, {"AAPL": 123.45})
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["symbol"], "AAPL")
        self.assertEqual(
            jobs[0]["payload"]["properties"]["Price"]["number"],
            123.45,
        )


class UpdateNotionPricesTests(unittest.TestCase):
    @mock.patch.dict(os.environ, {}, clear=True)
    def test_update_notion_prices_uses_defaults(self):
        jobs = [{"symbol": "BTC", "page_id": "page-1", "url": "u", "payload": {}}]
        with mock.patch(
            "lambda_function.build_update_jobs", return_value=jobs
        ), mock.patch("lambda_function.run_notion_updates_concurrently") as run_mock:
            lambda_function.update_notion_prices(
                "crypto", [], {"BTC": 1.0}, {"h": "v"}, mock.Mock()
            )
        self.assertTrue(run_mock.called)
        self.assertEqual(run_mock.call_args.args[3], 4)
        limiter = run_mock.call_args.args[4]
        self.assertAlmostEqual(limiter.interval, 0.4)
        self.assertEqual(limiter.burst, 1)

    def test_update_notion_prices_no_jobs(self):
        with mock.patch(
            "lambda_function.build_update_jobs", return_value=[]
        ), mock.patch("lambda_function.run_notion_updates_concurrently") as run_mock:
            lambda_function.update_notion_prices(
                "crypto", [], {"BTC": 1.0}, {"h": "v"}, mock.Mock()
            )
        run_mock.assert_not_called()


class RateLimiterTests(unittest.TestCase):
    def test_rate_limiter_enforces_interval(self):
        limiter = lambda_function.RateLimiter(rps_limit=2.0, burst=1)
        limiter._next_allowed_at = 0.0
        with mock.patch(
            "lambda_function.time.monotonic", side_effect=[0.0, 0.0, 0.5]
        ), mock.patch("lambda_function.time.sleep") as sleep_mock:
            limiter.wait_for_slot()
            limiter.wait_for_slot()
        sleep_mock.assert_called_once_with(0.5)


class RunNotionUpdatesConcurrentlyTests(unittest.TestCase):
    def test_runner_calls_rate_limited_request_per_job(self):
        jobs = [
            {"symbol": "BTC", "page_id": "p1", "url": "u1", "payload": {"a": 1}},
            {"symbol": "ETH", "page_id": "p2", "url": "u2", "payload": {"a": 2}},
        ]
        with mock.patch(
            "lambda_function.rate_limited_request_status",
            side_effect=[(True, 0.1), (True, 0.1)],
        ) as request_mock:
            outcomes = lambda_function.run_notion_updates_concurrently(
                jobs, {"h": "v"}, mock.Mock(), max_workers=1, limiter=mock.Mock()
            )
        self.assertEqual(request_mock.call_count, 2)
        self.assertEqual(outcomes, {"ok": 2, "fail": 0})

    def test_runner_failure_does_not_abort_batch(self):
        jobs = [
            {"symbol": "BTC", "page_id": "p1", "url": "u1", "payload": {"a": 1}},
            {"symbol": "ETH", "page_id": "p2", "url": "u2", "payload": {"a": 2}},
            {"symbol": "SOL", "page_id": "p3", "url": "u3", "payload": {"a": 3}},
        ]
        with mock.patch(
            "lambda_function.rate_limited_request_status",
            side_effect=[(True, 0.1), (False, 0.2), (True, 0.15)],
        ):
            outcomes = lambda_function.run_notion_updates_concurrently(
                jobs, {"h": "v"}, mock.Mock(), max_workers=2, limiter=mock.Mock()
            )
        self.assertEqual(outcomes, {"ok": 2, "fail": 1})


if __name__ == "__main__":
    unittest.main()
