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
        with (
            mock.patch("lambda_function.request_json", return_value=block_response),
            mock.patch(
                "lambda_function.request_status", return_value=True
            ) as status_mock,
        ):
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


if __name__ == "__main__":
    unittest.main()
