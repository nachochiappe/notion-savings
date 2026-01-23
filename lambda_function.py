import datetime
import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

if os.environ.get('ENVIRONMENT') != 'production':
    from dotenv import load_dotenv
    load_dotenv()

GLOBAL_QUOTE_LITERAL = "Global Quote"
PRICE_LITERAL = "05. price"
INFO_LITERAL = "Information"
DEFAULT_TIMEOUT_SECONDS = 10
COINGECKO_SYMBOL_CACHE = None

def create_session():
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PATCH"],
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def request_json(session, method, url, headers=None, payload=None, params=None):
    try:
        response = session.request(
            method,
            url,
            headers=headers,
            json=payload,
            params=params,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        print(f"Request failed for {url}: {exc}")
        return None

    if response.status_code >= 400:
        print(f"Request failed for {url}: {response.status_code} {response.text}")
        return None

    if response.content:
        return response.json()
    return None

def request_status(session, method, url, headers=None, payload=None):
    try:
        response = session.request(
            method,
            url,
            headers=headers,
            json=payload,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        print(f"Request failed for {url}: {exc}")
        return False

    if response.status_code >= 400:
        print(f"Request failed for {url}: {response.status_code} {response.text}")
        return False
    return True

def get_required_env(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

def fetch_crypto_prices(unique_coins, session):
    if not unique_coins:
        return {}
    coins_list_url = 'https://api.coingecko.com/api/v3/coins/list'
    print("Retrieving coins list from CoinGecko")
    coins_list = get_cached_coins_list(session, coins_list_url)
    if not coins_list:
        return {}
    print("Coins list retrieved successfully")

    symbol_to_id = create_symbol_to_id_mapping(coins_list, unique_coins)

    coin_prices = {}
    coin_ids = [symbol_to_id.get(coin.lower()) for coin in unique_coins if symbol_to_id.get(coin.lower())]
    if coin_ids:
        crypto_price_url = f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(coin_ids)}&vs_currencies=usd"
        print("Retrieving prices from CoinGecko")
        price_data = request_json(session, "GET", crypto_price_url)
        if not price_data:
            print("Failed to retrieve prices from CoinGecko")
            return {}
        print("Prices retrieved successfully from CoinGecko")
        for coin in unique_coins:
            coin_id = symbol_to_id.get(coin.lower())
            if coin_id and coin_id.lower() in price_data:
                usd_price = price_data[coin_id.lower()].get('usd')
                if usd_price is not None:
                    coin_prices[coin] = usd_price
                else:
                    print(f"Warning: USD price not available for {coin}")

    return coin_prices

def get_cached_coins_list(session, coins_list_url):
    global COINGECKO_SYMBOL_CACHE
    if COINGECKO_SYMBOL_CACHE is None:
        COINGECKO_SYMBOL_CACHE = request_json(session, "GET", coins_list_url)
    return COINGECKO_SYMBOL_CACHE

def create_symbol_to_id_mapping(coins_list, unique_coins):
    symbol_to_id = {}
    for coin in coins_list:
        symbol = coin['symbol']
        coin_id = coin['id']
        if symbol.upper() in unique_coins and not (
            (symbol == 'dai' and coin_id != 'dai') or
            (symbol == 'mana' and coin_id != 'decentraland') or
            (symbol == 'eth' and coin_id != 'ethereum') or
            (symbol == 'btc' and coin_id != 'bitcoin') or
            (symbol == 'usdt' and coin_id != 'tether') or
            (symbol == 'bnb' and coin_id != 'binancecoin')
        ):
            symbol_to_id[symbol] = coin_id
    return symbol_to_id

def fetch_stock_prices(unique_stocks, alpha_vantage_api_key, session):
    stock_prices = {'USD': 1.00}  # Initialize with a value for USD
    for stock_symbol in unique_stocks:
        if stock_symbol != 'USD':
            price = get_stock_price(stock_symbol, alpha_vantage_api_key, session)
            if price is not None:
                print("Successfully fetched price for stock " + stock_symbol + ". New price: " + str(price))
                stock_prices[stock_symbol] = price
    return stock_prices

def get_stock_price(stock_symbol, alpha_vantage_api_key, session):
    if stock_symbol == 'CSPX':
        stock_symbol = 'CSPX.LON'  # Adjust the symbol for CSPX
    alpha_vantage_url = build_url(stock_symbol, alpha_vantage_api_key)
    print("Fetching stock price for " + stock_symbol)
    data = request_json(session, "GET", alpha_vantage_url)
    return parse_data(data)

def build_url(stock_symbol, alpha_vantage_api_key):
    return f"https://www.alphavantage.co/query?apikey={alpha_vantage_api_key}&function=GLOBAL_QUOTE&symbol={stock_symbol}"

def parse_data(data):
    if not data:
        return None
    if GLOBAL_QUOTE_LITERAL in data:
        global_quote = data[GLOBAL_QUOTE_LITERAL]
        price = global_quote.get(PRICE_LITERAL)
        try:
            return float(price)
        except (TypeError, ValueError):
            print(f"Unexpected price value: {price}")
            return None
    elif INFO_LITERAL in data:
        print(f"Rate limit exceeded or other information received: {data[INFO_LITERAL]}")
    return None

def update_notion_prices(type, database_results, prices, headers, session):
    for result in database_results:
        page_id = result["id"]
        notion_page_url = f"https://api.notion.com/v1/pages/{page_id}"
        if type == "crypto":
            symbol = result["properties"]["Coin"]["select"]["name"]
        else:
            symbol = result["properties"]["Stock"]["select"]["name"]
        current_price = result["properties"]["Price"]["number"]
        new_price = prices.get(symbol, 0)
        resolved_price = new_price if new_price is not None else current_price
        update_payload = {
            "properties": {
                "Price": {
                    "number": float(resolved_price) if resolved_price is not None else current_price
                }
            }
        }
        print("Updating price in Notion for " + symbol)
        request_status(session, "PATCH", notion_page_url, headers=headers, payload=update_payload)

def query_notion_database(database_id, headers, session):
    notion_db_url = f"https://api.notion.com/v1/databases/{database_id}/query"
    results = []
    payload = {}
    while True:
        database = request_json(session, "POST", notion_db_url, headers=headers, payload=payload)
        if not database:
            break
        results.extend(database.get("results", []))
        if database.get("has_more"):
            payload = {"start_cursor": database.get("next_cursor")}
        else:
            break
    return results

def calculate_total_assets(databases, headers, session):
    print("Calculating total assets")
    total = 0
    for database_id in databases:
        results = query_notion_database(database_id, headers, session)
        for result in results:
            if result["parent"]["database_id"] == os.environ['FIAT_DB_ID']:
                total += result["properties"]["Total"]["number"]
            else:
                total += result["properties"]["Total"]["formula"]["number"]
    return round(total, 2)

def update_total_assets_callout(block_id, total_assets, headers, session):
    notion_block_url = f"https://api.notion.com/v1/blocks/{block_id}"
    block = request_json(session, "GET", notion_block_url, headers=headers)
    if not block:
        return

    callout = block.get("callout")
    if not callout:
        print("Callout block not found in response")
        return

    rich_text = callout.get("rich_text", [])
    if len(rich_text) < 1:
        rich_text.append({"type": "text", "text": {"content": ""}})
    if len(rich_text) < 2:
        rich_text.append({"type": "text", "text": {"content": ""}})

    if "text" not in rich_text[1]:
        rich_text[1]["text"] = {"content": ""}
    rich_text[1]["text"]["content"] = f": ${total_assets:.2f}"
    callout["rich_text"] = rich_text
    print("Updating total assets")
    request_status(session, "PATCH", notion_block_url, headers=headers, payload={"callout": {"rich_text": callout["rich_text"]}})

def lambda_handler(event, context):
    notion_api_key = get_required_env('NOTION_API_KEY')
    alpha_vantage_api_key = get_required_env('ALPHA_VANTAGE_API_KEY')
    crypto_database_id = get_required_env('CRYPTO_DB_ID')
    stock_database_id = get_required_env('STOCK_DB_ID')
    fiat_database_id = get_required_env('FIAT_DB_ID')
    session = create_session()

    headers = {
        "Authorization": 'Bearer ' + notion_api_key,
        "accept": "application/json",
        "Notion-Version": "2022-06-28",
        "content-type": "application/json"
    }

    # CRYPTOCURRENCY PRICES
    print("Getting CRYPTO database information")
    crypto_results = query_notion_database(crypto_database_id, headers, session)
    unique_coins = list(set([result["properties"]["Coin"]["select"]["name"] for result in crypto_results]))
    crypto_prices = fetch_crypto_prices(unique_coins, session)
    if crypto_prices:
        update_notion_prices("crypto", crypto_results, crypto_prices, headers, session)

    # STOCK PRICES
    # Get current UTC hour
    current_utc_hour = datetime.datetime.utcnow().hour

    # Define the hour at which to perform the stock price update (e.g., 11 UTC)
    # This is because the free tier of the API has a limit of 25 requests per day
    stock_update_hour = 11

    # Only execute stock price update if the current hour matches the specified hour
    if current_utc_hour == stock_update_hour:
        stock_results = query_notion_database(stock_database_id, headers, session)
        unique_stocks = list(set([result["properties"]["Stock"]["select"]["name"] for result in stock_results]))
        stock_prices = fetch_stock_prices(unique_stocks, alpha_vantage_api_key, session)
        if stock_prices:
            update_notion_prices("stock", stock_results, stock_prices, headers, session)
    else:
        print("Skipping stock price updates")

    # CALCULATE TOTAL ASSETS
    block_id = get_required_env('TOTAL_CALLOUT_BLOCK_ID')
    total_assets = calculate_total_assets([crypto_database_id, stock_database_id, fiat_database_id], headers, session)
    update_total_assets_callout(block_id, total_assets, headers, session)

# Main execution
if __name__ == "__main__":
    lambda_handler(None, None)
