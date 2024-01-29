import requests
import os
import datetime

if os.environ.get('ENVIRONMENT') != 'production':
    from dotenv import load_dotenv
    load_dotenv()

def fetch_crypto_prices(unique_coins):
    coins_list_url = 'https://api.coingecko.com/api/v3/coins/list'
    coins_list_response = requests.get(coins_list_url)
    if coins_list_response.status_code != 200:
        print("Failed to retrieve coins list from CoinGecko")
        return {}

    coins_list = coins_list_response.json()

    symbol_to_id = create_symbol_to_id_mapping(coins_list, unique_coins)

    coin_prices = {}
    coin_ids = [symbol_to_id.get(coin.lower()) for coin in unique_coins if symbol_to_id.get(coin.lower())]
    if coin_ids:
        crypto_price_url = f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(coin_ids)}&vs_currencies=usd"
        crypto_price_response = requests.get(crypto_price_url)
        if crypto_price_response.status_code != 200:
            print("Failed to retrieve prices from CoinGecko")
            return {}

        price_data = crypto_price_response.json()
        for coin in unique_coins:
            coin_id = symbol_to_id.get(coin.lower())
            if coin_id and coin_id.lower() in price_data:
                coin_prices[coin] = price_data[coin_id.lower()]['usd']

    return coin_prices

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
            (symbol == 'usdt' and coin_id != 'tether')
        ):
            symbol_to_id[symbol] = coin_id
    return symbol_to_id

def fetch_stock_prices(unique_stocks, alpha_vantage_api_key):
    stock_prices = {}
    for stock_symbol in unique_stocks:
        if stock_symbol != 'USD':
            alpha_vantage_url = f"https://www.alphavantage.co/query?apikey={alpha_vantage_api_key}&function=GLOBAL_QUOTE&symbol={stock_symbol}"
            response = requests.get(alpha_vantage_url)
            if response.status_code == 200:
                data = response.json()
                # Check if the response contains the expected 'Global Quote' data
                if "Global Quote" in data:
                    latest_price = data["Global Quote"]["05. price"]
                    stock_prices[stock_symbol] = latest_price
                elif "Information" in data:
                    # Handle the case where rate limit message is received
                    print(f"Rate limit exceeded or other information received: {data['Information']}")
                    break  # Optionally break out of the loop if rate limit is hit
            else:
                print(f"Failed to retrieve data for {stock_symbol}")
    return stock_prices


def update_notion_prices(type, database_results, prices, headers):
    for result in database_results:
        page_id = result["id"]
        notion_page_url = f"https://api.notion.com/v1/pages/{page_id}"
        if type == "crypto":
            symbol = result["properties"]["Coin"]["select"]["name"]
        else:
            symbol = result["properties"]["Stock"]["select"]["name"]
        new_price = prices.get(symbol, 0)
        update_payload = {
            "properties": {
                "Price": {
                    "number": float(new_price) if new_price else None
                }
            }
        }
        requests.patch(notion_page_url, headers=headers, json=update_payload)

def calculate_total_assets(databases, headers):
    total = 0
    for database_id in databases:
        notion_db_url = f"https://api.notion.com/v1/databases/{database_id}/query"
        database = requests.post(notion_db_url, headers=headers).json()
        for result in database["results"]:
            if result["parent"]["database_id"] == os.environ['FIAT_DB_ID']:
                total += result["properties"]["Total"]["number"]
            else:
                total += result["properties"]["Total"]["formula"]["number"]
    return round(total, 2)

def lambda_handler(event, context):
    notion_api_key = os.environ['NOTION_API_KEY']
    alpha_vantage_api_key = os.environ['ALPHA_VANTAGE_API_KEY']
    crypto_database_id = os.environ['CRYPTO_DB_ID']
    stock_database_id = os.environ['STOCK_DB_ID']
    fiat_database_id = os.environ['FIAT_DB_ID']

    headers = {
        "Authorization": 'Bearer ' + notion_api_key,
        "accept": "application/json",
        "Notion-Version": "2022-06-28",
        "content-type": "application/json"
    }

    # CRYPTOCURRENCY PRICES
    notion_db_url = f"https://api.notion.com/v1/databases/{crypto_database_id}/query"
    crypto_database = requests.post(notion_db_url, headers=headers).json()
    unique_coins = list(set([result["properties"]["Coin"]["select"]["name"] for result in crypto_database["results"]]))
    crypto_prices = fetch_crypto_prices(unique_coins)
    if crypto_prices:
        update_notion_prices("crypto", crypto_database["results"], crypto_prices, headers)

    # STOCK PRICES
    # Get current UTC hour
    current_utc_hour = datetime.datetime.utcnow().hour

    # Define the hour at which to perform the stock price update (e.g., 11 UTC)
    stock_update_hour = 11

    # Only execute stock price update if the current hour matches the specified hour
    if current_utc_hour == stock_update_hour:
        notion_db_url = f"https://api.notion.com/v1/databases/{stock_database_id}/query"
        stock_database = requests.post(notion_db_url, headers=headers).json()
        unique_stocks = list(set([result["properties"]["Stock"]["select"]["name"] for result in stock_database["results"]]))
        stock_prices = fetch_stock_prices(unique_stocks, alpha_vantage_api_key)
        if stock_prices:
            update_notion_prices("stock", stock_database["results"], stock_prices, headers)

    # CALCULATE TOTAL ASSETS
    block_id = os.environ['TOTAL_CALLOUT_BLOCK_ID']
    notion_block_url = f"https://api.notion.com/v1/blocks/{block_id}"
    total_assets = calculate_total_assets([crypto_database_id, stock_database_id, fiat_database_id], headers)
    block = requests.get(notion_block_url, headers=headers).json()
    block["callout"]["rich_text"][1]["text"]["content"] = f": ${total_assets:.2f}"
    callout = block["callout"]
    requests.patch(notion_block_url, headers=headers, json={"callout": {"rich_text": callout["rich_text"]}})

# Main execution
if __name__ == "__main__":
    lambda_handler(None, None)