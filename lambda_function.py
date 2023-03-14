import requests
import json
import os

def lambda_handler(event, context):

    # Notion Data
    notion_api_key = os.environ['NOTION_API_KEY']
    crypto_database_id = os.environ['CRYPTO_DB_ID']
    fiat_database_id = os.environ['FIAT_DB_ID']
    stock_database_id = os.environ['STOCK_DB_ID']
    block_id = os.environ['TOTAL_CALLOUT_BLOCK_ID']
    notion_db_url = f"https://api.notion.com/v1/databases/{crypto_database_id}/query"
    notion_block_url = f"https://api.notion.com/v1/blocks/{block_id}"

    # Notion headers
    headers = {
        "Authorization": 'Bearer ' + notion_api_key,
        "accept": "application/json",
        "Notion-Version": "2022-06-28",
        "content-type": "application/json"
    }

    # Get the database information
    crypto_database = requests.post(notion_db_url, headers=headers).json()

    # Iterate over each page in the database and add its Coin property to the coins list and remove duplicates from the list of coins
    unique_coins = list(set([result["properties"]["Coin"]["select"]["name"] for result in crypto_database["results"]]))

    # Get the list of coins and their corresponding IDs
    coins_list_url = 'https://api.coingecko.com/api/v3/coins/list'
    coins_list = requests.get(coins_list_url).json()

    # Create a dictionary of symbol to ID value pairs
    symbol_to_id = {coin['symbol']: coin['id'] for coin in coins_list
                    if coin['symbol'].upper() in unique_coins and 
                    not ((coin['symbol'] == 'dai' and coin['id'] != 'dai') or 
                        (coin['symbol'] == 'mana' and coin['id'] != 'decentraland') or 
                        (coin['symbol'] == 'eth' and coin['id'] != 'ethereum'))}

    # Initialize an empty dictionary to store coin prices
    coin_prices = {}
    
    # Get the prices of each coin in unique_coins using their corresponding IDs
    coin_ids = [symbol_to_id.get(coin.lower()) for coin in unique_coins if symbol_to_id.get(coin.lower())]
    if coin_ids:
        crypto_url = f"https://api.coingecko.com/api/v3/simple/price?ids={','.join(coin_ids)}&vs_currencies=usd"
        response = requests.get(crypto_url)
        data = response.json()
        for coin, coin_id in zip(unique_coins, coin_ids):
            price = data[coin_id.lower()]['usd'] if coin_id.lower() in data else 0
            coin_prices[coin] = price

    for result in crypto_database["results"]:
        page_id = result["id"]
        notion_page_url = f"https://api.notion.com/v1/pages/{page_id}"
        coin = result["properties"]["Coin"]["select"]["name"]
        new_price = coin_prices.get(coin, 0)
        result["properties"]["Price"]["number"] = new_price
        properties = result["properties"]
        response_crypto_price_update = requests.patch(notion_page_url, headers=headers, json={"properties": {"Price": properties["Price"]}})

    # Sum total of Fiat + Crypto + Stocks

    sum_fiat = 0
    sum_crypto = 0
    sum_stock = 0
    notion_db_url = f"https://api.notion.com/v1/databases/{stock_database_id}/query"
    stock_database = requests.post(notion_db_url, headers=headers).json()
    for result in stock_database["results"]:
        sum_stock = sum_stock + result["properties"]["Total"]["formula"]["number"]
    notion_db_url = f"https://api.notion.com/v1/databases/{crypto_database_id}/query"
    crypto_database = requests.post(notion_db_url, headers=headers).json()
    for result in crypto_database["results"]:
        sum_crypto = sum_crypto + result["properties"]["Total"]["formula"]["number"]
    notion_db_url = f"https://api.notion.com/v1/databases/{fiat_database_id}/query"
    fiat_database = requests.post(notion_db_url, headers=headers).json()
    for result in fiat_database["results"]:
        sum_fiat = sum_fiat + result["properties"]["Total"]["number"]
    total = sum_fiat + sum_crypto + sum_stock
    total = round(total, 2)
    block = requests.get(notion_block_url, headers=headers).json()
    block["callout"]["rich_text"][1]["text"]["content"] = f": ${total:.2f}"
    callout = block["callout"]
    response_total_sum_update = requests.patch(notion_block_url, headers=headers, json={"callout": {"rich_text": callout["rich_text"]}})

    response = {
        "crypto_price_update_response": response_crypto_price_update.json(),
        "total_sum_update_response": response_total_sum_update.json(),
    }

    status_code = 200
    if response_crypto_price_update.status_code != 200 or response_total_sum_update.status_code != 200:
        status_code = 400

    return {
        'statusCode': status_code,
        'body': json.dumps(response)
    }