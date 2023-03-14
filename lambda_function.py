import requests
import json
import os
import yfinance as yf

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

    # List of cryptocurrencies to update in Notion
    coins = []

    # Iterate over each page in the database and add its Coin property to the coins list
    for result in crypto_database["results"]:
        coin = result["properties"]["Coin"]["select"]["name"]
        coins.append(coin)

    # Remove duplicates from the list of coins
    unique_coins = list(set(coins))

    # Obtener la lista de monedas y sus IDs correspondientes
    coins_list_url = 'https://api.coingecko.com/api/v3/coins/list'
    response = requests.get(coins_list_url)
    coins_list = response.json()

    # Crear un diccionario de pares de valores (símbolo, ID)
    symbol_to_id = {}
    for coin in coins_list:
        if coin['symbol'].upper() in unique_coins:
            match coin['symbol']:
                case 'dai':
                    if coin['id'] != 'dai':
                        continue
                case 'mana':
                    if coin['id'] != 'decentraland':
                        continue
                case 'eth':
                    if coin['id'] != 'ethereum':
                        continue
            symbol_to_id[coin['symbol']] = coin['id']

    # Obtener los precios de cada moneda en unique_coins utilizando sus IDs correspondientes
    coin_prices = {}
    for coin in unique_coins:
        coin_id = symbol_to_id.get(coin.lower(), None)
        if coin_id:
            crypto_url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
            response = requests.get(crypto_url)
            data = response.json()
            price = data.get(coin_id.lower(), {}).get('usd', 0)
            coin_prices[coin] = price

    for result in crypto_database["results"]:
        page_id = result["id"]
        notion_page_url = f"https://api.notion.com/v1/pages/{page_id}"
        coin = result["properties"]["Coin"]["select"]["name"]
        new_price = coin_prices.get(coin, 0)
        result["properties"]["Price"]["number"] = new_price
        properties = result["properties"]
        response_crypto_price_update = requests.patch(notion_page_url, headers=headers, json={"properties": {"Price": properties["Price"]}})

    # STOCKS
    notion_db_url = f"https://api.notion.com/v1/databases/{stock_database_id}/query"
    stock_database = requests.post(notion_db_url, headers=headers).json()
    for result in stock_database["results"]:
        page_id = result["id"]
        notion_page_url = f"https://api.notion.com/v1/pages/{page_id}"
        stock = result["properties"]["Stock"]["select"]["name"]
        new_stock_price = yf.Ticker(stock).history(period='1d')['Close'][0]
        result["properties"]["Price"]["number"] = new_stock_price
        properties = result["properties"]
        response_stock_price_update = requests.patch(notion_page_url, headers=headers, json={"properties": {"Price": properties["Price"]}})

    # Sum total of Fiat + Crypto + Stocks

    sum_fiat = 0
    sum_crypto = 0
    sum_stock = 0
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
        "stock_price_update_response": response_stock_price_update.json(),
        "crypto_price_update_response": response_crypto_price_update.json(),
        "total_sum_update_response": response_total_sum_update.json(),
    }

    status_code = 200
    if response_stock_price_update.status_code != 200 or response_crypto_price_update.status_code != 200 or response_total_sum_update.status_code != 200:
        status_code = 400

    return {
        'statusCode': status_code,
        'body': json.dumps(response)
    }