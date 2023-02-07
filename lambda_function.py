import requests
import json
import os

def lambda_handler(event, context):

    # Notion Data
    notion_api_key = os.environ['NOTION_API_KEY']
    crypto_database_id = os.environ['CRYPTO_DB_ID']
    fiat_database_id = os.environ['FIAT_DB_ID']
    block_id = os.environ['TOTAL_CALLOUT_BLOCK_ID']
    notion_db_url = f"https://api.notion.com/v1/databases/{crypto_database_id}/query"
    notion_block_url = f"https://api.notion.com/v1/blocks/{block_id}"

    # CoinGecko API endpoint for fetching latest crypto prices
    crypto_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,dai,chainlink,decentraland,the-graph,matic-network&vs_currencies=usd"

    # Get the latest price of 7 cryptocurrencies
    response = requests.get(crypto_url)
    data = response.json()
    coin_prices = {
        "BTC": data["bitcoin"]["usd"],
        "ETH": data["ethereum"]["usd"],
        "DAI": data["dai"]["usd"],
        "LINK": data["chainlink"]["usd"],
        "MANA": data["decentraland"]["usd"],
        "GRT": data["the-graph"]["usd"],
        "MATIC": data["matic-network"]["usd"],
    }

    # Notion headers
    headers = {
        "Authorization": 'Bearer ' + notion_api_key,
        "accept": "application/json",
        "Notion-Version": "2022-06-28",
        "content-type": "application/json"
    }

    # Get the database information
    database = requests.post(notion_db_url, headers=headers).json()

    # List of cryptocurrencies to update in Notion
    coins = ["BTC", "ETH", "DAI", "LINK", "MANA", "GRT", "MATIC"]

    for result in database["results"]:
        page_id = result["id"]
        notion_page_url = f"https://api.notion.com/v1/pages/{page_id}"
        coin = result["properties"]["Coin"]["select"]["name"]
        if coin in coins:
            new_price = coin_prices.get(coin, 0)
            result["properties"]["Price"]["number"] = new_price
            properties = result["properties"]
            response_crypto_price_update = requests.patch(notion_page_url, headers=headers, json={"properties": {"Price": properties["Price"]}})

    # Sum total of Fiat + Crypto

    sum_fiat = 0
    sum_crypto = 0
    crypto_database = requests.post(notion_db_url, headers=headers).json()
    for result in crypto_database["results"]:
        sum_crypto = sum_crypto + result["properties"]["Total"]["formula"]["number"]
    notion_db_url = f"https://api.notion.com/v1/databases/{fiat_database_id}/query"
    fiat_database = requests.post(notion_db_url, headers=headers).json()
    for result in fiat_database["results"]:
        sum_fiat = sum_fiat + result["properties"]["Total"]["number"]
    total = sum_fiat + sum_crypto
    total = round(total, 2)
    block = requests.get(notion_block_url, headers=headers).json()
    block["callout"]["rich_text"][1]["text"]["content"] = f": ${total:.2f}"
    callout = block["callout"]
    response_total_sum_update = requests.patch(notion_block_url, headers=headers, json={"callout": {"rich_text": callout["rich_text"]}})

    response = {
        "crypto_price_update_response": response_crypto_price_update.json(),
        "total_sum_update_response": response_total_sum_update.json()
    }

    status_code = 200
    if response_crypto_price_update.status_code != 200 or response_total_sum_update.status_code != 200:
        status_code = 400

    return {
        'statusCode': status_code,
        'body': json.dumps(response)
    }