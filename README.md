# Notion Savings

This AWS Lambda Python script runs every hour and updates the prices of cryptocurrencies. It also calculates the total value of your crypto and fiat savings.

## Features

- Updates the prices of specified cryptocurrencies every hour.
- Calculates the total value of your crypto and fiat savings.
- Saves the data to a database or file for future reference.

## Requirements

- AWS Lambda account
- Python 3.x
- Required Python packages (specified in requirements.txt)
- (Optional) `python -m unittest` for tests

## Usage

1. Clone or download the repository to your local machine.
2. Create an AWS Lambda function and upload the code.
3. Set up the required environment variables (e.g. API keys, database connection details).
4. Configure a CloudWatch event to trigger the Lambda function every hour.

## Notes

- The Lambda validates required environment variables at startup and logs a clear error if one is missing.
- Notion database queries are paginated, so totals and updates include all rows.
- HTTP calls use timeouts and retries for transient errors.

## Tests

Run the unit tests locally:

```sh
pytest
```

<img width="1649" alt="image" src="https://github.com/nachochiappe/notion-savings/assets/8737907/a18d98ff-0671-4f2f-b1a8-0d5f49cc14c3">
