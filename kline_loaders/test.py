import requests

# Define the base URL for Bybit's API
base_url = "https://api.bybit.com"

# Define the endpoint for Klines (Candlestick) data
endpoint = "/v2/public/klines"

# Define the symbol, interval, and other parameters
params = {
    "symbol": "BTCUSD",  # Symbol you are interested in (e.g., BTCUSD)
    "interval": "1",    # Interval in minutes (e.g., "1" for 1 minute, "5" for 5 minutes)
    "from": 1632465600,  # Start time in UNIX timestamp
    "limit": 5          # Number of results to fetch
}

# Build the URL
url = f"{base_url}{endpoint}"

# Make the request to Bybit's API
response = requests.get(url, params=params)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON data in the response
    data = response.json()
    # Print the data
    print(data['result'])
else:
    # Print an error message if the request was unsuccessful
    print(f"Failed to fetch data. Response code: {response.status_code}")
