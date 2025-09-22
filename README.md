# City Spending Enricher

A Python command-line tool that reads a CSV of city expenses, enriches each row with:

- **Geocoding** (latitude/longitude) from [Open-Meteo Geocoding API](https://open-meteo.com/)
- **Current weather** (temperature °C, windspeed m/s) from [Open-Meteo Forecast API](https://open-meteo.com/)
- **FX conversion to USD** using:
  - [APILayer currencylayer](https://currencylayer.com/) if an API key is provided via `--fx-key`
  - Falls back to [exchangerate.host](https://exchangerate.host/) if no key or key fails

The result is written as an enriched JSON file.

---

## Requirements

- Python 3.9+
- [requests](https://pypi.org/project/requests/)

Install dependencies:

```bash
pip install requests
Input CSV

The input file (default: expenses.csv) must have headers:

city,country_code,local_currency,amount
Bengaluru,IN,INR,1250.50
Berlin,DE,EUR,89.90
San Francisco,US,USD,42.00
Tokyo,JP,JPY,3600


city → City name

country_code → ISO-3166-1 alpha-2 country code (e.g., IN, DE, US, JP)

local_currency → ISO 4217 currency code (e.g., INR, EUR, USD, JPY)

amount → Positive decimal number

Usage
Basic usage (no API key, fallback to exchangerate.host)
python city_spending_enricher.py -i expenses.csv -o enriched.json --pretty

With API key (currencylayer)

Pass the key on the command line:

python city_spending_enricher.py -i expenses.csv -o enriched.json --pretty --fx-key "YOUR_API_KEY"


Or set the key once as an environment variable:

Windows PowerShell:

$env:EXCHANGERATE_HOST_KEY="YOUR_API_KEY"
python .\city_spending_enricher.py -i expenses.csv -o enriched.json --pretty

CLI Options

--input, -i → Path to input CSV (default: expenses.csv)

--output, -o → Path to output JSON (default: enriched.json)

--pretty → Pretty-print JSON with indentation

--fx-key → Currencylayer API key (optional, falls back to exchangerate.host if omitted or unauthorized)

Output JSON

Each row in the CSV becomes an object in the output JSON. Example:

[
  {
    "city": "Berlin",
    "country_code": "DE",
    "local_currency": "EUR",
    "amount_local": 89.9,
    "fx_rate_to_usd": 1.07,
    "amount_usd": 96.19,
    "latitude": 52.52437,
    "longitude": 13.41053,
    "temperature_c": 12.3,
    "wind_speed_mps": 3.8,
    "retrieved_at": "2025-09-22T12:34:56Z"
  }
]

Notes

If fx_rate_to_usd or amount_usd is null:

Your API key plan may not support /convert (free currencylayer plans usually only allow /live).

The script will print warnings like:

[warn] currencylayer /convert error: {...}


Weather/geocoding may be null if the city/country lookup fails.
