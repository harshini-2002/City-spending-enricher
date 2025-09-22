
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Dict, Optional, Tuple

import requests

DEFAULT_TIMEOUT = 10  # seconds
USER_AGENT = "city-spending-enricher/1.2"


# ----------------------------- HTTP helpers -----------------------------

def _http_get(url: str, params: Dict[str, Any] | None = None, headers: Dict[str, str] | None = None, *, retries: int = 1) -> Dict[str, Any]:
    """
    GET with ≤10s timeout, tiny retry/backoff, returns parsed JSON or raises.
    """
    backoff = 0.75
    base_headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if headers:
        base_headers.update(headers)
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, headers=base_headers, timeout=DEFAULT_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt >= retries:
                raise
            time.sleep(backoff)
            backoff *= 2.0
    return {}


# ----------------------------- API wrappers -----------------------------

def geocode_city(city: str, country_code: str) -> Tuple[Optional[float], Optional[float]]:
    """
    City → (lat, lon) using Open-Meteo Geocoding.
    """
    url = "https://geocoding-api.open-meteo.com/v1/search"
    data = _http_get(url, {"name": city, "country": country_code, "count": 1})
    results = data.get("results") or []
    if not results:
        return None, None
    r0 = results[0]
    return float(r0.get("latitude")), float(r0.get("longitude"))


def get_current_weather(lat: float, lon: float) -> Tuple[Optional[float], Optional[float]]:
    """
    Current weather by coords → (temperature °C, windspeed m/s).
    """
    url = "https://api.open-meteo.com/v1/forecast"
    data = _http_get(url, {"latitude": lat, "longitude": lon, "current_weather": "true"})
    cw = data.get("current_weather") or {}
    temp_c = cw.get("temperature")
    wind = cw.get("windspeed")
    return (float(temp_c) if temp_c is not None else None,
            float(wind) if wind is not None else None)


def convert_to_usd(local_currency: str, amount_local: Decimal, api_key: Optional[str] = None) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """
    Convert local amount to USD.

    Preference order:
      1) APILayer currencylayer (if api_key given):
         - /convert (direct)
         - /live (USD base) and invert USD{CUR} if /convert not available
      2) exchangerate.host (no key)
         - /convert
         - /latest (compute ourselves)

    Returns (fx_rate_to_usd, amount_usd) or (None, None) if all failed.
    """
    cur = (local_currency or "").upper()

    # Fast path: already USD
    if cur == "USD":
        return Decimal("1"), amount_local.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # ---- 1) APILayer currencylayer (requires access_key in query string) ----
    # Docs: https://currencylayer.com/documentation
    if api_key:
        # 1a) /convert (if plan supports)
        try:
            url = "https://api.currencylayer.com/convert"
            params = {"from": cur, "to": "USD", "amount": str(amount_local), "access_key": api_key}
            data = _http_get(url, params)
            if isinstance(data, dict) and data.get("success"):
                info = data.get("info") or {}
                rate = info.get("rate")
                result_amt = data.get("result")
                if rate is not None and result_amt is not None:
                    rate_dec = Decimal(str(rate))
                    usd_dec = Decimal(str(result_amt)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    return rate_dec, usd_dec
            else:
                err = (data or {}).get("error") if isinstance(data, dict) else None
                if err:
                    print(f"[warn] currencylayer /convert error: {err}", file=sys.stderr)
        except Exception as e:
            print(f"[warn] currencylayer /convert failed: {e}", file=sys.stderr)

        # 1b) /live (USD base) — free plan usually supports this
        #    Returns quotes like {"quotes": {"USDINR": 83.12}}
        #    To convert CUR -> USD, use rate = 1 / USD{CUR}
        try:
            url = "https://api.currencylayer.com/live"
            params = {"access_key": api_key, "currencies": cur}
            data = _http_get(url, params)
            if isinstance(data, dict) and data.get("success"):
                quotes = data.get("quotes") or {}
                key = f"USD{cur}"
                usd_cur = quotes.get(key)
                if usd_cur:
                    rate_dec = Decimal("1") / Decimal(str(usd_cur))
                    usd_dec = (rate_dec * amount_local).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                    return rate_dec, usd_dec
            else:
                err = (data or {}).get("error") if isinstance(data, dict) else None
                if err:
                    print(f"[warn] currencylayer /live error: {err}", file=sys.stderr)
        except Exception as e:
            print(f"[warn] currencylayer /live failed: {e}", file=sys.stderr)

    # ---- 2) exchangerate.host (no key required) ----
    # Docs: https://exchangerate.host/#/ (public)
    try:
        url = "https://api.exchangerate.host/convert"
        params = {"from": cur, "to": "USD", "amount": str(amount_local)}
        data = _http_get(url, params)
        info = data.get("info") or {}
        rate = info.get("rate")
        result_amt = data.get("result")
        if rate is not None and result_amt is not None:
            rate_dec = Decimal(str(rate))
            usd_dec = Decimal(str(result_amt)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            return rate_dec, usd_dec
    except Exception as e:
        print(f"[warn] exchangerate.host /convert failed: {e}", file=sys.stderr)

    try:
        url = "https://api.exchangerate.host/latest"
        params = {"base": cur, "symbols": "USD"}
        data = _http_get(url, params)
        rate = (data.get("rates") or {}).get("USD")
        if rate is not None:
            rate_dec = Decimal(str(rate))
            usd_dec = (rate_dec * amount_local).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            return rate_dec, usd_dec
    except Exception as e:
        print(f"[warn] exchangerate.host /latest failed: {e}", file=sys.stderr)

    return None, None


# ----------------------------- Data model -----------------------------

@dataclass
class EnrichedRow:
    city: str
    country_code: str
    local_currency: str
    amount_local: Decimal
    fx_rate_to_usd: Optional[Decimal]
    amount_usd: Optional[Decimal]
    latitude: Optional[float]
    longitude: Optional[float]
    temperature_c: Optional[float]
    wind_speed_mps: Optional[float]
    retrieved_at: str


# ----------------------------- Core logic -----------------------------

def parse_amount(v: str) -> Decimal:
    try:
        amt = Decimal(v)
        if amt <= 0:
            raise ValueError
        return amt
    except Exception as e:
        raise ValueError(f"Invalid amount '{v}'") from e


def enrich_csv(input_path: str, fx_key: Optional[str] = None) -> list[EnrichedRow]:
    rows: list[EnrichedRow] = []
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = ["city", "country_code", "local_currency", "amount"]
        for k in required:
            if k not in (reader.fieldnames or []):
                raise ValueError(f"Missing required CSV header: {k}")

        for i, r in enumerate(reader, start=1):
            city = (r.get("city") or "").strip()
            country = (r.get("country_code") or "").strip()
            currency = (r.get("local_currency") or "").strip()
            amount_local = parse_amount((r.get("amount") or "").strip())

            lat = lon = None
            temp_c = wind = None
            rate = amt_usd = None

            # Geocode
            try:
                lat, lon = geocode_city(city, country)
            except Exception as e:
                print(f"[warn] geocode failed for row {i} ({city}, {country}): {e}", file=sys.stderr)

            # Weather
            if lat is not None and lon is not None:
                try:
                    temp_c, wind = get_current_weather(lat, lon)
                except Exception as e:
                    print(f"[warn] weather failed for row {i} ({city}): {e}", file=sys.stderr)

            # FX
            try:
                rate, amt_usd = convert_to_usd(currency, amount_local, api_key=fx_key)
            except Exception as e:
                print(f"[warn] fx failed for row {i} ({currency}): {e}", file=sys.stderr)

            rows.append(
                EnrichedRow(
                    city=city,
                    country_code=country,
                    local_currency=currency,
                    amount_local=amount_local,
                    fx_rate_to_usd=rate,
                    amount_usd=amt_usd,
                    latitude=lat,
                    longitude=lon,
                    temperature_c=temp_c,
                    wind_speed_mps=wind,
                    retrieved_at=now_iso,
                )
            )
    return rows


def decimal_default(o: Any) -> Any:
    if isinstance(o, Decimal):
        return float(o)
    return o


# ----------------------------- CLI -----------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="City Spending Enricher (CSV → APIs → JSON)")
    p.add_argument("--input", "-i", default="expenses.csv", help="Path to input CSV (default: expenses.csv)")
    p.add_argument("--output", "-o", default="enriched.json", help="Path to output JSON file")
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON with indentation")
    p.add_argument(
        "--fx-key",
        default=os.getenv("EXCHANGERATE_HOST_KEY"),
        help="API key for currencylayer (optional). If omitted or unauthorized, falls back to exchangerate.host."
    )
    args = p.parse_args(argv)

    rows = enrich_csv(args.input, fx_key=args.fx_key)
    out_list = [asdict(r) for r in rows]

    with open(args.output, "w", encoding="utf-8") as f:
        if args.pretty:
            json.dump(out_list, f, indent=2, default=decimal_default)
            f.write("\n")
        else:
            json.dump(out_list, f, separators=(",", ":"), default=decimal_default)

    print(f"Wrote {len(out_list)} rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())