import httpx

from config import settings
from logger import log


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {settings.tradier_live_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _order_url() -> str:
    # Ensure TRADIER_LIVE_BASE has no trailing /v1
    base = settings.tradier_live_base.rstrip("/")
    return f"{base}/v1/accounts/{settings.tradier_account_id}/orders"


def place_equity_market(symbol: str, qty: int, side: str) -> float | None:
    """
    Place a market order for an equity.
    Returns an approximate fill price (if available) or None.
    """
    url = _order_url()
    payload = {
        "class": "equity",
        "symbol": symbol,
        "side": side,      # "buy" or "sell"
        "quantity": qty,
        "type": "market",
        "duration": "day",
    }

    try:
        with httpx.Client(timeout=8.0) as client:
            resp = client.post(url, headers=_headers(), json=payload)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        log("error", "tradier_equity_order_error", symbol=symbol, qty=qty, side=side, error=str(e))
        return None

    # Try to extract an average fill price if the response has it,
    # otherwise we just return None and the caller can fall back to signal price.
    order = (data or {}).get("order") or {}
    fills = order.get("fills") or []
    if fills:
        # take the first fill's price as an approximation
        price = fills[0].get("price")
        if price is not None:
            try:
                return float(price)
            except Exception:
                pass

    avg = order.get("average_fill_price")
    if avg is not None:
        try:
            return float(avg)
        except Exception:
            pass

    return None


def place_option_market(occ: str, qty: int, side: str) -> float | None:
    """
    Place a market order for an option.
    occ may be stored as "O:AMD250919C00160000" or without "O:".
    Tradier expects the OCC symbol without the "O:" prefix.
    Returns an approximate fill price or None.
    """
    symbol = occ[2:] if occ and occ.startswith("O:") else occ

    url = _order_url()
    payload = {
        "class": "option",
        "symbol": symbol,
        "side": side,  # "buy_to_open" or "sell_to_close"
        "quantity": qty,
        "type": "market",
        "duration": "day",
    }

    try:
        with httpx.Client(timeout=8.0) as client:
            resp = client.post(url, headers=_headers(), json=payload)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        log("error", "tradier_option_order_error", occ=occ, qty=qty, side=side, error=str(e))
        return None

    order = (data or {}).get("order") or {}
    fills = order.get("fills") or []
    if fills:
        price = fills[0].get("price")
        if price is not None:
            try:
                return float(price)
            except Exception:
                pass

    avg = order.get("average_fill_price")
    if avg is not None:
        try:
            return float(avg)
        except Exception:
            pass

    return None
