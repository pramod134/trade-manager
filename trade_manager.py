import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from config import settings
from logger import log
import supabase_client
import tradier_client


def _get_spot_price(spot_row: Optional[Dict[str, Any]]) -> Optional[float]:
    if not spot_row:
        return None
    return spot_row.get("last_price")


def _get_tf_close(spot_row: Optional[Dict[str, Any]], tf: Optional[str]) -> Optional[float]:
    if not spot_row or not tf:
        return None
    tf_closes = spot_row.get("tf_closes") or {}
    tf_row = tf_closes.get(tf)
    if not tf_row:
        return None
    return tf_row.get("close")


def _choose_spot_row(row: Dict[str, Any], price_type: str,
                     spot_under: Optional[Dict[str, Any]],
                     spot_option: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    price_type in {'equity', 'option'}.
    Returns the correct spot row depending on which instrument we want prices from.
    """
    ptype = (price_type or "").lower()
    if ptype == "option":
        return spot_option
    # default: equity/underlying
    return spot_under


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- ENTRY / SL / TP CHECKS ----------


def check_entry(row: Dict[str, Any],
                spot_under: Optional[Dict[str, Any]],
                spot_option: Optional[Dict[str, Any]]) -> Tuple[bool, Optional[float]]:
    """
    Returns (should_enter, entry_price_used)

    entry_cond:
      - 'now' -> use spot price of entry_type instrument
      - 'ca'/'cb' -> use candle close of entry_tf for entry_type instrument
    """
    cond = row.get("entry_cond")
    level = row.get("entry_level")
    entry_type = row.get("entry_type") or "equity"
    entry_tf = row.get("entry_tf")

    spot_row = _choose_spot_row(row, entry_type, spot_under, spot_option)
    if not spot_row:
        return False, None

    # NOW -> tick-based entry
    if cond == "now":
        price = _get_spot_price(spot_row)
        return (price is not None), price

    # CA / CB -> candle-based entry
    if cond in ("ca", "cb"):
        if entry_tf is None:
            return False, None
        price = _get_tf_close(spot_row, entry_tf)
        if price is None or level is None:
            return False, None

        if cond == "ca" and price > level:
            return True, price
        if cond == "cb" and price < level:
            return True, price

    return False, None


def check_sl(row: Dict[str, Any],
             spot_under: Optional[Dict[str, Any]],
             spot_option: Optional[Dict[str, Any]]) -> Tuple[bool, Optional[float]]:
    """
    Returns (sl_hit, sl_price_used)

    sl_cond:
      - 'ca' / 'cb' -> candle-based stop, always uses candle close
      - 'at'       -> tick-based / spot stop
    sl_type:
      - 'equity' / 'option' to choose which instrument's prices to read
    """
    cond = row.get("sl_cond")
    if not cond:
        return False, None

    level = row.get("sl_level")
    sl_type = row.get("sl_type") or "equity"
    sl_tf = row.get("sl_tf") or row.get("entry_tf")

    spot_row = _choose_spot_row(row, sl_type, spot_under, spot_option)
    if not spot_row or level is None:
        return False, None

    # CA / CB -> candle-based SL
    if cond in ("ca", "cb"):
        if sl_tf is None:
            return False, None
        price = _get_tf_close(spot_row, sl_tf)
        if price is None:
            return False, None

        if cond == "cb" and price < level:
            return True, price
        if cond == "ca" and price > level:
            return True, price
        return False, None

    # AT -> tick-based SL
    if cond == "at":
        price = _get_spot_price(spot_row)
        if price is None:
            return False, None
        if price <= level:
            return True, price
        return False, None

    return False, None


def check_tp(row: Dict[str, Any],
             spot_under: Optional[Dict[str, Any]],
             spot_option: Optional[Dict[str, Any]]) -> Tuple[bool, Optional[float]]:
    """
    Returns (tp_hit, tp_price_used)

    TP is touch-based:
      - tp_type='equity' -> use underlying spot
      - tp_type='option' -> use option spot
    """
    level = row.get("tp_level")
    if level is None:
        return False, None

    tp_type = row.get("tp_type") or "equity"
    spot_row = _choose_spot_row(row, tp_type, spot_under, spot_option)
    if not spot_row:
        return False, None

    price = _get_spot_price(spot_row)
    if price is None:
        return False, None

    # For long trades
    if price >= level:
        return True, price

    return False, None


# ---------- MAIN LOOP ----------


def run_trade_manager() -> None:
    log("info", "trade_manager_start", interval=settings.trade_manager_interval)

    while True:
        try:
            rows = supabase_client.fetch_active_trades()
        except Exception as e:
            log("error", "tm_fetch_active_trades_error", error=str(e))
            time.sleep(settings.trade_manager_interval)
            continue

        for row in rows:
            row_id = row["id"]
            manage = row.get("manage")
            status = row.get("status")
            symbol = row.get("symbol")
            occ = row.get("occ")
            asset_type = (row.get("asset_type") or "").lower()
            qty = int(row.get("qty") or 0)

            # Fetch spot rows for underlying + option
            spot_under = None
            spot_option = None
            try:
                if symbol:
                    spot_under = supabase_client.fetch_spot(symbol)
                if occ:
                    spot_option = supabase_client.fetch_spot(occ)
            except Exception as e:
                log("error", "tm_fetch_spot_error", id=row_id, symbol=symbol, occ=occ, error=str(e))
                continue

            # ---------- MANAGE = 'C' (force close) ----------
            if manage == "C":
                log("info", "tm_force_close", id=row_id, symbol=symbol, status=status)

                # If no position yet, just delete the row
                if status == "nt-waiting":
                    try:
                        supabase_client.delete_trade(row_id)
                    except Exception as e:
                        log("error", "tm_force_delete_error", id=row_id, error=str(e))
                    continue

                # If managing, close via broker, record close price if possible
                if status in ("nt-managing", "pos-managing"):
                    if asset_type == "equity":
                        signal_price = _get_spot_price(spot_under)
                        fill_price = tradier_client.place_equity_market(symbol, qty, "sell")
                    else:
                        signal_price = _get_spot_price(spot_option)
                        fill_price = tradier_client.place_option_market(occ, qty, "sell_to_close")

                    # choose best available close price
                    close_price = fill_price if (fill_price is not None) else signal_price
                    if close_price is not None:
                        try:
                            supabase_client.update_executed_trade_close(
                                active_trade_id=row_id,
                                asset_type=asset_type,
                                qty=qty,
                                close_price=close_price,
                                reason="force",
                            )
                        except Exception as e:
                            log("error", "tm_force_executed_update_error", id=row_id, error=str(e))

                    try:
                        supabase_client.delete_trade(row_id)
                    except Exception as e:
                        log("error", "tm_force_delete_error", id=row_id, error=str(e))

                continue  # done with manage='C'

            # ---------- MANAGE = 'Y' ----------
            if manage != "Y":
                continue

            # ---------- STATUS = 'nt-waiting' (entry) ----------
            if status == "nt-waiting":
                should_enter, entry_price = check_entry(row, spot_under, spot_option)
                if not should_enter or entry_price is None:
                    continue

                log("info", "tm_entry_triggered", id=row_id, symbol=symbol, price=entry_price)

                # Place order
                if asset_type == "equity":
                    fill_price = tradier_client.place_equity_market(symbol, qty, "buy")
                else:
                    fill_price = tradier_client.place_option_market(occ, qty, "buy_to_open")

                # choose best approximation for cost basis
                open_price = fill_price if (fill_price is not None) else entry_price

                try:
                    supabase_client.insert_executed_trade_open(row, open_price)
                    supabase_client.mark_as_managing(row_id)
                except Exception as e:
                    log("error", "tm_entry_db_error", id=row_id, error=str(e))

                continue

            # ---------- STATUS = 'nt-managing' (SL / TP) ----------
            if status in ("nt-managing", "pos-managing"):
                # Check SL first
                sl_hit, sl_price_signal = check_sl(row, spot_under, spot_option)
                if sl_hit and sl_price_signal is not None:
                    log("info", "tm_sl_hit", id=row_id, symbol=symbol, price=sl_price_signal)

                    if asset_type == "equity":
                        fill_price = tradier_client.place_equity_market(symbol, qty, "sell")
                    else:
                        fill_price = tradier_client.place_option_market(occ, qty, "sell_to_close")

                    close_price = fill_price if (fill_price is not None) else sl_price_signal

                    if close_price is not None:
                        try:
                            supabase_client.update_executed_trade_close(
                                active_trade_id=row_id,
                                asset_type=asset_type,
                                qty=qty,
                                close_price=close_price,
                                reason="sl",
                            )
                        except Exception as e:
                            log("error", "tm_sl_executed_update_error", id=row_id, error=str(e))

                    try:
                        supabase_client.delete_trade(row_id)
                    except Exception as e:
                        log("error", "tm_sl_delete_error", id=row_id, error=str(e))

                    continue  # done with this trade

                # Then TP
                tp_hit, tp_price_signal = check_tp(row, spot_under, spot_option)
                if tp_hit and tp_price_signal is not None:
                    log("info", "tm_tp_hit", id=row_id, symbol=symbol, price=tp_price_signal)

                    if asset_type == "equity":
                        fill_price = tradier_client.place_equity_market(symbol, qty, "sell")
                    else:
                        fill_price = tradier_client.place_option_market(occ, qty, "sell_to_close")

                    close_price = fill_price if (fill_price is not None) else tp_price_signal

                    if close_price is not None:
                        try:
                            supabase_client.update_executed_trade_close(
                                active_trade_id=row_id,
                                asset_type=asset_type,
                                qty=qty,
                                close_price=close_price,
                                reason="tp",
                            )
                        except Exception as e:
                            log("error", "tm_tp_executed_update_error", id=row_id, error=str(e))

                    try:
                        supabase_client.delete_trade(row_id)
                    except Exception as e:
                        log("error", "tm_tp_delete_error", id=row_id, error=str(e))

                    continue  # done with this trade

        time.sleep(settings.trade_manager_interval)
