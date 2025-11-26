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


def _get_entry_price(row: Dict[str, Any],
                     spot_under: Optional[Dict[str, Any]],
                     spot_option: Optional[Dict[str, Any]]) -> Optional[float]:
    """
    Returns entry price based on the price_type:
      - last: spot last price
      - tf:   time-frame close
    """
    price_type = (row.get("entry_price_type") or "").lower()
    tf = row.get("entry_tf")

    if row.get("asset_type") == "equity":
        if price_type == "tf":
            return _get_tf_close(spot_under, tf)
        return _get_spot_price(spot_under)

    # options
    if price_type == "tf":
        return _get_tf_close(spot_option, tf)
    return _get_spot_price(spot_option)


def _get_sl_level(row: Dict[str, Any]) -> Optional[float]:
    return row.get("sl")


def _get_tp_level(row: Dict[str, Any]) -> Optional[float]:
    return row.get("tp")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- ENTRY / SL / TP CHECKS ----------


def check_entry(row: Dict[str, Any],
                spot_under: Optional[Dict[str, Any]],
                spot_option: Optional[Dict[str, Any]]) -> Tuple[bool, Optional[float]]:
    """
    Returns (should_enter, entry_price_used)
    """
    enabled = row.get("entry_enabled")
    if not enabled:
        return False, None

    cond = (row.get("entry_cond") or "").lower()
    if not cond:
        return False, None

    is_long = row.get("side") == "long"

    price = _get_entry_price(row, spot_under, spot_option)
    if price is None:
        return False, None

    entry = row.get("entry")
    if entry is None:
        return False, None

    # Condition: "touch" -> for long: price <= entry, for short: price >= entry
    if cond == "touch":
        if is_long and price <= entry:
            return True, price
        if (not is_long) and price >= entry:
            return True, price
        return False, None

    # Condition: "candle-close" -> check close based on tf
    if cond == "candle-close":
        # we re-use entry_price logic
        if is_long and price <= entry:
            return True, price
        if (not is_long) and price >= entry:
            return True, price
        return False, None

    # "now" -> enter immediately based on current price
    if cond == "now":
        return True, price

    return False, None


def check_sl(row: Dict[str, Any],
             spot_under: Optional[Dict[str, Any]],
             spot_option: Optional[Dict[str, Any]]) -> Tuple[bool, Optional[float]]:
    """
    Returns (sl_hit, price_used)
    """
    enabled = row.get("sl_enabled")
    if not enabled:
        return False, None

    cond = (row.get("sl_cond") or "").lower()
    if not cond:
        return False, None

    is_long = row.get("side") == "long"
    price_type = (row.get("sl_price_type") or "").lower()
    tf = row.get("sl_tf")
    level = _get_sl_level(row)
    if level is None:
        return False, None

    # Determine which price to use
    if row.get("asset_type") == "equity":
        if price_type == "tf":
            price = _get_tf_close(spot_under, tf)
        else:
            price = _get_spot_price(spot_under)
    else:
        if price_type == "tf":
            price = _get_tf_close(spot_option, tf)
        else:
            price = _get_spot_price(spot_option)

    if price is None:
        return False, None

    # Condition: tick-based SL
    if cond == "at":
        if is_long and price <= level:
            return True, price
        if (not is_long) and price >= level:
            return True, price
        return False, None

    return False, None


def check_tp(row: Dict[str, Any],
             spot_under: Optional[Dict[str, Any]],
             spot_option: Optional[Dict[str, Any]]) -> Tuple[bool, Optional[float]]:
    """
    Returns (tp_hit, price_used)
    """
    enabled = row.get("tp_enabled")
    if not enabled:
        return False, None

    cond = (row.get("tp_cond") or "").lower()
    if not cond:
        return False, None

    is_long = row.get("side") == "long"
    price_type = (row.get("tp_price_type") or "").lower()
    tf = row.get("tp_tf")
    level = _get_tp_level(row)
    if level is None:
        return False, None

    # Determine which price to use
    if row.get("asset_type") == "equity":
        if price_type == "tf":
            price = _get_tf_close(spot_under, tf)
        else:
            price = _get_spot_price(spot_under)
    else:
        if price_type == "tf":
            price = _get_tf_close(spot_option, tf)
        else:
            price = _get_spot_price(spot_option)

    if price is None:
        return False, None

    # Condition: tick-based TP
    if cond == "at":
        if is_long and price >= level:
            return True, price
        if (not is_long) and price <= level:
            return True, price
        return False, None

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

            log(
                "debug",
                "tm_row_context",
                id=row_id,
                symbol=symbol,
                occ=occ,
                manage=manage,
                status=status,
                asset_type=asset_type,
                qty=qty,
            )

            # Fetch spot rows for underlying + option
            spot_under = None
            spot_option = None
            try:
                if symbol:
                    spot_under = supabase_client.fetch_spot(symbol)
                if occ:
                    spot_option = supabase_client.fetch_spot(occ)
            except Exception as e:
                log(
                    "error",
                    "tm_fetch_spot_error",
                    id=row_id,
                    symbol=symbol,
                    occ=occ,
                    error=str(e),
                )
                continue

            log(
                "debug",
                "tm_spot_context",
                id=row_id,
                symbol=symbol,
                under_last=_get_spot_price(spot_under),
                option_last=_get_spot_price(spot_option),
            )

            # ---------- MANAGE = 'C' (force close) ----------
            if manage == "C":
                log(
                    "info",
                    "tm_force_close",
                    id=row_id,
                    symbol=symbol,
                    status=status,
                    asset_type=asset_type,
                    qty=qty,
                )

                # If no position yet, just delete the row (no broker trade existed)
                if status == "nt-waiting":
                    log(
                        "info",
                        "tm_force_close_nt_waiting_delete",
                        id=row_id,
                        symbol=symbol,
                    )
                    try:
                        supabase_client.delete_trade(row_id)
                    except Exception as e:
                        log(
                            "error",
                            "tm_force_delete_error",
                            id=row_id,
                            error=str(e),
                        )
                    continue

                # If managing, close via broker, record close price ONLY if we have a fill
                if status in ("nt-managing", "pos-managing"):
                    if asset_type == "equity":
                        signal_price = _get_spot_price(spot_under)
                        log(
                            "debug",
                            "tm_force_close_place_equity",
                            id=row_id,
                            symbol=symbol,
                            qty=qty,
                            signal_price=signal_price,
                        )
                        fill_price = tradier_client.place_equity_market(
                            symbol, qty, "sell"
                        )
                    else:
                        signal_price = _get_spot_price(spot_option)
                        log(
                            "debug",
                            "tm_force_close_place_option",
                            id=row_id,
                            occ=occ,
                            qty=qty,
                            signal_price=signal_price,
                        )
                        fill_price = tradier_client.place_option_market(
                            occ, qty, "sell_to_close"
                        )

                    log(
                        "debug",
                        "tm_force_close_result",
                        id=row_id,
                        symbol=symbol,
                        occ=occ,
                        fill_price=fill_price,
                        signal_price=signal_price,
                    )

                    # Only treat as closed if we have a confirmed fill price
                    if fill_price is None:
                        log(
                            "error",
                            "tm_force_close_no_fill",
                            id=row_id,
                            symbol=symbol,
                            occ=occ,
                            asset_type=asset_type,
                            qty=qty,
                        )
                    else:
                        close_price = fill_price
                        try:
                            supabase_client.update_executed_trade_close(
                                active_trade_id=row_id,
                                asset_type=asset_type,
                                qty=qty,
                                close_price=close_price,
                                reason="force",
                            )
                        except Exception as e:
                            log(
                                "error",
                                "tm_force_executed_update_error",
                                id=row_id,
                                error=str(e),
                            )

                        try:
                            supabase_client.delete_trade(row_id)
                        except Exception as e:
                            log(
                                "error",
                                "tm_force_delete_error",
                                id=row_id,
                                error=str(e),
                            )

                continue  # done with manage='C'

            # ---------- MANAGE = 'Y' ----------
            if manage != "Y":
                log("debug", "tm_manage_skip", id=row_id, manage=manage)
                continue

            # ---------- STATUS = 'nt-waiting' (entry) ----------
            if status == "nt-waiting":
                should_enter, entry_price = check_entry(
                    row, spot_under, spot_option
                )
                log(
                    "debug",
                    "tm_entry_check",
                    id=row_id,
                    symbol=symbol,
                    should_enter=should_enter,
                    entry_price=entry_price,
                )

                if not should_enter or entry_price is None:
                    continue

                log(
                    "info",
                    "tm_entry_triggered",
                    id=row_id,
                    symbol=symbol,
                    price=entry_price,
                )

                # Place order
                if asset_type == "equity":
                    log(
                        "debug",
                        "tm_entry_place_equity",
                        id=row_id,
                        symbol=symbol,
                        qty=qty,
                    )
                    fill_price = tradier_client.place_equity_market(
                        symbol, qty, "buy"
                    )
                else:
                    log(
                        "debug",
                        "tm_entry_place_option",
                        id=row_id,
                        occ=occ,
                        qty=qty,
                    )
                    fill_price = tradier_client.place_option_market(
                        occ, qty, "buy_to_open"
                    )

                log(
                    "debug",
                    "tm_entry_result",
                    id=row_id,
                    symbol=symbol,
                    occ=occ,
                    entry_price=entry_price,
                    fill_price=fill_price,
                )

                # Only move to managing if we have a confirmed fill
                if fill_price is None:
                    log(
                        "error",
                        "tm_entry_no_fill",
                        id=row_id,
                        symbol=symbol,
                        occ=occ,
                        asset_type=asset_type,
                        qty=qty,
                    )
                    continue

                open_price = fill_price

                try:
                    supabase_client.insert_executed_trade_open(row, open_price)
                    supabase_client.mark_as_managing(row_id)
                    log(
                        "info",
                        "tm_entry_db_update",
                        id=row_id,
                        symbol=symbol,
                        open_price=open_price,
                    )
                except Exception as e:
                    log(
                        "error",
                        "tm_entry_db_error",
                        id=row_id,
                        error=str(e),
                    )

                continue

            # ---------- STATUS = 'nt-managing' / 'pos-managing' (SL / TP) ----------
            if status in ("nt-managing", "pos-managing"):
                # Check SL first
                sl_hit, sl_price_signal = check_sl(
                    row, spot_under, spot_option
                )
                log(
                    "debug",
                    "tm_sl_check",
                    id=row_id,
                    symbol=symbol,
                    sl_hit=sl_hit,
                    sl_price_signal=sl_price_signal,
                )

                if sl_hit and sl_price_signal is not None:
                    log(
                        "info",
                        "tm_sl_hit",
                        id=row_id,
                        symbol=symbol,
                        price=sl_price_signal,
                    )

                    if asset_type == "equity":
                        log(
                            "debug",
                            "tm_sl_place_equity",
                            id=row_id,
                            symbol=symbol,
                            qty=qty,
                        )
                        fill_price = tradier_client.place_equity_market(
                            symbol, qty, "sell"
                        )
                    else:
                        log(
                            "debug",
                            "tm_sl_place_option",
                            id=row_id,
                            occ=occ,
                            qty=qty,
                        )
                        fill_price = tradier_client.place_option_market(
                            occ, qty, "sell_to_close"
                        )

                    log(
                        "debug",
                        "tm_sl_result",
                        id=row_id,
                        symbol=symbol,
                        occ=occ,
                        sl_price_signal=sl_price_signal,
                        fill_price=fill_price,
                    )

                    # Only close if we have a confirmed fill
                    if fill_price is None:
                        log(
                            "error",
                            "tm_sl_no_fill",
                            id=row_id,
                            symbol=symbol,
                            occ=occ,
                            asset_type=asset_type,
                            qty=qty,
                        )
                    else:
                        close_price = fill_price
                        try:
                            supabase_client.update_executed_trade_close(
                                active_trade_id=row_id,
                                asset_type=asset_type,
                                qty=qty,
                                close_price=close_price,
                                reason="sl",
                            )
                        except Exception as e:
                            log(
                                "error",
                                "tm_sl_executed_update_error",
                                id=row_id,
                                error=str(e),
                            )

                        try:
                            supabase_client.delete_trade(row_id)
                        except Exception as e:
                            log(
                                "error",
                                "tm_sl_delete_error",
                                id=row_id,
                                error=str(e),
                            )

                    continue  # done with this trade

                # Then TP
                tp_hit, tp_price_signal = check_tp(
                    row, spot_under, spot_option
                )
                log(
                    "debug",
                    "tm_tp_check",
                    id=row_id,
                    symbol=symbol,
                    tp_hit=tp_hit,
                    tp_price_signal=tp_price_signal,
                )

                if tp_hit and tp_price_signal is not None:
                    log(
                        "info",
                        "tm_tp_hit",
                        id=row_id,
                        symbol=symbol,
                        price=tp_price_signal,
                    )

                    if asset_type == "equity":
                        log(
                            "debug",
                            "tm_tp_place_equity",
                            id=row_id,
                            symbol=symbol,
                            qty=qty,
                        )
                        fill_price = tradier_client.place_equity_market(
                            symbol, qty, "sell"
                        )
                    else:
                        log(
                            "debug",
                            "tm_tp_place_option",
                            id=row_id,
                            occ=occ,
                            qty=qty,
                        )
                        fill_price = tradier_client.place_option_market(
                            occ, qty, "sell_to_close"
                        )

                    log(
                        "debug",
                        "tm_tp_result",
                        id=row_id,
                        symbol=symbol,
                        occ=occ,
                        tp_price_signal=tp_price_signal,
                        fill_price=fill_price,
                    )

                    # Only close if we have a confirmed fill
                    if fill_price is None:
                        log(
                            "error",
                            "tm_tp_no_fill",
                            id=row_id,
                            symbol=symbol,
                            occ=occ,
                            asset_type=asset_type,
                            qty=qty,
                        )
                    else:
                        close_price = fill_price
                        try:
                            supabase_client.update_executed_trade_close(
                                active_trade_id=row_id,
                                asset_type=asset_type,
                                qty=qty,
                                close_price=close_price,
                                reason="tp",
                            )
                        except Exception as e:
                            log(
                                "error",
                                "tm_tp_executed_update_error",
                                id=row_id,
                                error=str(e),
                            )

                        try:
                            supabase_client.delete_trade(row_id)
                        except Exception as e:
                            log(
                                "error",
                                "tm_tp_delete_error",
                                id=row_id,
                                error=str(e),
                            )

                    continue  # done with this trade

        time.sleep(settings.trade_manager_interval)
