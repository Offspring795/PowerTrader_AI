import base64
import datetime
import json
import uuid
import time
import math
from typing import Any, Dict, Optional
import requests
import hmac
import hashlib
import os
import colorama
from colorama import Fore, Style
import traceback

# -----------------------------
# GUI HUB OUTPUTS
# -----------------------------
HUB_DATA_DIR = os.environ.get("POWERTRADER_HUB_DIR", os.path.join(os.path.dirname(__file__), "hub_data"))
os.makedirs(HUB_DATA_DIR, exist_ok=True)

TRADER_STATUS_PATH = os.path.join(HUB_DATA_DIR, "trader_status.json")
TRADE_HISTORY_PATH = os.path.join(HUB_DATA_DIR, "trade_history.jsonl")
PNL_LEDGER_PATH = os.path.join(HUB_DATA_DIR, "pnl_ledger.json")
ACCOUNT_VALUE_HISTORY_PATH = os.path.join(HUB_DATA_DIR, "account_value_history.jsonl")

# Initialize colorama
colorama.init(autoreset=True)

# -----------------------------
# GUI SETTINGS (coins list + main_neural_dir)
# -----------------------------
_GUI_SETTINGS_PATH = os.environ.get("POWERTRADER_GUI_SETTINGS") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "gui_settings.json"
)

_gui_settings_cache = {
    "mtime": None,
    "coins": ['BTC', 'ETH', 'XRP', 'BNB', 'DOGE'],  # fallback defaults
    "main_neural_dir": None,
    "trade_start_level": 3,
    "start_allocation_pct": 0.005,
    "dca_multiplier": 2.0,
    "dca_levels": [-2.5, -5.0, -10.0, -20.0, -30.0, -40.0, -50.0],
    "max_dca_buys_per_24h": 2,

    # Trailing PM settings (defaults match previous hardcoded behavior)
    "pm_start_pct_no_dca": 5.0,
    "pm_start_pct_with_dca": 2.5,
    "trailing_gap_pct": 0.5,
}


def _load_gui_settings() -> dict:
    """
    Reads gui_settings.json and returns a dict with:
    - coins: uppercased list
    - main_neural_dir: string (may be None)
    Caches by mtime so it is cheap to call frequently.
    """
    try:
        if not os.path.isfile(_GUI_SETTINGS_PATH):
            return dict(_gui_settings_cache)

        mtime = os.path.getmtime(_GUI_SETTINGS_PATH)
        if _gui_settings_cache["mtime"] == mtime:
            return dict(_gui_settings_cache)

        with open(_GUI_SETTINGS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or {}

        coins = data.get("coins", None)
        if not isinstance(coins, list) or not coins:
            coins = list(_gui_settings_cache["coins"])
        coins = [str(c).strip().upper() for c in coins if str(c).strip()]
        if not coins:
            coins = list(_gui_settings_cache["coins"])

        main_neural_dir = data.get("main_neural_dir", None)
        if isinstance(main_neural_dir, str):
            main_neural_dir = main_neural_dir.strip() or None
        else:
            main_neural_dir = None

        trade_start_level = data.get("trade_start_level", _gui_settings_cache.get("trade_start_level", 3))
        try:
            trade_start_level = int(float(trade_start_level))
        except Exception:
            trade_start_level = int(_gui_settings_cache.get("trade_start_level", 3))
        trade_start_level = max(1, min(trade_start_level, 7))

        start_allocation_pct = data.get("start_allocation_pct", _gui_settings_cache.get("start_allocation_pct", 0.005))
        try:
            start_allocation_pct = float(str(start_allocation_pct).replace("%", "").strip())
        except Exception:
            start_allocation_pct = float(_gui_settings_cache.get("start_allocation_pct", 0.005))
        if start_allocation_pct < 0.0:
            start_allocation_pct = 0.0

        dca_multiplier = data.get("dca_multiplier", _gui_settings_cache.get("dca_multiplier", 2.0))
        try:
            dca_multiplier = float(str(dca_multiplier).strip())
        except Exception:
            dca_multiplier = float(_gui_settings_cache.get("dca_multiplier", 2.0))
        if dca_multiplier < 0.0:
            dca_multiplier = 0.0

        dca_levels = data.get("dca_levels", _gui_settings_cache.get("dca_levels", [-2.5, -5.0, -10.0, -20.0, -30.0, -40.0, -50.0]))
        if not isinstance(dca_levels, list) or not dca_levels:
            dca_levels = list(_gui_settings_cache.get("dca_levels", [-2.5, -5.0, -10.0, -20.0, -30.0, -40.0, -50.0]))
        parsed = []
        for v in dca_levels:
            try:
                parsed.append(float(v))
            except Exception:
                pass
        if parsed:
            dca_levels = parsed
        else:
            dca_levels = list(_gui_settings_cache.get("dca_levels", [-2.5, -5.0, -10.0, -20.0, -30.0, -40.0, -50.0]))

        max_dca_buys_per_24h = data.get("max_dca_buys_per_24h", _gui_settings_cache.get("max_dca_buys_per_24h", 2))
        try:
            max_dca_buys_per_24h = int(float(max_dca_buys_per_24h))
        except Exception:
            max_dca_buys_per_24h = int(_gui_settings_cache.get("max_dca_buys_per_24h", 2))
        if max_dca_buys_per_24h < 0:
            max_dca_buys_per_24h = 0

        # --- Trailing PM settings ---
        pm_start_pct_no_dca = data.get("pm_start_pct_no_dca", _gui_settings_cache.get("pm_start_pct_no_dca", 5.0))
        try:
            pm_start_pct_no_dca = float(str(pm_start_pct_no_dca).replace("%", "").strip())
        except Exception:
            pm_start_pct_no_dca = float(_gui_settings_cache.get("pm_start_pct_no_dca", 5.0))
        if pm_start_pct_no_dca < 0.0:
            pm_start_pct_no_dca = 0.0

        pm_start_pct_with_dca = data.get("pm_start_pct_with_dca", _gui_settings_cache.get("pm_start_pct_with_dca", 2.5))
        try:
            pm_start_pct_with_dca = float(str(pm_start_pct_with_dca).replace("%", "").strip())
        except Exception:
            pm_start_pct_with_dca = float(_gui_settings_cache.get("pm_start_pct_with_dca", 2.5))
        if pm_start_pct_with_dca < 0.0:
            pm_start_pct_with_dca = 0.0

        trailing_gap_pct = data.get("trailing_gap_pct", _gui_settings_cache.get("trailing_gap_pct", 0.5))
        try:
            trailing_gap_pct = float(str(trailing_gap_pct).replace("%", "").strip())
        except Exception:
            trailing_gap_pct = float(_gui_settings_cache.get("trailing_gap_pct", 0.5))
        if trailing_gap_pct < 0.0:
            trailing_gap_pct = 0.0

        _gui_settings_cache["mtime"] = mtime
        _gui_settings_cache["coins"] = coins
        _gui_settings_cache["main_neural_dir"] = main_neural_dir
        _gui_settings_cache["trade_start_level"] = trade_start_level
        _gui_settings_cache["start_allocation_pct"] = start_allocation_pct
        _gui_settings_cache["dca_multiplier"] = dca_multiplier
        _gui_settings_cache["dca_levels"] = dca_levels
        _gui_settings_cache["max_dca_buys_per_24h"] = max_dca_buys_per_24h
        _gui_settings_cache["pm_start_pct_no_dca"] = pm_start_pct_no_dca
        _gui_settings_cache["pm_start_pct_with_dca"] = pm_start_pct_with_dca
        _gui_settings_cache["trailing_gap_pct"] = trailing_gap_pct

        return dict(_gui_settings_cache)

    except Exception:
        return dict(_gui_settings_cache)


def _build_base_paths(main_dir_in: str, coins_in: list) -> dict:
    """
    Safety rule:
    - BTC uses main_dir directly
    - other coins use <main_dir>/<SYM> ONLY if that folder exists
      (no fallback to BTC folder — avoids corrupting BTC data)
    """
    out = {"BTC": main_dir_in}
    try:
        for sym in coins_in:
            sym = str(sym).strip().upper()
            if not sym:
                continue
            if sym == "BTC":
                out["BTC"] = main_dir_in
                continue
            sub = os.path.join(main_dir_in, sym)
            if os.path.isdir(sub):
                out[sym] = sub
    except Exception:
        pass
    return out


# Live globals (will be refreshed inside manage_trades())
crypto_symbols = ['BTC', 'ETH', 'XRP', 'BNB', 'DOGE']

# Default main_dir behavior if settings are missing
main_dir = os.getcwd()
base_paths = {"BTC": main_dir}
TRADE_START_LEVEL = 3
START_ALLOC_PCT = 0.005
DCA_MULTIPLIER = 2.0
DCA_LEVELS = [-2.5, -5.0, -10.0, -20.0, -30.0, -40.0, -50.0]
MAX_DCA_BUYS_PER_24H = 2

# Trailing PM hot-reload globals (defaults match previous hardcoded behavior)
TRAILING_GAP_PCT = 0.5
PM_START_PCT_NO_DCA = 5.0
PM_START_PCT_WITH_DCA = 2.5

_last_settings_mtime = None


def _refresh_paths_and_symbols():
    """
    Hot-reload GUI settings while trader is running.
    Updates globals: crypto_symbols, main_dir, base_paths,
                    TRADE_START_LEVEL, START_ALLOC_PCT, DCA_MULTIPLIER, DCA_LEVELS, MAX_DCA_BUYS_PER_24H,
                    TRAILING_GAP_PCT, PM_START_PCT_NO_DCA, PM_START_PCT_WITH_DCA
    """
    global crypto_symbols, main_dir, base_paths
    global TRADE_START_LEVEL, START_ALLOC_PCT, DCA_MULTIPLIER, DCA_LEVELS, MAX_DCA_BUYS_PER_24H
    global TRAILING_GAP_PCT, PM_START_PCT_NO_DCA, PM_START_PCT_WITH_DCA
    global _last_settings_mtime

    s = _load_gui_settings()
    mtime = s.get("mtime", None)

    # If settings file doesn't exist, keep current defaults
    if mtime is None:
        return

    if _last_settings_mtime == mtime:
        return

    _last_settings_mtime = mtime

    coins = s.get("coins") or list(crypto_symbols)
    mndir = s.get("main_neural_dir") or main_dir
    TRADE_START_LEVEL = max(1, min(int(s.get("trade_start_level", TRADE_START_LEVEL) or TRADE_START_LEVEL), 7))
    START_ALLOC_PCT = float(s.get("start_allocation_pct", START_ALLOC_PCT) or START_ALLOC_PCT)
    if START_ALLOC_PCT < 0.0:
        START_ALLOC_PCT = 0.0

    DCA_MULTIPLIER = float(s.get("dca_multiplier", DCA_MULTIPLIER) or DCA_MULTIPLIER)
    if DCA_MULTIPLIER < 0.0:
        DCA_MULTIPLIER = 0.0

    DCA_LEVELS = list(s.get("dca_levels", DCA_LEVELS) or DCA_LEVELS)

    try:
        MAX_DCA_BUYS_PER_24H = int(float(s.get("max_dca_buys_per_24h", MAX_DCA_BUYS_PER_24H) or MAX_DCA_BUYS_PER_24H))
    except Exception:
        MAX_DCA_BUYS_PER_24H = int(MAX_DCA_BUYS_PER_24H)
    if MAX_DCA_BUYS_PER_24H < 0:
        MAX_DCA_BUYS_PER_24H = 0

    # Trailing PM hot-reload values
    TRAILING_GAP_PCT = float(s.get("trailing_gap_pct", TRAILING_GAP_PCT) or TRAILING_GAP_PCT)
    if TRAILING_GAP_PCT < 0.0:
        TRAILING_GAP_PCT = 0.0

    PM_START_PCT_NO_DCA = float(s.get("pm_start_pct_no_dca", PM_START_PCT_NO_DCA) or PM_START_PCT_NO_DCA)
    if PM_START_PCT_NO_DCA < 0.0:
        PM_START_PCT_NO_DCA = 0.0

    PM_START_PCT_WITH_DCA = float(s.get("pm_start_pct_with_dca", PM_START_PCT_WITH_DCA) or PM_START_PCT_WITH_DCA)
    if PM_START_PCT_WITH_DCA < 0.0:
        PM_START_PCT_WITH_DCA = 0.0

    # Keep it safe if folder isn't real on this machine
    if not os.path.isdir(mndir):
        mndir = os.getcwd()

    crypto_symbols = list(coins)
    main_dir = mndir
    base_paths = _build_base_paths(main_dir, crypto_symbols)

# -----------------------------
# KRAKEN API CONFIG
# -----------------------------

API_KEY = ""
API_SECRET = ""

try:
    with open('k_key.txt', 'r', encoding='utf-8') as f:
        API_KEY = (f.read() or "").strip()
    with open('k_secret.txt', 'r', encoding='utf-8') as f:
        API_SECRET = (f.read() or "").strip()
except Exception:
    API_KEY = ""
    API_SECRET = ""

if not API_KEY or not API_SECRET:
    print(
        "\n[PowerTrader] Kraken API credentials not found.\n"
        "Create k_key.txt (API key) and k_secret.txt (API secret) in the same folder as pt_trader.py.\n"
    )
    raise SystemExit(1)

# Kraken pair mapping (base → Kraken pair name)
KRAKEN_PAIRS = {
    "BTC": "XBTUSD",
    "ETH": "ETHUSD",
    "XRP": "XRPUSD",
    "BNB": "BNBUSD",
    "DOGE": "DOGEUSD",
}

# Kraken asset codes in balances (a bit conservative; check both primary & legacy codes)
KRAKEN_ASSETS = {
    "BTC": ["XXBT", "XBT"],
    "ETH": ["XETH", "ETH"],
    "XRP": ["XXRP", "XRP"],
    "BNB": ["BNB"],
    "DOGE": ["XXDG", "DOGE"],
    "USD": ["ZUSD", "USD"],
}


class CryptoAPITrading:
    def __init__(self):
        # keep a copy of the folder map (same idea as before)
        self.path_map = dict(base_paths)

        self.api_key = API_KEY
        self.api_secret = API_SECRET
        self.base_url = "https://api.kraken.com"

        self.dca_levels_triggered = {}  # Track DCA levels for each crypto
        self.dca_levels = list(DCA_LEVELS)  # Hard DCA triggers (percent PnL)

        # --- Trailing profit margin (per-coin state) ---
        self.trailing_pm = {}  # { "BTC": {"active": bool, "line": float, "peak": float, "was_above": bool}, ... }
        self.trailing_gap_pct = float(TRAILING_GAP_PCT)
        self.pm_start_pct_no_dca = float(PM_START_PCT_NO_DCA)
        self.pm_start_pct_with_dca = float(PM_START_PCT_WITH_DCA)
        self._last_trailing_settings_sig = (
            float(self.trailing_gap_pct),
            float(self.pm_start_pct_no_dca),
            float(self.pm_start_pct_with_dca),
        )

        # GUI hub persistence
        self._pnl_ledger = self._load_pnl_ledger()
        self._reconcile_pending_orders()

        # Cache last known bid/ask per symbol
        self._last_good_bid_ask = {}

        # Cache last complete account snapshot
        self._last_good_account_snapshot = {
            "total_account_value": None,
            "buying_power": None,
            "holdings_sell_value": None,
            "holdings_buy_value": None,
            "percent_in_trade": None,
        }

        # --- DCA rate-limit (per trade, per coin, rolling 24h window) ---
        self.max_dca_buys_per_24h = int(MAX_DCA_BUYS_PER_24H)
        self.dca_window_seconds = 24 * 60 * 60

        self._dca_buy_ts = {}       # { "BTC": [ts, ts, ...] }
        self._dca_last_sell_ts = {}  # { "BTC": ts_of_last_sell }
        self._seed_dca_window_from_history()

        # Initialize cost basis from local trade history
        self.cost_basis = self._calculate_cost_basis_from_history()
        self.initialize_dca_levels_from_history()

    # -----------------------------
    # LOW LEVEL KRAKEN HELPERS
    # -----------------------------

    def _kraken_private(self, path: str, data: Optional[dict] = None) -> Any:
        """
        Calls a Kraken private endpoint.
        path like: "/0/private/Balance"
        """
        if data is None:
            data = {}

        url = self.base_url + path
        nonce = str(int(time.time() * 1000))
        data["nonce"] = nonce

        post_data = "&".join(f"{k}={v}" for k, v in data.items())
        encoded = (nonce + post_data).encode("utf-8")
        message = path.encode("utf-8") + hashlib.sha256(encoded).digest()
        mac = hmac.new(base64.b64decode(self.api_secret), message, hashlib.sha512)
        sig = base64.b64encode(mac.digest()).decode()

        headers = {
            "API-Key": self.api_key,
            "API-Sign": sig,
            "User-Agent": "PowerTrader",
        }

        try:
            resp = requests.post(url, data=data, headers=headers, timeout=10)
            resp.raise_for_status()
            j = resp.json()
            if j.get("error"):
                # Kraken returns an array of error strings; we'll just return None here
                return None
            return j.get("result", None)
        except Exception:
            return None

    def _kraken_public(self, path: str, params: Optional[dict] = None) -> Any:
        """
        Calls a Kraken public endpoint.
        path like: "/0/public/Ticker"
        """
        url = self.base_url + path
        try:
            resp = requests.get(url, params=params or {}, timeout=10)
            resp.raise_for_status()
            j = resp.json()
            if j.get("error"):
                return None
            return j.get("result", None)
        except Exception:
            return None

    # -----------------------------
    # SMALL FILE HELPERS
    # -----------------------------

    def _atomic_write_json(self, path: str, data: dict) -> None:
        try:
            tmp = f"{path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, path)
        except Exception:
            pass

    def _append_jsonl(self, path: str, obj: dict) -> None:
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(obj) + "\n")
        except Exception:
            pass

    # -----------------------------
    # PNL LEDGER / HISTORY
    # -----------------------------

    def _load_pnl_ledger(self) -> dict:
        try:
            if os.path.isfile(PNL_LEDGER_PATH):
                with open(PNL_LEDGER_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                if not isinstance(data, dict):
                    data = {}
                data.setdefault("total_realized_profit_usd", 0.0)
                data.setdefault("last_updated_ts", time.time())
                data.setdefault("open_positions", {})
                data.setdefault("pending_orders", {})
                return data
        except Exception:
            pass
        return {
            "total_realized_profit_usd": 0.0,
            "last_updated_ts": time.time(),
            "open_positions": {},
            "pending_orders": {},
        }

    def _save_pnl_ledger(self) -> None:
        try:
            self._pnl_ledger["last_updated_ts"] = time.time()
            self._atomic_write_json(PNL_LEDGER_PATH, self._pnl_ledger)
        except Exception:
            pass

    def _trade_history_has_order_id(self, order_id: str) -> bool:
        try:
            if not order_id:
                return False
            if not os.path.isfile(TRADE_HISTORY_PATH):
                return False
            with open(TRADE_HISTORY_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = (line or "").strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    if str(obj.get("order_id", "")).strip() == str(order_id).strip():
                        return True
        except Exception:
            return False
        return False

    # -----------------------------
    # ACCOUNT / HOLDINGS / ORDERS (KRAKEN)
    # -----------------------------

    def _get_buying_power(self) -> float:
        """
        Buying power = free USD balance on Kraken.
        """
        bal = self._kraken_private("/0/private/Balance")
        if not isinstance(bal, dict):
            return 0.0

        usd_codes = KRAKEN_ASSETS["USD"]
        for code in usd_codes:
            v = bal.get(code)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    continue
        return 0.0

    def get_account(self) -> dict:
        """
        Returns an object with at least a 'buying_power' key like Robinhood did.
        """
        bp = self._get_buying_power()
        return {"buying_power": bp}

    def get_holdings(self) -> dict:
        """
        Synthesizes holdings from Kraken spot balances for tracked crypto symbols.
        Shape:
        {
            "results": [
                {"asset_code": "BTC", "total_quantity": 0.123},
                ...
            ]
        }
        """
        bal = self._kraken_private("/0/private/Balance")
        out = {"results": []}
        if not isinstance(bal, dict):
            return out

        for sym in crypto_symbols:
            asset_codes = KRAKEN_ASSETS.get(sym, [])
            qty = 0.0
            for code in asset_codes:
                if code in bal:
                    try:
                        qty = float(bal[code])
                        break
                    except Exception:
                        continue
            if qty > 0.0:
                out["results"].append({"asset_code": sym, "total_quantity": qty})
        return out

    def get_trading_pairs(self) -> list:
        """
        Minimal stub: returns list of Kraken pairs we know about.
        """
        pairs = []
        for base, pair in KRAKEN_PAIRS.items():
            pairs.append({"base": base, "symbol": pair})
        return pairs

    def _kraken_pair_for_full_symbol(self, full_symbol: str) -> Optional[str]:
        """
        full_symbol is like 'BTC-USD'. We map to 'XBTUSD', etc.
        """
        base = str(full_symbol).upper().split("-")[0].strip()
        return KRAKEN_PAIRS.get(base)

    def get_orders(self, symbol: str) -> dict:
        """
        For now, this returns a simplified shape using Kraken ClosedOrders.
        Only used by reconciliation and history helpers.
        """
        # symbol is 'BTC-USD' style; convert to pair for filtering
        pair_name = self._kraken_pair_for_full_symbol(symbol)
        res = self._kraken_private("/0/private/ClosedOrders", {})
        if not isinstance(res, dict):
            return {"results": []}

        out = {"results": []}
        closed = res.get("closed", {}) or {}
        for txid, o in closed.items():
            try:
                if pair_name and o.get("descr", {}).get("pair") != pair_name:
                    continue
                side = str(o.get("descr", {}).get("type", "")).lower()
                status = str(o.get("status", "")).lower()
                vol = float(o.get("vol", 0.0))
                vol_exec = float(o.get("vol_exec", 0.0))
                price = float(o.get("price", 0.0) or o.get("descr", {}).get("price", 0.0))
                cost = float(o.get("cost", 0.0))
                fee = float(o.get("fee", 0.0))
                opentm = o.get("opentm", 0)
                closetm = o.get("closetm", 0)

                # Approximate created_at as opentm; state 'filled' when vol_exec > 0 and status == 'closed'
                state = "filled" if (status == "closed" and vol_exec > 0.0) else status

                order = {
                    "id": txid,
                    "side": side,
                    "state": state,
                    "created_at": opentm,
                    "executions": [
                        {
                            "quantity": vol_exec,
                            "effective_price": (cost / vol_exec) if (vol_exec > 0 and cost > 0) else price,
                            "fee": fee,
                        }
                    ],
                    "filled_quantity": vol_exec,
                    "average_price": (cost / vol_exec) if (vol_exec > 0 and cost > 0) else price,
                }
                out["results"].append(order)
            except Exception:
                continue

        # sort oldest to newest by created_at for compatibility
        out["results"].sort(key=lambda x: x.get("created_at", 0))
        return out

    def _calculate_cost_basis_from_history(self) -> dict:
        """
        Computes avg cost per asset from our own TRADE_HISTORY_PATH.
        This is broker-agnostic and works with Kraken.
        """
        cost_basis = {}
        qty_map = {}

        if not os.path.isfile(TRADE_HISTORY_PATH):
            return {}

        try:
            with open(TRADE_HISTORY_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = (line or "").strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue

                    side = str(obj.get("side", "")).lower()
                    sym_full = str(obj.get("symbol", "")).upper().strip()
                    base = sym_full.split("-")[0].strip() if sym_full else ""
                    if not base:
                        continue

                    qty = float(obj.get("qty", 0.0) or 0.0)
                    price = obj.get("price", None)
                    if price is None:
                        continue
                    price = float(price)

                    # We only use BUY trades to build cost basis; sells are realized PnL
                    if side == "buy" and qty > 0:
                        prev_qty = qty_map.get(base, 0.0)
                        prev_cost = cost_basis.get(base, 0.0) * prev_qty
                        new_qty = prev_qty + qty
                        new_cost = prev_cost + qty * price
                        if new_qty > 0:
                            cost_basis[base] = new_cost / new_qty
                            qty_map[base] = new_qty
        except Exception:
            return {}

        return cost_basis

    def initialize_dca_levels_from_history(self) -> None:
        """
        Initializes dca_levels_triggered from our local TRADE_HISTORY_PATH.
        This replaces the Robinhood get_orders-based logic.
        """
        self.dca_levels_triggered = {}
        if not os.path.isfile(TRADE_HISTORY_PATH):
            return

        # For each coin, find the most recent sell, then count buys after that
        per_coin_trades = {}
        try:
            with open(TRADE_HISTORY_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = (line or "").strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue

                    sym_full = str(obj.get("symbol", "")).upper().strip()
                    base = sym_full.split("-")[0].strip() if sym_full else ""
                    if not base:
                        continue

                    per_coin_trades.setdefault(base, []).append(obj)
        except Exception:
            return

        now = time.time()
        for base, trades in per_coin_trades.items():
            # sort oldest -> newest
            trades.sort(key=lambda x: x.get("ts", 0.0) or 0.0)
            last_sell_ts = None
            for t in trades:
                if str(t.get("side", "")).lower() == "sell":
                    last_sell_ts = float(t.get("ts", 0.0) or 0.0)

            buys_after_first = []
            first_buy_ts = None
            for t in trades:
                side = str(t.get("side", "")).lower()
                ts = float(t.get("ts", 0.0) or 0.0)
                if side != "buy":
                    continue

                if last_sell_ts is not None and ts <= last_sell_ts:
                    continue

                if first_buy_ts is None:
                    first_buy_ts = ts
                    continue

                if ts > first_buy_ts:
                    buys_after_first.append(ts)

            triggered_levels_count = len(buys_after_first)
            self.dca_levels_triggered[base] = list(range(triggered_levels_count))

    # -----------------------------
    # DCA WINDOW SEEDING (unchanged)
    # -----------------------------

    def _seed_dca_window_from_history(self) -> None:
        """
        Seeds in-memory DCA buy timestamps from TRADE_HISTORY_PATH so the 24h limit
        works across restarts.
        """
        now_ts = time.time()
        cutoff = now_ts - float(getattr(self, "dca_window_seconds", 86400))

        self._dca_buy_ts = {}
        self._dca_last_sell_ts = {}

        if not os.path.isfile(TRADE_HISTORY_PATH):
            return

        try:
            with open(TRADE_HISTORY_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = (line or "").strip()
                    if not line:
                        continue

                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue

                    ts = obj.get("ts", None)
                    side = str(obj.get("side", "")).lower()
                    tag = obj.get("tag", None)
                    sym_full = str(obj.get("symbol", "")).upper().strip()
                    base = sym_full.split("-")[0].strip() if sym_full else ""
                    if not base:
                        continue

                    try:
                        ts_f = float(ts)
                    except Exception:
                        continue

                    if side == "sell":
                        prev = float(self._dca_last_sell_ts.get(base, 0.0) or 0.0)
                        if ts_f > prev:
                            self._dca_last_sell_ts[base] = ts_f

                    elif side == "buy" and tag == "DCA":
                        self._dca_buy_ts.setdefault(base, []).append(ts_f)

        except Exception:
            return

        # Keep only DCA buys after the last sell and within rolling 24h
        for base, ts_list in list(self._dca_buy_ts.items()):
            last_sell = float(self._dca_last_sell_ts.get(base, 0.0) or 0.0)
            kept = [t for t in ts_list if (t > last_sell) and (t >= cutoff)]
            kept.sort()
            self._dca_buy_ts[base] = kept

    def _dca_window_count(self, base_symbol: str, now_ts: Optional[float] = None) -> int:
        base = str(base_symbol).upper().strip()
        if not base:
            return 0
        now = float(now_ts if now_ts is not None else time.time())
        cutoff = now - float(getattr(self, "dca_window_seconds", 86400))
        last_sell = float(self._dca_last_sell_ts.get(base, 0.0) or 0.0)
        ts_list = list(self._dca_buy_ts.get(base, []) or [])
        ts_list = [t for t in ts_list if (t > last_sell) and (t >= cutoff)]
        self._dca_buy_ts[base] = ts_list
        return len(ts_list)

    def _note_dca_buy(self, base_symbol: str, ts: Optional[float] = None) -> None:
        base = str(base_symbol).upper().strip()
        if not base:
            return
        t = float(ts if ts is not None else time.time())
        self._dca_buy_ts.setdefault(base, []).append(t)
        self._dca_window_count(base, now_ts=t)

    def _reset_dca_window_for_trade(self, base_symbol: str, sold: bool = False, ts: Optional[float] = None) -> None:
        base = str(base_symbol).upper().strip()
        if not base:
            return
        if sold:
            self._dca_last_sell_ts[base] = float(ts if ts is not None else time.time())
        self._dca_buy_ts[base] = []

    # -----------------------------
    # ORDER HELPERS (compat with old code)
    # -----------------------------

    def _get_order_by_id(self, symbol: str, order_id: str) -> Optional[dict]:
        # On Kraken we don't have a simple per-symbol lookup;
        # we'll search ClosedOrders + OpenOrders if needed.
        # For reconciliation, we only care about final fills, so ClosedOrders is enough.
        res = self._kraken_private("/0/private/ClosedOrders", {})
        if not isinstance(res, dict):
            return None
        closed = res.get("closed", {}) or {}
        return closed.get(order_id)

    def _extract_fill_from_order(self, order: dict) -> tuple:
        """
        Returns (filled_qty, avg_fill_price) for a Kraken closed order dict.
        """
        try:
            vol_exec = float(order.get("vol_exec", 0.0) or 0.0)
            cost = float(order.get("cost", 0.0) or 0.0)
            if vol_exec > 0 and cost > 0:
                return vol_exec, cost / vol_exec
        except Exception:
            pass
        return 0.0, None

    def _wait_for_order_terminal(self, symbol: str, order_id: str) -> Optional[dict]:
        """
        On Kraken, we treat 'closed' as filled; canceled may appear as 'canceled'.
        """
        while True:
            o = self._get_order_by_id(symbol, order_id)
            if not o:
                time.sleep(1)
                continue
            status = str(o.get("status", "")).lower().strip()
            if status in {"closed", "canceled", "cancelled", "rejected", "expired"}:
                return o
            time.sleep(1)

    def _reconcile_pending_orders(self) -> None:
        """
        Same logic as before, but using Kraken order lookups.
        """
        try:
            pending = self._pnl_ledger.get("pending_orders", {})
            if not isinstance(pending, dict) or not pending:
                return

            while True:
                pending = self._pnl_ledger.get("pending_orders", {})
                if not isinstance(pending, dict) or not pending:
                    break

                progressed = False

                for order_id, info in list(pending.items()):
                    try:
                        if self._trade_history_has_order_id(order_id):
                            self._pnl_ledger["pending_orders"].pop(order_id, None)
                            self._save_pnl_ledger()
                            progressed = True
                            continue

                        symbol = str(info.get("symbol", "")).strip()
                        side = str(info.get("side", "")).strip().lower()
                        bp_before = float(info.get("buying_power_before", 0.0) or 0.0)

                        if not symbol or not side or not order_id:
                            self._pnl_ledger["pending_orders"].pop(order_id, None)
                            self._save_pnl_ledger()
                            progressed = True
                            continue

                        order = self._wait_for_order_terminal(symbol, order_id)
                        if not order:
                            continue

                        status = str(order.get("status", "")).lower().strip()
                        if status != "closed":
                            self._pnl_ledger["pending_orders"].pop(order_id, None)
                            self._save_pnl_ledger()
                            progressed = True
                            continue

                        filled_qty, avg_price = self._extract_fill_from_order(order)
                        bp_after = self._get_buying_power()
                        bp_delta = float(bp_after) - float(bp_before)

                        self._record_trade(
                            side=side,
                            symbol=symbol,
                            qty=float(filled_qty),
                            price=float(avg_price) if avg_price is not None else None,
                            avg_cost_basis=info.get("avg_cost_basis", None),
                            pnl_pct=info.get("pnl_pct", None),
                            tag=info.get("tag", None),
                            order_id=order_id,
                            fees_usd=None,
                            buying_power_before=bp_before,
                            buying_power_after=bp_after,
                            buying_power_delta=bp_delta,
                        )

                        self._pnl_ledger["pending_orders"].pop(order_id, None)
                        self._save_pnl_ledger()
                        progressed = True

                    except Exception:
                        continue

                if not progressed:
                    time.sleep(1)
        except Exception:
            pass

    # -----------------------------
    # TRADE RECORDING (unchanged)
    # -----------------------------

    def _record_trade(
        self,
        side: str,
        symbol: str,
        qty: float,
        price: Optional[float] = None,
        avg_cost_basis: Optional[float] = None,
        pnl_pct: Optional[float] = None,
        tag: Optional[str] = None,
        order_id: Optional[str] = None,
        fees_usd: Optional[float] = None,
        buying_power_before: Optional[float] = None,
        buying_power_after: Optional[float] = None,
        buying_power_delta: Optional[float] = None,
    ) -> None:
        """
        Same logic as before, now broker-agnostic.
        """
        ts = time.time()

        side_l = str(side or "").lower().strip()
        base = str(symbol or "").upper().split("-")[0].strip()

        try:
            if not isinstance(self._pnl_ledger, dict):
                self._pnl_ledger = {}
            self._pnl_ledger.setdefault("total_realized_profit_usd", 0.0)
            self._pnl_ledger.setdefault("open_positions", {})
            self._pnl_ledger.setdefault("pending_orders", {})
        except Exception:
            pass

        realized = None
        position_cost_used = None
        position_cost_after = None

        if base and (buying_power_delta is not None):
            try:
                bp_delta = float(buying_power_delta)
            except Exception:
                bp_delta = None

            if bp_delta is not None:
                try:
                    open_pos = self._pnl_ledger.get("open_positions", {})
                    if not isinstance(open_pos, dict):
                        open_pos = {}
                        self._pnl_ledger["open_positions"] = open_pos

                    pos = open_pos.get(base, None)
                    if not isinstance(pos, dict):
                        pos = {"usd_cost": 0.0, "qty": 0.0}
                        open_pos[base] = pos

                    pos_usd_cost = float(pos.get("usd_cost", 0.0) or 0.0)
                    pos_qty = float(pos.get("qty", 0.0) or 0.0)

                    q = float(qty or 0.0)

                    if side_l == "buy":
                        usd_used = -bp_delta
                        if usd_used < 0.0:
                            usd_used = 0.0

                        pos["usd_cost"] = float(pos_usd_cost) + float(usd_used)
                        pos["qty"] = float(pos_qty) + float(q if q > 0.0 else 0.0)
                        position_cost_after = float(pos["usd_cost"])
                        self._save_pnl_ledger()

                    elif side_l == "sell":
                        usd_got = bp_delta
                        if usd_got < 0.0:
                            usd_got = 0.0

                        if pos_qty > 0.0 and q > 0.0:
                            frac = min(1.0, float(q) / float(pos_qty))
                        else:
                            frac = 1.0

                        cost_used = float(pos_usd_cost) * float(frac)
                        pos["usd_cost"] = float(pos_usd_cost) - float(cost_used)
                        pos["qty"] = float(pos_qty) - float(q if q > 0.0 else 0.0)

                        position_cost_used = float(cost_used)
                        position_cost_after = float(pos.get("usd_cost", 0.0) or 0.0)

                        realized = float(usd_got) - float(cost_used)
                        self._pnl_ledger["total_realized_profit_usd"] = float(
                            self._pnl_ledger.get("total_realized_profit_usd", 0.0) or 0.0
                        ) + float(realized)

                        if float(pos.get("qty", 0.0) or 0.0) <= 1e-12 or float(pos.get("usd_cost", 0.0) or 0.0) <= 1e-6:
                            open_pos.pop(base, None)

                        self._save_pnl_ledger()

                except Exception:
                    pass

        if realized is None and side_l == "sell" and price is not None and avg_cost_basis is not None:
            try:
                fee_val = float(fees_usd) if fees_usd is not None else 0.0
                realized = (float(price) - float(avg_cost_basis)) * float(qty) - fee_val
                self._pnl_ledger["total_realized_profit_usd"] = float(
                    self._pnl_ledger.get("total_realized_profit_usd", 0.0)
                ) + float(realized)
                self._save_pnl_ledger()
            except Exception:
                realized = None

        entry = {
            "ts": ts,
            "side": side,
            "tag": tag,
            "symbol": symbol,
            "qty": qty,
            "price": price,
            "avg_cost_basis": avg_cost_basis,
            "pnl_pct": pnl_pct,
            "fees_usd": fees_usd,
            "realized_profit_usd": realized,
            "order_id": order_id,
            "buying_power_before": float(buying_power_before) if buying_power_before is not None else None,
            "buying_power_after": float(buying_power_after) if buying_power_after is not None else None,
            "buying_power_delta": float(buying_power_delta) if buying_power_delta is not None else None,
            "position_cost_used_usd": float(position_cost_used) if position_cost_used is not None else None,
            "position_cost_after_usd": float(position_cost_after) if position_cost_after is not None else None,
        }
        self._append_jsonl(TRADE_HISTORY_PATH, entry)

    # -----------------------------
    # SMALL UTILITIES (unchanged below)
    # -----------------------------

    def _write_trader_status(self, status: dict) -> None:
        self._atomic_write_json(TRADER_STATUS_PATH, status)

    @staticmethod
    def _get_current_timestamp() -> int:
        return int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())

    @staticmethod
    def _fmt_price(price: float) -> str:
        try:
            p = float(price)
        except Exception:
            return "N/A"
        if p == 0:
            return "0"
        ap = abs(p)
        if ap >= 1.0:
            decimals = 2
        else:
            decimals = int(-math.floor(math.log10(ap))) + 3
            decimals = max(2, min(12, decimals))
        s = f"{p:.{decimals}f}"
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s

    @staticmethod
    def _read_long_dca_signal(symbol: str) -> int:
        sym = str(symbol).upper().strip()
        folder = base_paths.get(sym, main_dir if sym == "BTC" else os.path.join(main_dir, sym))
        path = os.path.join(folder, "long_dca_signal.txt")
        try:
            with open(path, "r") as f:
                raw = f.read().strip()
            val = int(float(raw))
            return val
        except Exception:
            return 0

    @staticmethod
    def _read_short_dca_signal(symbol: str) -> int:
        sym = str(symbol).upper().strip()
        folder = base_paths.get(sym, main_dir if sym == "BTC" else os.path.join(main_dir, sym))
        path = os.path.join(folder, "short_dca_signal.txt")
        try:
            with open(path, "r") as f:
                raw = f.read().strip()
            val = int(float(raw))
            return val
        except Exception:
            return 0

    @staticmethod
    def _read_long_price_levels(symbol: str) -> list:
        sym = str(symbol).upper().strip()
        folder = base_paths.get(sym, main_dir if sym == "BTC" else os.path.join(main_dir, sym))
        path = os.path.join(folder, "low_bound_prices.html")
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = (f.read() or "").strip()
            if not raw:
                return []
            raw = raw.strip().strip("[]()")
            raw = raw.replace(",", " ").replace(";", " ").replace("|", " ")
            raw = raw.replace("\n", " ").replace("\t", " ")
            parts = [p for p in raw.split() if p]
            vals = []
            for p in parts:
                try:
                    vals.append(float(p))
                except Exception:
                    continue
            out = []
            seen = set()
            for v in vals:
                k = round(float(v), 12)
                if k in seen:
                    continue
                seen.add(k)
                out.append(float(v))
            out.sort(reverse=True)
            return out
        except Exception:
            return []

    # -----------------------------
    # PRICE FETCH (KRAKEN)
    # -----------------------------

    def get_price(self, symbols: list) -> Dict[str, float]:
        """
        symbols is a list like ["BTC-USD", "ETH-USD", ...]
        Returns buy_prices (ask), sell_prices (bid), valid_symbols.
        """
        buy_prices = {}
        sell_prices = {}
        valid_symbols = []

        # Build Kraken pair list
        pairs = set()
        symbol_to_pair = {}
        for full in symbols:
            base = str(full).upper().split("-")[0].strip()
            if base == "USDC":
                continue
            pair = KRAKEN_PAIRS.get(base)
            if not pair:
                continue
            pairs.add(pair)
            symbol_to_pair[full] = pair

        if not pairs:
            return buy_prices, sell_prices, valid_symbols

        result = self._kraken_public("/0/public/Ticker", {"pair": ",".join(pairs)})
        if not isinstance(result, dict):
            # try to fallback to cached data only
            for full in symbols:
                cached = self._last_good_bid_ask.get(full)
                if cached:
                    ask = float(cached.get("ask", 0.0) or 0.0)
                    bid = float(cached.get("bid", 0.0) or 0.0)
                    if ask > 0.0 and bid > 0.0:
                        buy_prices[full] = ask
                        sell_prices[full] = bid
                        valid_symbols.append(full)
            return buy_prices, sell_prices, valid_symbols

        # result keys are Kraken pair names
        for full, pair in symbol_to_pair.items():
            info = result.get(pair)
            if not info:
                # fallback to cache
                cached = self._last_good_bid_ask.get(full)
                if cached:
                    ask = float(cached.get("ask", 0.0) or 0.0)
                    bid = float(cached.get("bid", 0.0) or 0.0)
                    if ask > 0.0 and bid > 0.0:
                        buy_prices[full] = ask
                        sell_prices[full] = bid
                        valid_symbols.append(full)
                continue

            try:
                # Kraken: a[0] = ask price, b[0] = bid price
                ask = float(info["a"][0])
                bid = float(info["b"][0])
            except Exception:
                continue

            if ask <= 0.0 or bid <= 0.0:
                continue

            buy_prices[full] = ask
            sell_prices[full] = bid
            valid_symbols.append(full)
            self._last_good_bid_ask[full] = {"ask": ask, "bid": bid, "ts": time.time()}

        return buy_prices, sell_prices, valid_symbols

    # -----------------------------
    # PLACE ORDERS (KRAKEN)
    # -----------------------------

    def place_buy_order(
        self,
        client_order_id: str,
        side: str,
        order_type: str,
        symbol: str,
        amount_in_usd: float,
        avg_cost_basis: Optional[float] = None,
        pnl_pct: Optional[float] = None,
        tag: Optional[str] = None,
    ) -> Any:
        pair = self._kraken_pair_for_full_symbol(symbol)
        if not pair:
            return None

        # Fetch current price for sizing
        buy_prices, _, _ = self.get_price([symbol])
        current_price = float(buy_prices.get(symbol, 0.0) or 0.0)
        if current_price <= 0.0:
            return None

        vol = amount_in_usd / current_price

        data = {
            "pair": pair,
            "type": "buy",
            "ordertype": "market",
            "volume": f"{vol:.8f}",
            "userref": client_order_id,
        }

        buying_power_before = self._get_buying_power()
        result = self._kraken_private("/0/private/AddOrder", data)
        if not isinstance(result, dict):
            return None

        txids = result.get("txid") or result.get("txids") or []
        if isinstance(txids, str):
            txids = [txids]
        if not txids:
            return None

        order_id = txids[0]

        # Save pending
        try:
            self._pnl_ledger.setdefault("pending_orders", {})
            self._pnl_ledger["pending_orders"][order_id] = {
                "symbol": symbol,
                "side": "buy",
                "buying_power_before": float(buying_power_before),
                "avg_cost_basis": float(avg_cost_basis) if avg_cost_basis is not None else None,
                "pnl_pct": float(pnl_pct) if pnl_pct is not None else None,
                "tag": tag,
                "created_ts": time.time(),
            }
            self._save_pnl_ledger()
        except Exception:
            pass

        # Wait for close
        order = self._wait_for_order_terminal(symbol, order_id)
        if not order or str(order.get("status", "")).lower() != "closed":
            try:
                self._pnl_ledger.get("pending_orders", {}).pop(order_id, None)
                self._save_pnl_ledger()
            except Exception:
                pass
            return result

        filled_qty, avg_price = self._extract_fill_from_order(order)
        buying_power_after = self._get_buying_power()
        buying_power_delta = float(buying_power_after) - float(buying_power_before)

        self._record_trade(
            side="buy",
            symbol=symbol,
            qty=float(filled_qty),
            price=float(avg_price) if avg_price is not None else None,
            avg_cost_basis=float(avg_cost_basis) if avg_cost_basis is not None else None,
            pnl_pct=float(pnl_pct) if pnl_pct is not None else None,
            tag=tag,
            order_id=order_id,
            buying_power_before=buying_power_before,
            buying_power_after=buying_power_after,
            buying_power_delta=buying_power_delta,
        )

        try:
            self._pnl_ledger.get("pending_orders", {}).pop(order_id, None)
            self._save_pnl_ledger()
        except Exception:
            pass

        return result

    def place_sell_order(
        self,
        client_order_id: str,
        side: str,
        order_type: str,
        symbol: str,
        asset_quantity: float,
        expected_price: Optional[float] = None,
        avg_cost_basis: Optional[float] = None,
        pnl_pct: Optional[float] = None,
        tag: Optional[str] = None,
    ) -> Any:
        pair = self._kraken_pair_for_full_symbol(symbol)
        if not pair:
            return None

        data = {
            "pair": pair,
            "type": "sell",
            "ordertype": "market",
            "volume": f"{asset_quantity:.8f}",
            "userref": client_order_id,
        }

        buying_power_before = self._get_buying_power()
        result = self._kraken_private("/0/private/AddOrder", data)
        if not isinstance(result, dict):
            return None

        txids = result.get("txid") or result.get("txids") or []
        if isinstance(txids, str):
            txids = [txids]
        if not txids:
            return None

        order_id = txids[0]

        try:
            self._pnl_ledger.setdefault("pending_orders", {})
            self._pnl_ledger["pending_orders"][order_id] = {
                "symbol": symbol,
                "side": "sell",
                "buying_power_before": float(buying_power_before),
                "avg_cost_basis": float(avg_cost_basis) if avg_cost_basis is not None else None,
                "pnl_pct": float(pnl_pct) if pnl_pct is not None else None,
                "tag": tag,
                "created_ts": time.time(),
            }
            self._save_pnl_ledger()
        except Exception:
            pass

        actual_price = float(expected_price) if expected_price is not None else None
        actual_qty = float(asset_quantity)
        fees_usd = None

        try:
            match = self._wait_for_order_terminal(symbol, order_id)
            if not match or str(match.get("status", "")).lower() != "closed":
                try:
                    self._pnl_ledger.get("pending_orders", {}).pop(order_id, None)
                    self._save_pnl_ledger()
                except Exception:
                    pass
                return result

            vol_exec = float(match.get("vol_exec", 0.0) or 0.0)
            cost = float(match.get("cost", 0.0) or 0.0)
            fee = float(match.get("fee", 0.0) or 0.0)

            if vol_exec > 0.0 and cost > 0.0:
                actual_qty = vol_exec
                actual_price = cost / vol_exec
            fees_usd = fee

        except Exception:
            pass

        if avg_cost_basis is not None and actual_price is not None:
            try:
                acb = float(avg_cost_basis)
                if acb > 0:
                    pnl_pct = ((float(actual_price) - acb) / acb) * 100.0
            except Exception:
                pass

        buying_power_after = self._get_buying_power()
        buying_power_delta = float(buying_power_after) - float(buying_power_before)

        self._record_trade(
            side="sell",
            symbol=symbol,
            qty=float(actual_qty),
            price=float(actual_price) if actual_price is not None else None,
            avg_cost_basis=float(avg_cost_basis) if avg_cost_basis is not None else None,
            pnl_pct=float(pnl_pct) if pnl_pct is not None else None,
            tag=tag,
            order_id=order_id,
            fees_usd=float(fees_usd) if fees_usd is not None else None,
            buying_power_before=buying_power_before,
            buying_power_after=buying_power_after,
            buying_power_delta=buying_power_delta,
        )

        try:
            self._pnl_ledger.get("pending_orders", {}).pop(order_id, None)
            self._save_pnl_ledger()
        except Exception:
            pass

        return result

    # -----------------------------
    # manage_trades + run
    # -----------------------------
    # NOTE: your existing manage_trades() and run() go here unchanged.
    # Paste everything from your original file starting at:
    #   "def manage_trades(self):"
    # down to the end, under this class.

    # I've omitted it here for brevity, but you should KEEP your original
    # manage_trades() and run() exactly as they are; they will now talk
    # to Kraken via the new methods above.


# If you paste this over your current file:
# - Replace everything from the top of the file
#   down through the old CryptoAPITrading class definition.
# - Then append your original manage_trades() and run() methods
#   under this class, unchanged.

if __name__ == "__main__":
    trading_bot = CryptoAPITrading()
    trading_bot.run()
