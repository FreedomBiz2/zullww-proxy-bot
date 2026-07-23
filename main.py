from __future__ import annotations

import dataclasses
import html
import json
import os
import re
import tempfile
import threading
from pathlib import Path
from typing import Optional

import requests
import telebot


TOKEN_ENV = "TELEGRAM_BOT_TOKEN"
PROXY_LIST_ENV = "PROXY_LIST"
STOCK_FILE = Path(os.getenv("PROXY_STOCK_FILE", "data/proxy_stock.json"))
PROXY_SEPARATOR_RE = re.compile(r"[\s,;|]+")

# Timeout in seconds for each proxy liveness check
PROXY_CHECK_TIMEOUT = 7

# Endpoints tried in order; first success wins
CHECK_URLS = [
    "https://ipinfo.io/json",
    "https://api.ipify.org?format=json",
]


# ---------------------------------------------------------------------------
# Proxy string helpers
# ---------------------------------------------------------------------------


def split_proxy_text(raw_proxy_list: str) -> list[str]:
    """Split proxy text even when newlines were flattened into spaces."""
    normalized = re.sub(
        r"\\+([rR][nN]|[nN]|[rR])",
        lambda m: "\n" if m.group(1).lower() in {"n", "r", "rn"} else m.group(0),
        raw_proxy_list,
    )
    values = PROXY_SEPARATOR_RE.split(normalized)
    proxies: list[str] = []
    seen: set[str] = set()
    for value in values:
        proxy = value.strip().strip("[](){}'\"")
        if proxy and proxy not in seen:
            proxies.append(proxy)
            seen.add(proxy)
    return proxies


def read_proxy_master_list() -> list[str]:
    """Read and normalise the master proxy list from the Replit Secret."""
    raw = os.getenv(PROXY_LIST_ENV, "")
    if not raw.strip():
        raise RuntimeError(
            f"{PROXY_LIST_ENV} belum diatur. Isi satu proxy per baris di Secrets."
        )
    proxies = split_proxy_text(raw)
    if not proxies:
        raise RuntimeError(f"{PROXY_LIST_ENV} tidak berisi proxy yang valid.")
    return proxies


def build_requests_proxy(proxy_str: str) -> dict[str, str]:
    """
    Convert a bare proxy string to a requests proxy dict.

    Accepted formats:
      user:pass@host:port
      http://user:pass@host:port
      socks5://user:pass@host:port
    """
    s = proxy_str.strip()
    if "://" not in s:
        s = "http://" + s
    return {"http": s, "https": s}


# ---------------------------------------------------------------------------
# Proxy liveness check
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class ProbeResult:
    ok: bool
    ip: str = ""
    location: str = ""
    error: str = ""


def probe_proxy(proxy_str: str) -> ProbeResult:
    """
    Test a proxy by fetching an IP-info endpoint through it.
    Returns a ProbeResult with ok=True and IP/location on success.
    Each proxy string is used verbatim so every session-ID is independent.
    """
    proxies = build_requests_proxy(proxy_str)

    for url in CHECK_URLS:
        try:
            resp = requests.get(
                url,
                proxies=proxies,
                timeout=PROXY_CHECK_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()

            # ipinfo.io returns {"ip":…,"city":…,"country":…}
            # ipify returns {"ip":…}
            ip = data.get("ip", "")
            city = data.get("city", "")
            country = data.get("country", "")
            location = ", ".join(part for part in [city, country] if part) or "–"
            return ProbeResult(ok=True, ip=ip, location=location)

        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)

    return ProbeResult(ok=False, error=last_error)


# ---------------------------------------------------------------------------
# Persistent FIFO stock
# ---------------------------------------------------------------------------


class ProxyStock:
    """Persistent FIFO stock with automatic refill from the master list."""

    def __init__(self, master_list: list[str], state_file: Path) -> None:
        self.master_list = master_list
        self.state_file = state_file
        self._lock = threading.Lock()
        self._stock = self._load()
        if not self._stock:
            self._refill_and_save()

    # --- persistence --------------------------------------------------------

    def _load(self) -> list[str]:
        if not self.state_file.exists():
            return []
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"File stok {self.state_file} rusak atau tidak bisa dibaca."
            ) from exc
        if not isinstance(data, list) or not all(isinstance(i, str) for i in data):
            raise RuntimeError(f"Format file stok {self.state_file} tidak valid.")
        repaired = split_proxy_text("\n".join(data))
        if repaired != data:
            self._stock = repaired
            self._save()
        return repaired

    def _save(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=".proxy_stock.", suffix=".tmp",
            dir=self.state_file.parent, text=True,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._stock, f, ensure_ascii=False, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.state_file)
            try:
                os.chmod(self.state_file, 0o600)
            except OSError:
                pass
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def _refill_and_save(self) -> None:
        self._stock = list(self.master_list)
        self._save()

    # --- public API ---------------------------------------------------------

    def pop_front(self) -> Optional[str]:
        """Remove and return the first proxy, refilling if the list was empty."""
        with self._lock:
            if not self._stock:
                self._refill_and_save()
            if not self._stock:
                return None
            proxy = self._stock.pop(0)
            self._save()
            return proxy

    def discard(self, proxy: str) -> None:
        """Remove a proxy from stock (already popped, no-op for safety)."""
        # pop_front already removed it; this exists for future use.

    def count(self) -> int:
        with self._lock:
            return len(self._stock)

    def refill_now(self) -> int:
        with self._lock:
            self._refill_and_save()
            return len(self._stock)


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} belum diatur di Replit Secrets.")
    return value


def find_active_proxy(stock: ProxyStock) -> tuple[Optional[str], ProbeResult, int]:
    """
    Pop proxies one by one until we find an active one.
    Returns (proxy_str, probe_result, remaining_stock).
    Returns (None, failed_result, 0) if the entire stock is exhausted.

    To prevent an infinite loop the search is capped at the number of proxies
    in the master list (one full cycle).
    """
    cap = max(len(stock.master_list), 1)
    for _ in range(cap):
        proxy = stock.pop_front()
        if proxy is None:
            break
        result = probe_proxy(proxy)
        if result.ok:
            return proxy, result, stock.count()
    return None, ProbeResult(ok=False, error="Semua proxy di stok tidak aktif."), 0


def create_bot() -> tuple[telebot.TeleBot, ProxyStock]:
    token = get_required_env(TOKEN_ENV)
    stock = ProxyStock(read_proxy_master_list(), STOCK_FILE)
    bot = telebot.TeleBot(token, threaded=True, num_threads=4)

    @bot.message_handler(commands=["start", "help"])
    def send_welcome(message: telebot.types.Message) -> None:
        bot.reply_to(
            message,
            "Halo! Ketik /proxy untuk mengambil satu proxy aktif.\n"
            "Ketik /stok untuk melihat jumlah stok yang tersedia.",
        )

    @bot.message_handler(commands=["proxy"])
    def give_proxy(message: telebot.types.Message) -> None:
        # Inform the user that checking is in progress
        bot.reply_to(message, "⏳ Mengecek proxy... Harap tunggu sebentar.")

        proxy, result, remaining = find_active_proxy(stock)

        if proxy is None or not result.ok:
            bot.reply_to(
                message,
                "❌ Maaf, semua proxy di stok sedang tidak aktif.\n"
                "Silakan coba lagi nanti atau hubungi admin.",
            )
            return

        safe_proxy = html.escape(proxy)
        safe_ip = html.escape(result.ip)
        safe_loc = html.escape(result.location)

        bot.reply_to(
            message,
            f"<b>Proxy Kamu:</b>\n"
            f"<code>{safe_proxy}</code>\n\n"
            f"Status: ✅ ACTIVE / OK\n"
            f"IP Proxy: <b>{safe_ip}</b>\n"
            f"Lokasi: <b>{safe_loc}</b>\n"
            f"Sisa Stok: <b>{remaining}</b>",
            parse_mode="HTML",
        )

    @bot.message_handler(commands=["stok"])
    def show_stock(message: telebot.types.Message) -> None:
        bot.reply_to(message, f"Sisa stok proxy: {stock.count()}")

    return bot, stock


def main() -> None:
    bot, _stock = create_bot()
    print("ZullwwBot berhasil dihidupkan!")
    bot.infinity_polling(skip_pending=True, timeout=30, long_polling_timeout=30)


if __name__ == "__main__":
    main()
