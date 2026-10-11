#!/usr/bin/env python3
"""
odoo_inventory_sync.py - Revolv Space
Sync inventory changes from a CSV file into Odoo (stock.quant) from OUTSIDE Odoo.

Nothing is installed in the Odoo database (no module, no Studio): the script is an
external client that talks to Odoo's standard External API (XML-RPC) or, when the
API is not available on the subscription plan, produces a file for Odoo's built-in
importer. Python 3.10+ standard library only - no pip install needed.

CSV input (header row required, ';' or ',' separated, extra columns are ignored):
    material_sku     Odoo product Internal Reference (default_code), e.g. RS-SADA-MTR-01
    quantity_change  signed change: +12 (received) or -3 (consumed)
    location         Odoo full location name, e.g. WH/Stock

Modes:
    check   read-only preview: validates every row against Odoo, changes nothing (default)
    apply   preview -> asks for confirmation -> writes to Odoo -> verifies each row
    export  no-API fallback: needs --snapshot (Odoo stock export) and writes an
            Odoo "Physical Inventory" import file with absolute counted quantities

Examples:
    python odoo_inventory_sync.py check  updates.csv
    python odoo_inventory_sync.py apply  updates.csv
    python odoo_inventory_sync.py apply                    # folder mode (folders from odoo_sync.ini)
    python odoo_inventory_sync.py export updates.csv --snapshot stock_export.csv

Exit codes: 0 = every row OK, 1 = some rows need attention, 2 = run stopped (config/login/file).
"""
from __future__ import annotations

import argparse
import configparser
import csv
import hashlib
import http.client
import io
import json
import logging
import logging.handlers
import os
import shutil
import sys
import time
import xmlrpc.client
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

REQUIRED_COLUMNS = ("material_sku", "quantity_change", "location")
SCRIPT_DIR = Path(__file__).resolve().parent
LOG = logging.getLogger("odoo_sync")

# ---------------------------------------------------------------- result vocabulary
APPLIED, READY, SKIPPED, FAILED, NOT_PROCESSED, CHECK_IN_ODOO = (
    "APPLIED", "READY", "SKIPPED", "FAILED", "NOT PROCESSED", "CHECK IN ODOO")

# Plain-English advice shown in the results file (written for non-technical colleagues).
ADVICE = {
    "SKU_NOT_FOUND": "Check the SKU spelling in Odoo (Inventory > Products), correct it in the FIX_ME file and sync that file.",
    "LOCATION_NOT_FOUND": "Use the full location name exactly as in Odoo, e.g. WH/Stock.",
    "MISSING_VALUE": "Fill in the empty cell, then sync the FIX_ME file.",
    "INVALID_QUANTITY": "Enter a plain number such as 12 or -3 (no text, units or Excel errors).",
    "ZERO_CHANGE": "Nothing to do: the change is 0.",
    "LIMIT_EXCEEDED": "Unusually large change - double-check it. If it is correct, ask Operations to enter it in Odoo.",
    "NEGATIVE_STOCK": "This would make stock negative. Check the +/- sign and the location.",
    "NOT_STORABLE": "This item is not stock-tracked in Odoo, so no update is needed. Ask Operations if unsure.",
    "TRACKED_PRODUCT": "Lot/serial-tracked part: Operations records it in Odoo with its lot/serial number.",
    "COUNT_IN_PROGRESS": "Someone has an unfinished stock count for this item in Odoo. Ask Operations to apply or clear it, then sync again.",
    "AMBIGUOUS": "Odoo has several matching records. Ask Operations to check this item.",
    "API_UNAVAILABLE": "Odoo did not answer in time. Nothing was changed for this row - sync the FIX_ME file again later.",
    "API_UNCERTAIN": "Odoo stopped answering during the update. Do NOT resend: Operations will check this item in Odoo.",
    "ODOO_REJECTED": "Odoo refused the change. Forward the results file to Operations.",
    "VERIFY_MISMATCH": "Odoo shows a different quantity than expected after the update. Forward the results file to Operations.",
    "NOT_PROCESSED": "Not processed because the run stopped early. Sync the FIX_ME file again.",
    "UNEXPECTED": "Unexpected problem. Forward the results file and sync.log to Operations.",
}


class FatalError(Exception):
    """Stops the whole run (configuration, login, unreadable file). Reported cleanly."""


class RowError(Exception):
    """A problem with a single row. The row is marked and the run continues."""

    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code, self.detail = code, detail


class OdooUnavailable(Exception):
    """Odoo did not answer after all retries (timeout, network, HTTP 5xx/429)."""


@dataclass
class Row:
    line: int                      # spreadsheet row number (header = row 1)
    sku: str
    location: str
    qty_raw: str
    change: Decimal | None = None
    status: str = ""
    code: str = ""
    detail: str = ""
    before: Decimal | None = None
    after: Decimal | None = None

    def mark(self, status: str, code: str = "", detail: str = "") -> None:
        self.status, self.code, self.detail = status, code, detail

    def log_line(self) -> str:
        qty = f" | {fmt(self.before)} -> {fmt(self.after)}" if self.after is not None else ""
        problem = f" {self.code}: {self.detail}" if self.code else ""
        return f"Row {self.line} | {self.sku or '-'} @ {self.location or '-'} | {self.qty_raw or '-'} | {self.status}{problem}{qty}"


def fmt(d: Decimal | None) -> str:
    if d is None:
        return ""
    s = format(d.normalize(), "f")
    return s if "." not in s else s.rstrip("0").rstrip(".")


# ---------------------------------------------------------------- configuration
@dataclass
class Config:
    url: str
    db: str
    login: str
    api_key: str
    timeout: float
    retries: int
    backoff: float
    max_outages: int
    max_abs_change: Decimal
    inbox: Path | None
    results: Path
    archive: Path | None


def load_config(path: Path, need_api: bool) -> Config:
    """odoo_sync.ini next to the script; environment variables override it.
    The API key is NEVER read from the ini file: environment variable ODOO_API_KEY
    or a separate key file (api_key_file) that only the Ops admin can read."""
    cp = configparser.ConfigParser()
    if path.exists():
        cp.read(path, encoding="utf-8")
    g = lambda sec, key, env=None, default="": os.environ.get(env or "", "") or cp.get(sec, key, fallback=default)

    api_key = os.environ.get("ODOO_API_KEY", "")
    key_file = cp.get("odoo", "api_key_file", fallback="")
    if not api_key and key_file and Path(key_file).expanduser().exists():
        api_key = Path(key_file).expanduser().read_text(encoding="utf-8").strip()

    def folder(key: str) -> Path | None:
        v = cp.get("folders", key, fallback="")
        return (path.parent / v).resolve() if v and not Path(v).is_absolute() else (Path(v) if v else None)

    cfg = Config(
        url=g("odoo", "url", "ODOO_URL").rstrip("/"),
        db=g("odoo", "db", "ODOO_DB"),
        login=g("odoo", "login", "ODOO_LOGIN"),
        api_key=api_key,
        timeout=float(g("odoo", "timeout_seconds", default="20")),
        retries=int(g("odoo", "max_retries", default="3")),
        backoff=float(g("odoo", "backoff_seconds", default="2")),
        max_outages=int(g("odoo", "stop_after_consecutive_outages", default="3")),
        max_abs_change=Decimal(g("safety", "max_abs_change", default="10000")),
        inbox=folder("inbox"),
        results=folder("results") or Path.cwd(),
        archive=folder("archive"),
    )
    if need_api:
        missing = [n for n, v in (("url", cfg.url), ("db", cfg.db), ("login", cfg.login), ("ODOO_API_KEY", cfg.api_key)) if not v]
        if missing:
            raise FatalError(f"Missing configuration: {', '.join(missing)}. See odoo_sync.ini.")
        if not cfg.url.startswith("https://") and "localhost" not in cfg.url and "127.0.0.1" not in cfg.url:
            raise FatalError("The Odoo URL must use https:// (the API key would otherwise travel unencrypted).")
    return cfg


# ---------------------------------------------------------------- CSV reading & row validation
def read_updates(path: Path) -> tuple[list[Row], str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise FatalError(f"Cannot open '{path.name}': {exc.strerror}. Is it still open in Excel?") from exc
    for encoding in ("utf-8-sig", "cp1252"):              # Excel on Windows often saves cp1252
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    first = text.splitlines()[0] if text.strip() else ""
    if not first:
        raise FatalError(f"'{path.name}' is empty.")
    delimiter = ";" if first.count(";") > first.count(",") else ","   # European Excel uses ';'
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    header = [h.strip().lower().replace(" ", "_") for h in next(reader)]
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        raise FatalError(f"'{path.name}' is missing column(s) {', '.join(missing)}. "
                         f"Found: {', '.join(header)}. Required: {', '.join(REQUIRED_COLUMNS)}.")
    idx = {c: header.index(c) for c in REQUIRED_COLUMNS}
    rows: list[Row] = []
    for line, rec in enumerate(reader, start=2):
        if not any(cell.strip() for cell in rec):            # ignore blank Excel rows
            continue
        cell = lambda c: rec[idx[c]].strip() if idx[c] < len(rec) else ""
        rows.append(Row(line, cell("material_sku"), cell("location"), cell("quantity_change")))
    return rows, delimiter


def parse_quantity(raw: str, delimiter: str, max_abs: Decimal) -> Decimal:
    s = raw.strip().replace("−", "-").replace(" ", "").replace(" ", "")
    if not s:
        raise RowError("MISSING_VALUE", "quantity_change is empty.")
    if s.startswith("#"):
        raise RowError("INVALID_QUANTITY", f"'{raw}' is an Excel error value, not a number.")
    if "," in s:
        if delimiter == ";" and s.count(",") == 1 and "." not in s:
            s = s.replace(",", ".")                          # European decimal comma: 1,5 -> 1.5
        else:
            raise RowError("INVALID_QUANTITY", f"'{raw}' is ambiguous (comma). Use a plain number like 1500 or 1.5.")
    try:
        value = Decimal(s)
    except InvalidOperation:
        raise RowError("INVALID_QUANTITY", f"'{raw}' is not a number.") from None
    if not value.is_finite():
        raise RowError("INVALID_QUANTITY", f"'{raw}' is not a valid quantity.")
    if value == 0:
        raise RowError("ZERO_CHANGE", "Change is 0.")
    if abs(value) > max_abs:
        raise RowError("LIMIT_EXCEEDED", f"Change {fmt(value)} exceeds the safety limit of {fmt(max_abs)} units.")
    return value


def validate_locally(row: Row, delimiter: str, max_abs: Decimal) -> None:
    if not row.sku:
        raise RowError("MISSING_VALUE", "material_sku is empty.")
    if not row.location:
        raise RowError("MISSING_VALUE", "location is empty.")
    row.change = parse_quantity(row.qty_raw, delimiter, max_abs)


# ---------------------------------------------------------------- Odoo XML-RPC client
class _TimeoutTransport(xmlrpc.client.Transport):
    def __init__(self, timeout: float):
        super().__init__()
        self._timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self._timeout
        return conn


class _TimeoutSafeTransport(xmlrpc.client.SafeTransport):
    def __init__(self, timeout: float):
        super().__init__()
        self._timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self._timeout
        return conn


class OdooClient:
    """Thin wrapper around Odoo's External XML-RPC API with timeouts and bounded retries.
    All RPC traffic goes through call(), so moving to Odoo's JSON-2 API (XML-RPC is
    deprecated since Odoo 19) only means replacing this class."""

    TRANSIENT_HTTP = {429, 500, 502, 503, 504}

    def __init__(self, cfg: Config):
        self.cfg, self.uid = cfg, None
        transport = _TimeoutSafeTransport if cfg.url.startswith("https") else _TimeoutTransport
        self._common = xmlrpc.client.ServerProxy(f"{cfg.url}/xmlrpc/2/common", transport=transport(cfg.timeout), allow_none=True)
        self._object = xmlrpc.client.ServerProxy(f"{cfg.url}/xmlrpc/2/object", transport=transport(cfg.timeout), allow_none=True)

    def login(self) -> str:
        try:
            version = self._retry(lambda: self._common.version(), "version").get("server_version", "?")
            self.uid = self._retry(lambda: self._common.authenticate(self.cfg.db, self.cfg.login, self.cfg.api_key, {}), "authenticate")
        except OdooUnavailable as exc:
            raise FatalError(f"Cannot reach Odoo at {self.cfg.url} ({exc}). Check the internet connection and try again.") from None
        except xmlrpc.client.Fault as exc:
            raise FatalError(f"Odoo refused the login: {short_fault(exc)}") from None
        if not self.uid:
            raise FatalError("Login refused: check the login and API key of the integration user (they may have expired).")
        return version

    def call(self, model: str, method: str, args: list, kwargs: dict | None = None):
        def rpc():
            return self._object.execute_kw(self.cfg.db, self.uid, self.cfg.api_key, model, method, args, kwargs or {})
        try:
            return self._retry(rpc, f"{model}.{method}")
        except xmlrpc.client.Fault as exc:
            # Odoo cannot serialise a Python None over XML-RPC. Methods that return
            # nothing (e.g. action_apply_inventory) therefore raise this Fault AFTER the
            # change was committed -> treat as success; the caller verifies by re-reading.
            if "cannot marshal None" in exc.faultString:
                return None
            raise

    def _retry(self, fn, label: str):
        attempts = self.cfg.retries + 1
        for attempt in range(1, attempts + 1):
            try:
                return fn()
            except xmlrpc.client.Fault:
                raise                                        # business error: retrying will not help
            except xmlrpc.client.ProtocolError as exc:
                if exc.errcode in (401, 403, 404):
                    raise FatalError(f"Odoo answered HTTP {exc.errcode} on the API endpoint. The External API may not be "
                                     f"enabled for this database/plan (see report, assumption A2).") from None
                if exc.errcode not in self.TRANSIENT_HTTP:
                    raise
                reason = f"HTTP {exc.errcode}"
            except (TimeoutError, ConnectionError, http.client.HTTPException, OSError) as exc:
                reason = type(exc).__name__
            if attempt == attempts:
                raise OdooUnavailable(f"{label}: {reason} after {attempts} attempts")
            wait = self.cfg.backoff * 2 ** (attempt - 1)
            LOG.warning("  %s: %s - retry %d/%d in %.1fs", label, reason, attempt, self.cfg.retries, wait)
            time.sleep(wait)


def short_fault(exc: xmlrpc.client.Fault) -> str:
    lines = [ln.strip() for ln in str(exc.faultString).strip().splitlines() if ln.strip()]
    return (lines[-1] if lines else "unknown error")[:200]


# ---------------------------------------------------------------- Odoo inventory logic
class OdooInventory:
    """Product/location lookups (cached) and the standard Odoo inventory-adjustment flow:
    stock.quant.inventory_quantity  ->  action_apply_inventory  (context inventory_mode)."""

    CTX = {"context": {"inventory_mode": True}}

    def __init__(self, client: OdooClient):
        self.c = client
        self._products: dict[str, dict | RowError] = {}
        self._locations: dict[str, int | RowError] = {}
        fields = self.c.call("product.product", "fields_get", [], {"attributes": ["type"]})
        self._has_is_storable = "is_storable" in fields      # Odoo 18+; Odoo 17 uses type == 'product'
        self._has_tracking = "tracking" in fields

    def product(self, sku: str) -> dict:
        if sku not in self._products:
            try:
                self._products[sku] = self._find_product(sku)
            except RowError as exc:
                self._products[sku] = exc
        hit = self._products[sku]
        if isinstance(hit, RowError):
            raise hit
        return hit

    def _find_product(self, sku: str) -> dict:
        fields = ["id", "display_name", "type"] + (["is_storable"] if self._has_is_storable else []) \
            + (["tracking"] if self._has_tracking else [])
        recs = self.c.call("product.product", "search_read", [[("default_code", "=", sku)]], {"fields": fields, "limit": 2})
        if not recs:
            alt = self.c.call("product.product", "search_read", [[("default_code", "=ilike", sku)]], {"fields": ["default_code"], "limit": 2})
            hint = f" Did you mean '{alt[0]['default_code']}'?" if len(alt) == 1 else ""
            raise RowError("SKU_NOT_FOUND", f"'{sku}' does not exist in Odoo.{hint}")
        if len(recs) > 1:
            raise RowError("AMBIGUOUS", f"Several Odoo products use SKU '{sku}'.")
        p = recs[0]
        storable = p.get("is_storable") if self._has_is_storable else p.get("type") == "product"
        if not storable:
            raise RowError("NOT_STORABLE", f"'{sku}' is not a stock-tracked product in Odoo.")
        if p.get("tracking", "none") != "none":
            raise RowError("TRACKED_PRODUCT", f"'{sku}' is tracked by {p['tracking']}; a lot/serial number is required.")
        return p

    def location(self, name: str) -> int:
        if name not in self._locations:
            recs = self.c.call("stock.location", "search_read",
                               [[("complete_name", "=", name), ("usage", "=", "internal")]], {"fields": ["id"], "limit": 2})
            if len(recs) == 1:
                self._locations[name] = recs[0]["id"]
            else:
                alt = self.c.call("stock.location", "search_read",
                                  [[("complete_name", "=ilike", name), ("usage", "=", "internal")]],
                                  {"fields": ["complete_name"], "limit": 2}) if not recs else []
                hint = f" Did you mean '{alt[0]['complete_name']}'?" if len(alt) == 1 else ""
                self._locations[name] = RowError(
                    "LOCATION_NOT_FOUND" if not recs else "AMBIGUOUS",
                    f"Location '{name}' {'was not found' if not recs else 'is ambiguous'} in Odoo.{hint}")
        hit = self._locations[name]
        if isinstance(hit, RowError):
            raise hit
        return hit

    def quant(self, product_id: int, location_id: int) -> tuple[int | None, Decimal]:
        recs = self.c.call("stock.quant", "search_read",
                           [[("product_id", "=", product_id), ("location_id", "=", location_id)]],
                           {"fields": ["id", "quantity", "inventory_quantity_set"]})
        if len(recs) > 1:
            raise RowError("AMBIGUOUS", "Stock for this item is split (packages/owners); adjust it manually in Odoo.")
        if not recs:
            return None, Decimal(0)
        if recs[0].get("inventory_quantity_set"):
            raise RowError("COUNT_IN_PROGRESS", "An unapplied inventory count already exists for this item and location.")
        return recs[0]["id"], Decimal(str(recs[0]["quantity"]))

    def set_quantity(self, product_id: int, location_id: int, quant_id: int | None, target: Decimal) -> Decimal:
        """Set the ABSOLUTE target computed once (before + change). Re-sending the same
        target is harmless, so retries after a timeout can never double-count."""
        qty = float(target)
        if quant_id:
            self.c.call("stock.quant", "write", [[quant_id], {"inventory_quantity": qty}], self.CTX)
        else:   # in inventory mode Odoo re-uses an existing quant instead of duplicating it
            created = self.c.call("stock.quant", "create",
                                  [{"product_id": product_id, "location_id": location_id, "inventory_quantity": qty}], self.CTX)
            quant_id = created[0] if isinstance(created, list) else created
        result = self.c.call("stock.quant", "action_apply_inventory", [[quant_id]], self.CTX)
        if isinstance(result, dict):        # Odoo opened a confirmation wizard (e.g. count conflict)
            try:
                self.c.call("stock.quant", "action_clear_inventory_quantity", [[quant_id]], self.CTX)
            finally:
                raise RowError("ODOO_REJECTED", "Odoo asked for a manual confirmation (stock count conflict).")
        recs = self.c.call("stock.quant", "search_read", [[("id", "=", quant_id)]], {"fields": ["quantity"]})
        now = Decimal(str(recs[0]["quantity"])) if recs else Decimal(0)
        if abs(now - target) > Decimal("0.0001"):
            raise RowError("VERIFY_MISMATCH", f"Expected {fmt(target)} in Odoo after the update, found {fmt(now)}.")
        return now


# ---------------------------------------------------------------- per-file processing
def process_api(rows: list[Row], inv: OdooInventory, cfg: Config, apply: bool) -> None:
    """One loop for preview and apply. Every row is isolated: a bad row, an Odoo error
    or a crash inside one row is logged and the loop continues with the next row."""
    outages = 0
    projected: dict[tuple[int, int], Decimal] = {}    # running balance for duplicate SKU+location rows
    todo = [r for r in rows if r.status in ("", READY)]
    for i, row in enumerate(todo):
        phase = "read"
        try:
            p = inv.product(row.sku)
            loc = inv.location(row.location)
            quant_id, odoo_qty = inv.quant(p["id"], loc)
            key = (p["id"], loc)
            row.before = odoo_qty if apply else projected.get(key, odoo_qty)
            row.after = row.before + row.change
            if row.after < 0:
                raise RowError("NEGATIVE_STOCK", f"Stock would become {fmt(row.after)} (Odoo has {fmt(row.before)}).")
            if apply:
                phase = "write"
                row.after = inv.set_quantity(p["id"], loc, quant_id, row.after)
                row.mark(APPLIED)
            else:
                projected[key] = row.after
                row.mark(READY)
            outages = 0
        except RowError as exc:
            row.mark(FAILED, exc.code, exc.detail)
        except OdooUnavailable as exc:
            outages += 1
            if phase == "write":
                row.mark(CHECK_IN_ODOO, "API_UNCERTAIN", f"No answer from Odoo while writing ({exc}).")
            else:
                row.mark(FAILED, "API_UNAVAILABLE", f"No answer from Odoo ({exc}).")
            if outages >= cfg.max_outages:
                LOG.info(row.log_line())
                LOG.error("Odoo did not answer %d times in a row - stopping safely. Remaining rows are untouched.", outages)
                for rest in todo[i + 1:]:
                    rest.mark(FAILED, "NOT_PROCESSED", "Run stopped early because Odoo was unreachable.")
                    LOG.info(rest.log_line())
                return
        except xmlrpc.client.Fault as exc:
            row.mark(CHECK_IN_ODOO if phase == "write" else FAILED, "ODOO_REJECTED", short_fault(exc))
        except FatalError:
            raise
        except Exception as exc:  # noqa: BLE001 - last line of defence: never crash the whole run
            LOG.exception("Unexpected error on row %d", row.line)
            row.mark(CHECK_IN_ODOO if phase == "write" else FAILED, "UNEXPECTED", f"{type(exc).__name__}: {exc}")
        LOG.info(row.log_line())


def process_export(rows: list[Row], snapshot: Path) -> list[tuple[str, str, Decimal]]:
    """No-API fallback: compute absolute counted quantities from an Odoo stock export."""
    snap_rows, _ = read_snapshot(snapshot)
    stock: dict[tuple[str, str], Decimal] = {}
    skus, locations = set(), set()
    for sku, loc, qty in snap_rows:
        stock[(sku, loc)] = stock.get((sku, loc), Decimal(0)) + qty
        skus.add(sku)
        locations.add(loc)
    for row in rows:
        if row.status:
            continue
        try:
            if row.sku not in skus:
                raise RowError("SKU_NOT_FOUND", f"'{row.sku}' is not in the Odoo stock export (new item? enter it in Odoo first).")
            if row.location not in locations:
                raise RowError("LOCATION_NOT_FOUND", f"Location '{row.location}' is not in the Odoo stock export.")
            key = (row.sku, row.location)
            row.before = stock.get(key, Decimal(0))
            row.after = row.before + row.change
            if row.after < 0:
                raise RowError("NEGATIVE_STOCK", f"Stock would become {fmt(row.after)} (export shows {fmt(row.before)}).")
            stock[key] = row.after
            row.mark(READY)
        except RowError as exc:
            row.mark(FAILED, exc.code, exc.detail)
        LOG.info(row.log_line())
    final = {}
    for row in rows:
        if row.status == READY:
            final[(row.sku, row.location)] = stock[(row.sku, row.location)]
    return [(s, l, q) for (s, l), q in final.items()]


def read_snapshot(path: Path) -> tuple[list[tuple[str, str, Decimal]], str]:
    aliases = {
        "sku": ("material_sku", "default_code", "internal_reference", "product/internal_reference"),
        "location": ("location", "location/full_location_name", "location/complete_name", "full_location_name"),
        "qty": ("on_hand", "quantity", "on_hand_quantity", "quantity_on_hand"),
    }
    text = path.read_text(encoding="utf-8-sig")
    first = text.splitlines()[0]
    delim = ";" if first.count(";") > first.count(",") else ","
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    header = [h.strip().lower().replace(" ", "_") for h in next(reader)]
    idx = {}
    for key, names in aliases.items():
        found = [header.index(n) for n in names if n in header]
        if not found:
            raise FatalError(f"Snapshot '{path.name}' needs a {key} column (one of: {', '.join(names)}).")
        idx[key] = found[0]
    out = []
    for rec in reader:
        if rec and any(c.strip() for c in rec):
            out.append((rec[idx["sku"]].strip(), rec[idx["location"]].strip(), Decimal(rec[idx["qty"]].strip().replace(",", "."))))
    return out, delim


# ---------------------------------------------------------------- outputs
def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as fh:    # BOM: opens cleanly in Excel
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def write_outputs(src: Path, rows: list[Row], mode: str, out_dir: Path, stamp: str) -> list[Path]:
    files = []
    results = out_dir / f"{src.stem}_{stamp}_{mode.upper()}_results.csv"
    write_csv(results, ["row", "material_sku", "location", "quantity_change", "result", "problem", "details",
                        "what_to_do", "qty_before", "qty_after"],
              [[r.line, r.sku, r.location, r.qty_raw, r.status, r.code, r.detail, ADVICE.get(r.code, ""),
                fmt(r.before), fmt(r.after)] for r in rows])
    files.append(results)
    if mode == "apply":
        retry = [r for r in rows if r.status == FAILED]
        if retry:
            fix = out_dir / f"{src.stem}_{stamp}_FIX_ME.csv"
            write_csv(fix, list(REQUIRED_COLUMNS) + ["problem"],
                      [[r.sku, r.qty_raw, r.location, f"Row {r.line}: {r.detail}"] for r in retry])
            files.append(fix)
        done = [r for r in rows if r.status == APPLIED]
        if done:
            undo = out_dir / f"{src.stem}_{stamp}_UNDO.csv"
            write_csv(undo, list(REQUIRED_COLUMNS), [[r.sku, fmt(-r.change), r.location] for r in done])
            files.append(undo)
    return files


def summary(rows: list[Row]) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in rows:
        out[r.status] = out.get(r.status, 0) + 1
    return out


# ---------------------------------------------------------------- ledger (blocks sending the same file twice)
def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def ledger_path(cfg: Config) -> Path:
    return cfg.results / ".applied_files.json"


def ledger_load(cfg: Config) -> dict:
    try:
        return json.loads(ledger_path(cfg).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def ledger_add(cfg: Config, digest: str, src: Path, applied: int) -> None:
    data = ledger_load(cfg)
    data[digest] = {"file": src.name, "applied_at": datetime.now().isoformat(timespec="seconds"), "rows_applied": applied}
    ledger_path(cfg).parent.mkdir(parents=True, exist_ok=True)
    ledger_path(cfg).write_text(json.dumps(data, indent=2), encoding="utf-8")


# ---------------------------------------------------------------- orchestration
def run_file(src: Path, mode: str, cfg: Config, inv: OdooInventory | None, args) -> int:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    LOG.info("=" * 70)
    LOG.info("File: %s | mode: %s", src.name, mode.upper())
    rows, delimiter = read_updates(src)
    digest = file_hash(src)
    if mode == "apply" and digest in ledger_load(cfg) and not args.force:
        prev = ledger_load(cfg)[digest]
        raise FatalError(f"'{src.name}' was already sent to Odoo on {prev['applied_at']}. Sending it again would "
                         f"double the changes. (Use --force only if you are sure.)")
    LOG.info("%d data rows found (separator '%s').", len(rows), delimiter)
    for row in rows:
        try:
            validate_locally(row, delimiter, cfg.max_abs_change)
        except RowError as exc:
            row.mark(SKIPPED if exc.code == "ZERO_CHANGE" else FAILED, exc.code, exc.detail)
            LOG.info(row.log_line())

    try:
        if mode == "export":
            counted = process_export(rows, Path(args.snapshot))
            out = cfg.results / f"{src.stem}_{stamp}_ODOO_IMPORT.csv"
            write_csv(out, ["product_id", "location_id", "inventory_quantity"], [[s, l, fmt(q)] for s, l, q in counted])
            LOG.info("Odoo import file: %s  ->  Inventory > Physical Inventory > Import records > Test > Import > Apply", out.name)
        else:
            process_api(rows, inv, cfg, apply=False)                    # preview pass (read-only)
            if mode == "apply":
                ready = [r for r in rows if r.status == READY]
                LOG.info("Preview: %d row(s) ready, %d need attention.", len(ready), len(rows) - len(ready))
                if ready and confirm(len(ready), args.yes):
                    for r in ready:
                        r.mark("")
                    process_api(rows, inv, cfg, apply=True)            # write pass, re-reads live stock
                    applied = sum(r.status == APPLIED for r in rows)
                    if applied:
                        ledger_add(cfg, digest, src, applied)
                else:
                    for r in ready:
                        r.mark(NOT_PROCESSED, "", "Not sent (cancelled or nothing to send).")
                    mode = "check"
    finally:
        for f in write_outputs(src, rows, mode, cfg.results, stamp):
            LOG.info("Written: %s", f.name)

    counts = summary(rows)
    LOG.info("Summary: %s", ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    if mode == "apply" and cfg.archive:
        cfg.archive.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(cfg.archive / f"{src.stem}_{stamp}{src.suffix}"))
        LOG.info("Input file moved to %s (prevents sending it twice).", cfg.archive.name)
    bad = counts.get(FAILED, 0) + counts.get(CHECK_IN_ODOO, 0)
    return 1 if bad else 0


def confirm(n: int, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    if not sys.stdin or not sys.stdin.isatty():
        raise FatalError("Confirmation needed: run interactively or add --yes.")
    answer = input(f"\nSend {n} ready row(s) to Odoo now? Type Y and press Enter (anything else cancels): ")
    return answer.strip().lower() in ("y", "yes")


def setup_logging(results: Path, verbose: bool) -> None:
    results.mkdir(parents=True, exist_ok=True)
    LOG.setLevel(logging.DEBUG)
    fh = logging.handlers.RotatingFileHandler(results / "sync.log", maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s"))
    LOG.addHandler(fh)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    LOG.addHandler(ch)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Sync inventory changes from CSV into Odoo (runs outside Odoo).")
    ap.add_argument("mode", nargs="?", default="check", choices=("check", "apply", "export"))
    ap.add_argument("files", nargs="*", help="CSV file(s). If omitted, every CSV in the configured inbox folder.")
    ap.add_argument("--config", default=str(SCRIPT_DIR / "odoo_sync.ini"))
    ap.add_argument("--snapshot", help="export mode: Odoo stock export (sku, location, on-hand quantity)")
    ap.add_argument("--yes", action="store_true", help="apply without asking for confirmation")
    ap.add_argument("--force", action="store_true", help="allow re-sending a file that was already applied")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args(argv)

    try:
        cfg = load_config(Path(args.config), need_api=args.mode != "export")
    except FatalError as exc:
        print(f"STOPPED: {exc}")
        return 2
    setup_logging(cfg.results, args.verbose)

    try:
        if args.mode == "export" and not args.snapshot:
            raise FatalError("export mode needs --snapshot <Odoo stock export CSV>.")
        files = [Path(f) for f in args.files]
        if not files:
            if not cfg.inbox:
                raise FatalError("No CSV file given and no inbox folder configured.")
            files = sorted(p for p in cfg.inbox.glob("*.csv") if not p.name.startswith(("~$", ".")))
            if not files:
                LOG.info("Nothing to do: no CSV file in %s.", cfg.inbox)
                return 0
        inv = None
        if args.mode != "export":
            client = OdooClient(cfg)
            version = client.login()
            LOG.info("Connected to Odoo %s, database '%s' as %s.", version, cfg.db, cfg.login)
            inv = OdooInventory(client)
        worst = 0
        for f in files:
            try:
                worst = max(worst, run_file(f, args.mode, cfg, inv, args))
            except FatalError as exc:          # one unreadable file must not block the others
                LOG.error("STOPPED for %s: %s", f.name, exc)
                worst = 2
        return worst
    except FatalError as exc:
        LOG.error("STOPPED: %s", exc)
        return 2
    except KeyboardInterrupt:
        LOG.error("Cancelled by user. Results written so far are in the results folder.")
        return 2


if __name__ == "__main__":
    sys.exit(main())
