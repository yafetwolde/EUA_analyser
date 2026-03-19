"""
analyser.py
Core comparison logic for the EUA Delta Analyser.
Compares a previous approval snapshot against a current snapshot
and produces voyage-level and book-level delta reports.

Supports the native EUA Emission Tracker CSV format with automatic
column mapping from the raw export column names.
"""

import pandas as pd
import numpy as np
from io import BytesIO

# ---------------------------------------------------------------------------
# COLUMN MAPPING — raw CSV export → internal names
# The app accepts EITHER the raw CSV column names OR the internal names.
# ---------------------------------------------------------------------------
COLUMN_MAP = {
    # Raw CSV name           : internal name
    "DEX ACCOUNT"           : "TCI_CHARGE_ACCT_MNEM",
    "TOTAL EUA"             : "total_eua",
    "MONTH"                 : "month_date",
    "OnGoingVoyageFlag"     : "reconciliation_flag",
    "VESSEL"                : "vessel",
    "OWNER"                 : "owners",
    "EU ETS OPTION"         : "EUA_settlement_type",
    "FROM PORT"             : "port",
    "SHIP CLASS"            : "TRADE",
    "port_leg_key"          : "port_leg_key",   # kept as-is, used as voyage key
    "boss_key"              : "boss_key",
    "CLASS"                 : "vessel_class",
    "VOYAGE NO"             : "voyage_no",
    "CONDITION"             : "condition",
    "CONTRACT TYPE"         : "contract_type",
    "TO PORT"               : "to_port",
    "PORT ACTIVITY"         : "port_activity",
    "START DATE"            : "start_date",
    "end_date"              : "end_date",
}

# Columns the app requires to be present AFTER mapping
REQUIRED_COLS = [
    "TCI_CHARGE_ACCT_MNEM",
    "total_eua",
    "month_date",
    "reconciliation_flag",
    "vessel",
]

OPTIONAL_COLS = [
    "owners",
    "EUA_settlement_type",
    "port",
    "to_port",
    "port_activity",
    "TRADE",
    "port_leg_key",
    "boss_key",
    "vessel_class",
    "voyage_no",
    "condition",
    "contract_type",
    "start_date",
    "end_date",
]

# ---------------------------------------------------------------------------
# RECONCILIATION STATUS MAPPING
# Raw file uses OnGoingVoyageFlag: "Finished" | "On Going"
# We map these to meaningful reconciliation labels.
# ---------------------------------------------------------------------------
RECON_MAP = {
    "finished" : "Reconciled",
    "ongoing"  : "UnReconciled",
    "on going" : "UnReconciled",
}

# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _normalise_col_names(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace from column names."""
    df.columns = [c.strip() for c in df.columns]
    return df


def _apply_column_map(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename raw CSV export columns to internal names.
    Columns already using internal names are left unchanged.
    """
    rename = {k: v for k, v in COLUMN_MAP.items() if k in df.columns}
    return df.rename(columns=rename)


def _parse_month_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse month_date to YYYY-MM string.
    Handles:
      - "January 2025"  (raw CSV format)
      - "2025-01-01"    (datetime-style)
      - "01/2025"       (short format)
    """
    if "month_date" not in df.columns:
        return df
    col = df["month_date"].astype(str).str.strip()
    parsed = pd.to_datetime(col, format="%B %Y", errors="coerce")
    mask_failed = parsed.isna()
    if mask_failed.any():
        parsed[mask_failed] = pd.to_datetime(col[mask_failed], errors="coerce", dayfirst=True)
    df["month_date"] = parsed.dt.strftime("%Y-%m")
    return df


def _map_reconciliation_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise reconciliation_flag.
    Raw values: 'Finished' → 'Reconciled' | 'On Going' → 'UnReconciled'
    Already-mapped values ('Reconciled', 'UnReconciled') are kept as-is.
    """
    if "reconciliation_flag" not in df.columns:
        return df
    df["reconciliation_flag"] = (
        df["reconciliation_flag"]
        .astype(str)
        .str.strip()
        .map(lambda v: RECON_MAP.get(v.lower(), v))
    )
    return df


def _fill_optional_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Add optional columns with NaN if not present."""
    for col in OPTIONAL_COLS:
        if col not in df.columns:
            df[col] = np.nan
    return df


def _coerce_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure total_eua is numeric.
    Handles values like '"2,726.30"' (quoted with commas) from CSV exports.
    """
    if "total_eua" in df.columns:
        df["total_eua"] = (
            df["total_eua"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace('"', "", regex=False)
            .str.strip()
        )
        df["total_eua"] = pd.to_numeric(df["total_eua"], errors="coerce").fillna(0.0)
    return df


def _build_voyage_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a stable voyage key for matching rows between snapshots.
    Priority:
      1. port_leg_key  — exact unique leg identifier from the raw CSV (best)
      2. boss_key + condition — fallback composite
      3. book + vessel + month + port — last resort
    """
    if "port_leg_key" in df.columns and df["port_leg_key"].notna().all():
        df["_voyage_key"] = df["port_leg_key"].astype(str).str.strip().str.lower()
    elif "boss_key" in df.columns:
        cond = df.get("condition", pd.Series(["_"] * len(df), index=df.index))
        df["_voyage_key"] = (
            df["boss_key"].astype(str).str.strip().str.lower()
            + "||"
            + cond.astype(str).str.strip().str.lower()
        )
    else:
        parts = []
        for col in ["TCI_CHARGE_ACCT_MNEM", "vessel", "month_date", "port"]:
            if col in df.columns:
                parts.append(df[col].astype(str).str.strip().str.lower())
            else:
                parts.append(pd.Series(["_"] * len(df), index=df.index))
        df["_voyage_key"] = parts[0]
        for p in parts[1:]:
            df["_voyage_key"] = df["_voyage_key"] + "||" + p
    return df


def validate_columns(df: pd.DataFrame, label: str) -> list[str]:
    """
    Apply column mapping first, then check for required columns.
    Returns list of missing required columns (empty = all good).
    """
    df_mapped = _apply_column_map(_normalise_col_names(df.copy()))
    return [c for c in REQUIRED_COLS if c not in df_mapped.columns]


# ---------------------------------------------------------------------------
# MAIN ANALYSIS FUNCTION
# ---------------------------------------------------------------------------

def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Full normalisation pipeline for one snapshot."""
    return (df
            .pipe(_normalise_col_names)
            .pipe(_apply_column_map)
            .pipe(_parse_month_date)
            .pipe(_map_reconciliation_flag)
            .pipe(_fill_optional_cols)
            .pipe(_coerce_numerics)
            .pipe(_build_voyage_key))


def run_analysis(prev_df: pd.DataFrame, curr_df: pd.DataFrame) -> dict:
    """
    Compare previous and current approval snapshots.

    Returns a dict with keys:
        - book_summary      : pd.DataFrame  — net change per book
        - voyage_changes    : pd.DataFrame  — row-level deltas
        - recon_movements   : pd.DataFrame  — reconciliation status shifts per book
        - stats             : dict          — headline numbers
    """

    prev_df = _prepare(prev_df)
    curr_df = _prepare(curr_df)

    # --- Voyage-level merge (join on the stable voyage key only) ---
    prev_slim = prev_df[["_voyage_key", "TCI_CHARGE_ACCT_MNEM", "vessel",
                          "month_date", "total_eua", "reconciliation_flag",
                          "owners", "EUA_settlement_type", "port",
                          "port_activity", "to_port", "condition",
                          "voyage_no", "vessel_class"]].rename(columns={
        "total_eua":           "prev_eua",
        "reconciliation_flag": "prev_recon_flag",
    })

    curr_slim = curr_df[["_voyage_key", "TCI_CHARGE_ACCT_MNEM", "vessel",
                          "month_date", "total_eua", "reconciliation_flag",
                          "owners", "EUA_settlement_type", "port",
                          "port_activity", "to_port", "condition",
                          "voyage_no", "vessel_class"]].rename(columns={
        "total_eua":           "curr_eua",
        "reconciliation_flag": "curr_recon_flag",
    })

    merged = pd.merge(
        prev_slim,
        curr_slim,
        on="_voyage_key",
        how="outer",
        suffixes=("_prev", "_curr"),
    )

    # Coalesce metadata columns (prefer current, fall back to previous)
    for col in ["TCI_CHARGE_ACCT_MNEM", "vessel", "month_date",
                "owners", "EUA_settlement_type", "port",
                "port_activity", "to_port", "condition",
                "voyage_no", "vessel_class"]:
        prev_col = f"{col}_prev" if f"{col}_prev" in merged.columns else col
        curr_col = f"{col}_curr" if f"{col}_curr" in merged.columns else col
        if prev_col in merged.columns and curr_col in merged.columns:
            merged[col] = merged[curr_col].combine_first(merged[prev_col])
            merged.drop(columns=[prev_col, curr_col], inplace=True, errors="ignore")
        elif prev_col in merged.columns:
            merged.rename(columns={prev_col: col}, inplace=True)
        elif curr_col in merged.columns:
            merged.rename(columns={curr_col: col}, inplace=True)

    merged["prev_eua"] = merged["prev_eua"].fillna(0.0)
    merged["curr_eua"] = merged["curr_eua"].fillna(0.0)
    merged["prev_recon_flag"] = merged["prev_recon_flag"].fillna("—")
    merged["curr_recon_flag"] = merged["curr_recon_flag"].fillna("—")
    merged["TCI_CHARGE_ACCT_MNEM"] = merged["TCI_CHARGE_ACCT_MNEM"].fillna("UNKNOWN")

    # --- Delta and change type ---
    merged["delta_eua"] = merged["curr_eua"] - merged["prev_eua"]

    def _classify(row):
        if row["prev_eua"] == 0 and row["curr_eua"] != 0:
            return "✨ New Voyage"
        if row["curr_eua"] == 0 and row["prev_eua"] != 0:
            return "❌ Removed Voyage"
        if row["prev_recon_flag"] != row["curr_recon_flag"]:
            if abs(row["delta_eua"]) > 0.01:
                return "🔄 Recon Status Changed + EUA Changed"
            return "🔄 Recon Status Changed Only"
        if abs(row["delta_eua"]) > 0.01:
            return "✏️ EUA Value Changed"
        return "✅ No Change"

    merged["change_type"] = merged.apply(_classify, axis=1)

    # --- Voyage changes (exclude no-change rows) ---
    voyage_changes = merged[merged["change_type"] != "✅ No Change"].copy()
    voyage_changes = voyage_changes.sort_values(
        ["TCI_CHARGE_ACCT_MNEM", "month_date", "delta_eua"],
        ascending=[True, True, False],
        key=lambda col: col.abs() if col.name == "delta_eua" else col
    )

    voyage_changes = voyage_changes[[
        "TCI_CHARGE_ACCT_MNEM", "vessel", "vessel_class", "month_date",
        "port", "to_port", "port_activity", "condition",
        "owners", "EUA_settlement_type",
        "prev_eua", "curr_eua", "delta_eua",
        "prev_recon_flag", "curr_recon_flag", "change_type",
    ]].rename(columns={
        "TCI_CHARGE_ACCT_MNEM":  "Book",
        "vessel":                "Vessel",
        "vessel_class":          "Class",
        "month_date":            "Month",
        "port":                  "From Port",
        "to_port":               "To Port",
        "port_activity":         "Activity",
        "condition":             "Condition",
        "owners":                "Owner",
        "EUA_settlement_type":   "Settlement Type",
        "prev_eua":              "Prev EUAs",
        "curr_eua":              "Curr EUAs",
        "delta_eua":             "Delta EUAs",
        "prev_recon_flag":       "Prev Status",
        "curr_recon_flag":       "Curr Status",
        "change_type":           "Change Type",
    })

    # --- Book summary ---
    prev_book = (prev_df.groupby("TCI_CHARGE_ACCT_MNEM")["total_eua"]
                 .sum().rename("Prev Total EUAs"))
    curr_book = (curr_df.groupby("TCI_CHARGE_ACCT_MNEM")["total_eua"]
                 .sum().rename("Curr Total EUAs"))

    book_summary = pd.concat([prev_book, curr_book], axis=1).fillna(0.0)
    book_summary["Net Change"] = book_summary["Curr Total EUAs"] - book_summary["Prev Total EUAs"]
    book_summary["% Change"] = np.where(
        book_summary["Prev Total EUAs"] != 0,
        (book_summary["Net Change"] / book_summary["Prev Total EUAs"] * 100).round(1),
        np.nan,
    )

    # Count changed voyages per book
    if not voyage_changes.empty:
        changed_counts = (voyage_changes
                          .groupby("Book")
                          .size()
                          .rename("# Voyages Changed"))
        book_summary = book_summary.join(changed_counts, how="left")
    else:
        book_summary["# Voyages Changed"] = 0

    book_summary["# Voyages Changed"] = (book_summary["# Voyages Changed"]
                                          .fillna(0).astype(int))
    book_summary = book_summary.reset_index().rename(
        columns={"TCI_CHARGE_ACCT_MNEM": "Book"})
    book_summary = book_summary.sort_values("Net Change",
                                            key=lambda x: x.abs(),
                                            ascending=False)

    # --- Reconciliation movements per book ---
    recon_data = []
    for book in merged["TCI_CHARGE_ACCT_MNEM"].dropna().unique():
        sub = merged[merged["TCI_CHARGE_ACCT_MNEM"] == book]

        def _is_rec(s):
            return s.str.lower().str.strip() == "reconciled"

        def _is_unrec(s):
            return s.str.lower().str.strip() == "unreconciled"

        unrec_to_rec = int((_is_unrec(sub["prev_recon_flag"]) & _is_rec(sub["curr_recon_flag"])).sum())
        rec_to_unrec = int((_is_rec(sub["prev_recon_flag"]) & _is_unrec(sub["curr_recon_flag"])).sum())
        new_voyages  = int((sub["prev_eua"] == 0).sum())
        removed      = int((sub["curr_eua"] == 0).sum())
        eua_impact   = sub.loc[
            _is_unrec(sub["prev_recon_flag"]) & _is_rec(sub["curr_recon_flag"]),
            "delta_eua"
        ].sum()

        recon_data.append({
            "Book":                    book,
            "Unrec → Rec":             unrec_to_rec,
            "Rec → Unrec":             rec_to_unrec,
            "New Voyages":             new_voyages,
            "Removed Voyages":         removed,
            "EUA Impact (Unrec→Rec)":  round(eua_impact, 1),
        })

    recon_movements = pd.DataFrame(recon_data)
    recon_movements = recon_movements[
        (recon_movements["Unrec → Rec"] > 0) |
        (recon_movements["Rec → Unrec"] > 0) |
        (recon_movements["New Voyages"] > 0) |
        (recon_movements["Removed Voyages"] > 0)
    ].reset_index(drop=True)

    # --- Headline stats ---
    total_prev = prev_df["total_eua"].sum()
    total_curr = curr_df["total_eua"].sum()
    stats = {
        "total_prev_eua":    round(total_prev, 1),
        "total_curr_eua":    round(total_curr, 1),
        "total_delta":       round(total_curr - total_prev, 1),
        "books_changed":     int((book_summary["Net Change"].abs() > 0.01).sum()),
        "voyages_changed":   len(voyage_changes),
        "new_voyages":       int((merged["prev_eua"] == 0).sum()),
        "removed_voyages":   int((merged["curr_eua"] == 0).sum()),
        "recon_flips":       int(
            merged[
                (merged["prev_recon_flag"] != merged["curr_recon_flag"]) &
                (merged["prev_recon_flag"] != "—") &
                (merged["curr_recon_flag"] != "—")
            ].shape[0]
        ),
    }

    return {
        "book_summary":   book_summary,
        "voyage_changes": voyage_changes,
        "recon_movements": recon_movements,
        "stats":          stats,
    }


# ---------------------------------------------------------------------------
# EXCEL EXPORT
# ---------------------------------------------------------------------------

def build_excel_report(results: dict) -> bytes:
    """
    Write all three result tables into a multi-sheet Excel file.
    Returns raw bytes suitable for st.download_button.
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book

        # Formats
        header_fmt = workbook.add_format({
            "bold": True, "bg_color": "#003366", "font_color": "#FFFFFF",
            "border": 1, "align": "center",
        })
        red_fmt   = workbook.add_format({"font_color": "#CC0000", "num_format": "#,##0.0"})
        green_fmt = workbook.add_format({"font_color": "#006600", "num_format": "#,##0.0"})
        num_fmt   = workbook.add_format({"num_format": "#,##0.0"})
        pct_fmt   = workbook.add_format({"num_format": "0.0\"%\""})

        for sheet_name, df_key in [
            ("Book Summary", "book_summary"),
            ("Voyage Changes", "voyage_changes"),
            ("Recon Movements", "recon_movements"),
        ]:
            df = results[df_key]
            if df.empty:
                df = pd.DataFrame({"Info": ["No data for this section."]})
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            worksheet = writer.sheets[sheet_name]
            # Auto-fit columns
            for i, col in enumerate(df.columns):
                col_width = max(len(str(col)), df[col].astype(str).str.len().max()) + 2
                worksheet.set_column(i, i, min(col_width, 40))

    output.seek(0)
    return output.read()
