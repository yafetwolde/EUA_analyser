"""
app.py
EUA Delta Analyser — Streamlit frontend.
Compares two EUA approval snapshots and surfaces what changed,
which voyages drove the change, and reconciliation movements.
"""

import streamlit as st
import pandas as pd
import numpy as np
from analyser import (
    run_analysis,
    validate_columns,
    build_excel_report,
    REQUIRED_COLS,
)

# ---------------------------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="EUA Delta Analyser",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# PASSWORD PROTECTION
# ---------------------------------------------------------------------------
def check_password():
    """Returns `True` if the user had the correct password."""
    def password_entered():
        if st.session_state["password"] == "ShellEUA2026":  # ⚠️ Change this password!
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown('<div class="main-title">⚡ EUA Delta Analyser</div>', unsafe_allow_html=True)
        st.markdown('<div class="subtitle">Secure access required</div>', unsafe_allow_html=True)
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.info("👋 Enter the password to access the EUA Delta Analyser")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown('<div class="main-title">⚡ EUA Delta Analyser</div>', unsafe_allow_html=True)
        st.markdown('<div class="subtitle">Secure access required</div>', unsafe_allow_html=True)
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.error("😕 Password incorrect — please try again")
        return False
    else:
        return True

if not check_password():
    st.stop()

# ---------------------------------------------------------------------------
# STYLES
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .main-title {
        font-size: 2rem;
        font-weight: 700;
        color: #003366;
    }
    .subtitle {
        color: #555;
        margin-top: -10px;
        margin-bottom: 20px;
    }
    .metric-card {
        background: #f0f4ff;
        border-left: 4px solid #003366;
        border-radius: 6px;
        padding: 12px 16px;
        margin-bottom: 8px;
    }
    .metric-label { font-size: 0.78rem; color: #666; font-weight: 600; text-transform: uppercase; }
    .metric-value { font-size: 1.5rem; font-weight: 700; color: #003366; }
    .metric-delta-up   { color: #CC0000; font-size: 0.9rem; font-weight: 600; }
    .metric-delta-down { color: #006600; font-size: 0.9rem; font-weight: 600; }
    .metric-delta-zero { color: #888;    font-size: 0.9rem; }
    .info-box {
        background: #fffbe6;
        border-left: 4px solid #f0a500;
        border-radius: 6px;
        padding: 10px 14px;
        margin-bottom: 12px;
        font-size: 0.88rem;
    }
    .section-header {
        font-size: 1.1rem;
        font-weight: 700;
        color: #003366;
        margin-top: 12px;
        margin-bottom: 4px;
    }
    div[data-testid="stDataFrame"] { border-radius: 6px; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def load_file(uploaded) -> pd.DataFrame | None:
    """Load CSV or Excel into a DataFrame."""
    if uploaded is None:
        return None
    try:
        if uploaded.name.endswith(".csv"):
            return pd.read_csv(uploaded)
        else:
            return pd.read_excel(uploaded, engine="openpyxl")
    except Exception as e:
        st.error(f"Could not read **{uploaded.name}**: {e}")
        return None


def colour_delta(val):
    """Streamlit styler: red for positive delta (more exposure), green for negative."""
    try:
        v = float(val)
        if v > 0:
            return "color: #CC0000; font-weight: 600;"
        if v < 0:
            return "color: #006600; font-weight: 600;"
    except Exception:
        pass
    return ""


def colour_change_type(val):
    """Colour the Change Type column."""
    mapping = {
        "✨ New Voyage":                          "color: #0055cc;",
        "❌ Removed Voyage":                      "color: #888888;",
        "🔄 Recon Status Changed + EUA Changed":  "color: #b35900; font-weight:600;",
        "🔄 Recon Status Changed Only":           "color: #b35900;",
        "✏️ EUA Value Changed":                   "color: #9900cc;",
    }
    return mapping.get(val, "")


def fmt_eua(v):
    """Format a number as EUA with commas."""
    try:
        return f"{float(v):,.1f}"
    except Exception:
        return str(v)


def delta_arrow(v):
    try:
        f = float(v)
        if f > 0:
            return f"▲ +{f:,.1f}"
        if f < 0:
            return f"▼ {f:,.1f}"
        return "— 0.0"
    except Exception:
        return str(v)


# ---------------------------------------------------------------------------
# SIDEBAR — FILE UPLOAD
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### 📂 Upload Approval Files")
    st.markdown("Upload two EUA approval snapshots to compare.")

    prev_file = st.file_uploader(
        "Previous Approval File",
        type=["csv", "xlsx", "xls"],
        key="prev",
        help="The snapshot sent to RTLs last month.",
    )
    curr_file = st.file_uploader(
        "Current Approval File",
        type=["csv", "xlsx", "xls"],
        key="curr",
        help="The snapshot being sent to RTLs this month.",
    )

    run_btn = st.button("⚡ Run Analysis", type="primary", use_container_width=True)

    st.divider()
    st.markdown("**Required columns** *(auto-detected from raw export):*")
    st.markdown("""
    | Raw CSV Column | Meaning |
    |---|---|
    | `DEX ACCOUNT` | Book |
    | `TOTAL EUA` | EUA exposure |
    | `MONTH` | Month (e.g. *January 2025*) |
    | `OnGoingVoyageFlag` | Finished / On Going |
    | `VESSEL` | Vessel name |
    """)
    st.markdown("*Also works with internal column names (`TCI_CHARGE_ACCT_MNEM`, `total_eua`, etc.)*")


# ---------------------------------------------------------------------------
# MAIN AREA
# ---------------------------------------------------------------------------
st.markdown('<div class="main-title">⚡ EUA Delta Analyser</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="subtitle">Compare two EUA approval snapshots — instantly see what changed, '
    'which voyages drove it, and why.</div>',
    unsafe_allow_html=True,
)

if not run_btn:
    # --- Landing state ---
    st.markdown("""
    <div class="info-box">
    👈 Upload your <strong>Previous</strong> and <strong>Current</strong> approval files 
    in the sidebar, then click <strong>⚡ Run Analysis</strong>.
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("""
        **📊 Book Summary**
        See net EUA change per book at a glance.
        """)
    with col2:
        st.markdown("""
        **🔍 Voyage Changes**
        Drill into every row that changed — from what to what, and why.
        """)
    with col3:
        st.markdown("""
        **🔄 Reconciliation Movements**
        Track how many voyages flipped from Unreconciled → Reconciled and the EUA impact.
        """)
    st.stop()

# --- Load files ---
if prev_file is None or curr_file is None:
    st.warning("⚠️ Please upload both files before running the analysis.")
    st.stop()

prev_df = load_file(prev_file)
curr_df = load_file(curr_file)

if prev_df is None or curr_df is None:
    st.stop()

# --- Validate columns ---
prev_missing = validate_columns(prev_df, "Previous")
curr_missing = validate_columns(curr_df, "Current")

if prev_missing:
    st.error(f"**Previous file** is missing required columns: `{'`, `'.join(prev_missing)}`")
    st.stop()
if curr_missing:
    st.error(f"**Current file** is missing required columns: `{'`, `'.join(curr_missing)}`")
    st.stop()

# --- Run analysis ---
with st.spinner("Analysing changes..."):
    results = run_analysis(prev_df.copy(), curr_df.copy())

book_summary    = results["book_summary"]
voyage_changes  = results["voyage_changes"]
recon_movements = results["recon_movements"]
stats           = results["stats"]

# ---------------------------------------------------------------------------
# HEADLINE METRICS
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown('<div class="section-header">📌 Headline Summary</div>', unsafe_allow_html=True)

c1, c2, c3, c4, c5, c6 = st.columns(6)

def _metric(col, label, value, delta=None, delta_reverse=False):
    """Render a styled metric card."""
    delta_html = ""
    if delta is not None:
        try:
            d = float(delta)
            if d > 0:
                cls  = "metric-delta-up" if not delta_reverse else "metric-delta-down"
                sign = f"▲ +{d:,.1f}"
            elif d < 0:
                cls  = "metric-delta-down" if not delta_reverse else "metric-delta-up"
                sign = f"▼ {d:,.1f}"
            else:
                cls, sign = "metric-delta-zero", "— 0.0"
            delta_html = f'<div class="{cls}">{sign}</div>'
        except Exception:
            delta_html = f'<div class="metric-delta-zero">{delta}</div>'

    col.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)

_metric(c1, "Prev Total EUAs",    f"{stats['total_prev_eua']:,.1f}")
_metric(c2, "Curr Total EUAs",    f"{stats['total_curr_eua']:,.1f}",
        delta=stats['total_delta'])
_metric(c3, "Net EUA Change",     delta_arrow(stats['total_delta']))
_metric(c4, "Books Changed",      stats['books_changed'])
_metric(c5, "Voyages Changed",    stats['voyages_changed'])
_metric(c6, "Recon Flips",        stats['recon_flips'])

st.markdown("---")

# ---------------------------------------------------------------------------
# FILTERS
# ---------------------------------------------------------------------------
all_books = sorted(
    set(book_summary["Book"].tolist()) |
    (set(voyage_changes["Book"].tolist()) if not voyage_changes.empty else set())
)
selected_books = st.multiselect(
    "🔍 Filter by Book (leave blank for all)",
    options=all_books,
    default=[],
    placeholder="All books",
)

def apply_book_filter(df, col="Book"):
    if selected_books and col in df.columns:
        return df[df[col].isin(selected_books)]
    return df

# ---------------------------------------------------------------------------
# TABS
# ---------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs([
    "📊 Book Summary",
    "🔍 Voyage Changes",
    "🔄 Reconciliation Movements",
])

# ── TAB 1: BOOK SUMMARY ────────────────────────────────────────────────────
with tab1:
    st.markdown('<div class="section-header">Net EUA Change by Book</div>',
                unsafe_allow_html=True)

    filtered_bs = apply_book_filter(book_summary)

    if filtered_bs.empty:
        st.info("No book data found.")
    else:
        # Render with colour styling
        styled = (
            filtered_bs.style
            .format({
                "Prev Total EUAs": "{:,.1f}",
                "Curr Total EUAs": "{:,.1f}",
                "Net Change":      "{:,.1f}",
                "% Change":        lambda x: f"{x:.1f}%" if pd.notna(x) else "N/A",
            })
            .applymap(colour_delta, subset=["Net Change"])
            .set_properties(**{"text-align": "right"}, subset=["Prev Total EUAs",
                                                                "Curr Total EUAs",
                                                                "Net Change", "% Change"])
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)

        # Mini bar chart of net change
        chart_data = filtered_bs.set_index("Book")[["Net Change"]].sort_values("Net Change")
        st.markdown("**Net Change per Book (EUAs)**")
        st.bar_chart(chart_data)


# ── TAB 2: VOYAGE CHANGES ──────────────────────────────────────────────────
with tab2:
    st.markdown('<div class="section-header">Voyage-Level Changes</div>',
                unsafe_allow_html=True)

    filtered_vc = apply_book_filter(voyage_changes)

    if filtered_vc.empty:
        st.success("✅ No voyage-level changes detected between the two files.")
    else:
        # Sub-filter: change type
        change_types = filtered_vc["Change Type"].unique().tolist()
        selected_types = st.multiselect(
            "Filter by Change Type",
            options=change_types,
            default=change_types,
            key="change_type_filter",
        )
        filtered_vc = filtered_vc[filtered_vc["Change Type"].isin(selected_types)]

        st.caption(f"Showing **{len(filtered_vc)}** changed voyage legs")

        # Columns to display — drop empty optional ones gracefully
        display_cols = [c for c in [
            "Book", "Vessel", "Class", "Month", "From Port", "To Port",
            "Activity", "Condition", "Owner", "Settlement Type",
            "Prev EUAs", "Curr EUAs", "Delta EUAs",
            "Prev Status", "Curr Status", "Change Type",
        ] if c in filtered_vc.columns]

        styled_vc = (
            filtered_vc[display_cols].style
            .format({
                "Prev EUAs":  "{:,.1f}",
                "Curr EUAs":  "{:,.1f}",
                "Delta EUAs": "{:+,.1f}",
            })
            .applymap(colour_delta,       subset=["Delta EUAs"])
            .applymap(colour_change_type, subset=["Change Type"])
        )
        st.dataframe(styled_vc, use_container_width=True, hide_index=True)

        # Per-book delta breakdown inside tab2
        st.markdown("---")
        st.markdown("**Delta EUAs by Book (changed voyages only)**")
        book_delta = (
            filtered_vc.groupby("Book")["Delta EUAs"]
            .sum()
            .sort_values(key=lambda x: x.abs(), ascending=False)
            .reset_index()
        )
        book_delta["Delta EUAs (fmt)"] = book_delta["Delta EUAs"].apply(delta_arrow)
        st.dataframe(
            book_delta[["Book", "Delta EUAs", "Delta EUAs (fmt)"]].rename(
                columns={"Delta EUAs (fmt)": "Change"}),
            use_container_width=True,
            hide_index=True,
        )


# ── TAB 3: RECONCILIATION MOVEMENTS ────────────────────────────────────────
with tab3:
    st.markdown('<div class="section-header">Reconciliation Status Movements</div>',
                unsafe_allow_html=True)

    st.markdown("""
    <div class="info-box">
    This table shows books where voyage reconciliation status changed between snapshots.
    <strong>Unrec → Rec</strong> is the most common driver of EUA value changes.
    </div>
    """, unsafe_allow_html=True)

    filtered_rm = apply_book_filter(recon_movements)

    if filtered_rm.empty:
        st.success("✅ No reconciliation movements detected.")
    else:
        styled_rm = (
            filtered_rm.style
            .format({"EUA Impact (Unrec→Rec)": "{:+,.1f}"})
            .applymap(colour_delta, subset=["EUA Impact (Unrec→Rec)"])
        )
        st.dataframe(styled_rm, use_container_width=True, hide_index=True)

        # Narrative summary
        st.markdown("---")
        st.markdown("**📝 Plain-English Summary**")
        for _, row in filtered_rm.iterrows():
            lines = []
            if row["Unrec → Rec"] > 0:
                lines.append(
                    f"- **{row['Book']}**: {row['Unrec → Rec']} voyage(s) moved "
                    f"Unreconciled → Reconciled, EUA impact: **{row['EUA Impact (Unrec→Rec)']:+,.1f}**"
                )
            if row["Rec → Unrec"] > 0:
                lines.append(
                    f"- **{row['Book']}**: {row['Rec → Unrec']} voyage(s) moved "
                    f"Reconciled → Unreconciled ⚠️"
                )
            if row["New Voyages"] > 0:
                lines.append(f"- **{row['Book']}**: {row['New Voyages']} new voyage(s) added ✨")
            if row["Removed Voyages"] > 0:
                lines.append(
                    f"- **{row['Book']}**: {row['Removed Voyages']} voyage(s) removed ❌"
                )
            for line in lines:
                st.markdown(line)


# ---------------------------------------------------------------------------
# DOWNLOAD REPORT
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown('<div class="section-header">📥 Download Full Report</div>',
            unsafe_allow_html=True)

excel_bytes = build_excel_report(results)
st.download_button(
    label="⬇️ Download Excel Report",
    data=excel_bytes,
    file_name="EUA_Delta_Report.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True,
)
