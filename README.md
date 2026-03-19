# EUA Delta Analyser

Instantly compare two EUA approval snapshots to find what changed, which voyages drove it, and why.

---

## Setup (one-time)

### 1. Install Python
Make sure Python 3.10+ is installed. Check with:
```powershell
python --version
```

### 2. Install dependencies
```powershell
cd C:\Users\Yafet.Wolde\eua-delta-analyser
pip install -r requirements.txt
```

---

## How to Run

```powershell
cd C:\Users\Yafet.Wolde\eua-delta-analyser
streamlit run app.py
```

A browser window will open automatically at `http://localhost:8501`.

---

## How to Use

1. In the **sidebar**, upload:
   - **Previous Approval File** — last month's snapshot (CSV or Excel)
   - **Current Approval File** — this month's snapshot (CSV or Excel)
2. Click **⚡ Run Analysis**
3. Review the three tabs:
   - **📊 Book Summary** — net EUA change per book
   - **🔍 Voyage Changes** — every row that changed, from what to what
   - **🔄 Reconciliation Movements** — voyages that flipped status and their EUA impact
4. Download the full **Excel report** with all three sheets

---

## Required Columns in Input Files

| Column | Description |
|--------|-------------|
| `TCI_CHARGE_ACCT_MNEM` | Book/charge account identifier |
| `total_eua` | EUA exposure value for the leg |
| `month_date` | Month of the voyage/exposure |
| `reconciliation_flag` | `Reconciled` or `UnReconciled` |
| `vessel` | Vessel name |

**Optional (used when available):**
`owners`, `port`, `EUA_settlement_type`, `DEX_MT_AMT`, `TRADE`

---

## Change Type Legend

| Icon | Meaning |
|------|---------|
| ✨ New Voyage | Voyage exists in current file only |
| ❌ Removed Voyage | Voyage existed in previous file only |
| 🔄 Recon Status Changed + EUA Changed | Status AND EUA value both changed |
| 🔄 Recon Status Changed Only | Status changed, EUA value unchanged |
| ✏️ EUA Value Changed | EUA value changed, status unchanged |

---

## Files

```
eua-delta-analyser/
├── app.py          # Streamlit UI
├── analyser.py     # Core comparison logic
├── requirements.txt
└── README.md
```
