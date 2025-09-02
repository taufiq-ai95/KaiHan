# ===============================================================
# Full Python program - SAS to Python/Polars conversion
# ===============================================================

import polars as pl
from datetime import datetime

# -------------------------------------------------------------------
# Part 0: Read all parquet files
# -------------------------------------------------------------------
cis = pl.read_parquet("CUSTDAILY.parquet")      # CISFILE.CUSTDLY
indv = pl.read_parquet("DAILYINDV.parquet")     # INDFILE.INDVDLY
demo = pl.read_parquet("DEMOCODE.parquet")      # DEMOFILE

# -------------------------------------------------------------------
# Part 1: Date & Time Setup
# -------------------------------------------------------------------
now = datetime.now()
TIMEX = now.strftime("%H%M%S")      # Equivalent to SAS TIME() macro
DATE1  = now.strftime("%Y%m%d")
YEAR   = now.strftime("%Y")
MONTH  = now.strftime("%m")
DAY    = now.strftime("%d")

# -------------------------------------------------------------------
# Part 2: CIS - Customer Daily Handling
# -------------------------------------------------------------------

# HRC01–HRC20 → convert numeric to string Z3 format, then concatenate
hrc_cols = [f"HRC{i:02d}" for i in range(1, 21)]
for col in hrc_cols:
    cis = cis.with_columns(
        pl.col(col)
        .cast(pl.Int64)                    # Ensure numeric
        .fill_null(0)                      # Fill nulls with 0
        .cast(pl.Utf8)                     # Convert to string
        .str.zfill(3)                      # Pad to 3 digits
        .alias(col + "C")
    )

cis = cis.with_columns(
    pl.concat_str([pl.col(c + "C") for c in hrc_cols]).alias("HRCALL")
)

# Phone and reference numbers → 11 digits with leading zeros
for col in ["PRIPHONE", "SECPHONE", "MOBILEPH", "FAX", "ADDREF"]:
    cis = cis.with_columns(
        pl.col(col)
        .cast(pl.Int64)
        .fill_null(0)
        .cast(pl.Utf8)
        .str.zfill(11)
        .alias(col + "X")
    )

# Open Date cleaning
cis = cis.with_columns(
    pl.col("CUSTOPENDATE")
    .cast(pl.Utf8)
    .str.replace_all(" ", "0")           # Replace spaces with '0'
    .alias("CUSTOPEN")
)

# Slice into components
cis = cis.with_columns([
    pl.col("CUSTOPEN").str.slice(0,2).alias("OPENMM"),
    pl.col("CUSTOPEN").str.slice(2,2).alias("OPENDD"),
    pl.col("CUSTOPEN").str.slice(4,4).alias("OPENYY")
])

# Concatenate to YYYYMMDD format
cis = cis.with_columns(
    (pl.col("OPENYY") + pl.col("OPENMM") + pl.col("OPENDD")).alias("OPENDT")
)

# Special handling: if dummy value '00002000000' → '20000101'
cis = cis.with_columns(
    pl.when(pl.col("CUSTOPEN") == "00002000000")
    .then(pl.lit("20000101"))
    .otherwise(pl.col("OPENDT"))
    .alias("OPENDT")
)

# CUSTNOX = CUSTNO
cis = cis.with_columns(pl.col("CUSTNO").alias("CUSTNOX"))

# Remove duplicates by CUSTNOX
cis = cis.unique(subset=["CUSTNOX"])

# -------------------------------------------------------------------
# Part 3: DEMOFILE → SALES, RESTR, CITZN
# -------------------------------------------------------------------

# RESTR
restr = (
    demo.filter(pl.col("DEMOCATEGORY") == "RESTR")
        .select(RESTR = pl.col("DEMOCODE"), RESDESC = pl.col("CODEDESC"))
        .unique(subset=["RESTR"])
)

# SALES
sales = (
    demo.filter(pl.col("DEMOCATEGORY") == "SALES")
        .select(SALES = pl.col("DEMOCODE"), SALDESC = pl.col("CODEDESC"))
        .unique(subset=["SALES"])
)

# CITZN
citzn = (
    demo.filter(pl.col("DEMOCATEGORY") == "CITZN")
        .select(CITZN = pl.col("DEMOCODX"), CTZDESC = pl.col("CODEDESC"))
        .unique(subset=["CITZN"])
)

# -------------------------------------------------------------------
# Part 4: INDV – Individual Data
# -------------------------------------------------------------------

# Deduplicate by latest LAST_UPDATE_DATE
indv = (
    indv.sort(["CUSTNO", "LAST_UPDATE_DATE"], descending=[False, True])
         .unique(subset=["CUSTNO"], keep="first")
)

# Remove rows with empty CUSTNO
indvx = indv.filter(pl.col("CUSTNO").str.strip_chars().is_not_null()).with_columns(
    pl.col("CUSTNO").alias("CUSTNOX")
)

# -------------------------------------------------------------------
# Part 5: Merge CIS + INDV + DEMO categories
# -------------------------------------------------------------------

# Merge CIS + INDV
mrgcis = cis.join(indvx, on="CUSTNOX", how="left")

# Add runtime timestamp and assign RESTR, SALES, CITZN
mrgcis = mrgcis.with_columns(
    pl.lit(YEAR + MONTH + DAY + TIMEX).alias("RUNTIMESTAMP"),
    pl.col("RESIDENCY").alias("RESTR"),
    pl.col("CORPSTATUS").alias("SALES"),
    pl.col("CITIZENSHIP").alias("CITZN")
)

# Merge with RESTR
mrgres = mrgcis.join(restr, on="RESTR", how="left")

# Merge with SALES
mrgsal = mrgres.join(sales, on="SALES", how="left")

# Merge with CITZN
mrgctz = mrgsal.join(citzn, on="CITZN", how="left")

# Sort by CUSTNOX
mrgctz = mrgctz.sort("CUSTNOX")

# -------------------------------------------------------------------
# Part 6: Output to fixed-width text file
# -------------------------------------------------------------------

def format_row(row: dict) -> str:
    """
    Format a single row as fixed-width string, following SAS PUT layout.
    Only sample fields shown; add remaining fields as needed.
    """
    return (
        f"{row['RUNTIMESTAMP']:<20}"
        f"{row['CUSTNOX']:<20}"
        f"{row['ADDREFX']:<11}"
        f"{row['CUSTNAME']:<40}"
        f"{row['PRIPHONEX']:<11}"
        f"{row['SECPHONEX']:<11}"
        f"{row['MOBILEPHX']:<11}"
        f"{row['FAXX']:<11}"
        f"{row.get('ALIASKEY',''):<3}"
        f"{row.get('ALIAS',''):<20}"
        f"{row.get('PROCESSTIME',''):<8}"
        f"{row.get('CUSTSTAT',''):<1}"
        f"{row.get('TAXCODE',''):<1}"
        f"{row.get('TAXID',''):<9}"
        # ... continue for all SAS PUT fields
    )

with open("COMBINECUSTALL.txt", "w", encoding="utf-8") as f:
    for row in mrgctz.to_dicts():
        f.write(format_row(row) + "\n")
