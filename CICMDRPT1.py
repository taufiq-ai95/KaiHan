import polars as pl
from datetime import datetime

# -----------------------------------
# Step 1: Set current date/time
# -----------------------------------
today = datetime.now()
LOADDATE = today.strftime("%Y-%m-%d")  # equivalent to SAS YYMMDD10.
print(f"Load date: {LOADDATE}")

# -----------------------------------
# Step 2: Read input files (replace CTRLDATE SAS file)
# -----------------------------------
ALIASFL   = pl.read_parquet("ALIAS.parquet")
PBBBRCH   = pl.read_parquet("PBBRANCH.parquet")
OCCUPAT   = pl.read_parquet("OCCUPFL.parquet")
MASCO     = pl.read_parquet("MASCOFL.parquet")
MSIC      = pl.read_parquet("MSICFL.parquet")
CYCLEFL   = pl.read_parquet("INQFILE.parquet")
POSTFL    = pl.read_parquet("INDEXT.parquet")
DEPOFL    = pl.read_parquet("TRANSPO.parquet")
CISFILE   = pl.read_parquet("CUSTDAILY.parquet")
DPTRBALS  = pl.read_parquet("DPTRBLGS.parquet")
SAFEBOX   = pl.read_parquet("SDBX.parquet")
ACCTFILE  = pl.read_parquet("ACCTFILE.parquet")
UNICARD   = pl.read_parquet("PBCS.parquet")
COMCARD   = pl.read_parquet("COMCARD.parquet")

# -----------------------------------
# Step 3: Process ALIAS file
# -----------------------------------
ALIASFL = ALIASFL.sort(["ALIASKEY", "ALIAS"])
print(ALIASFL.head(10))

# -----------------------------------
# Step 4: Process PBBBRCH
# -----------------------------------
PBBBRCH = PBBBRCH.unique(subset=["ACCTBRCH"])
PBBBRCH = PBBBRCH.sort("ACCTBRCH")
print(PBBBRCH.head())

# -----------------------------------
# Step 5: Process OCCUP file
# -----------------------------------
OCCUPAT = OCCUPAT.filter(pl.col("TYPE") == "OCCUP").sort("DEMOCODE")
print(OCCUPAT.head())

# -----------------------------------
# Step 6: Process MASCO file
# -----------------------------------
MASCO = MASCO.sort("MASCO2008")
print(MASCO.head())

# -----------------------------------
# Step 7: Process MSIC file
# -----------------------------------
MSIC = MSIC.sort("MSICCODE")
print(MSIC.head())

# -----------------------------------
# Step 8: Process CYCLEFL (DPSTMT)
# -----------------------------------
def zero_pad_acct(acct):
    return str(acct).zfill(11)

DPSTMT = CYCLEFL.with_columns([
    pl.col("ACCTNO").apply(zero_pad_acct).alias("ACCTNOC")
])
DPSTMT = DPSTMT.sort("ACCTNOC")
print(DPSTMT.head())

# -----------------------------------
# Step 9: Process DPPOST
# -----------------------------------
DPPOST = POSTFL.sort("ACCTNOC")
print(DPPOST.head())

# -----------------------------------
# Step 10: Process DEPOSIT1
# -----------------------------------
DEPOSIT1 = DEPOFL.with_columns([
    pl.col("ACCTNO").apply(zero_pad_acct).alias("ACCTNOC")
]).sort("ACCTNOC")
print(DEPOSIT1.head())

# -----------------------------------
# Step 11: Process CIS
# -----------------------------------
CISFILE = CISFILE.with_columns([
    pl.col("OCCUP").alias("DEMOCODE")
])
CISFILE = CISFILE.sort(["ALIASKEY", "ALIAS"])

# Merge ALIAS with CIS
MERGEALS = ALIASFL.join(CISFILE, on=["ALIASKEY", "ALIAS"], how="inner")
MERGEOCC = MERGEALS.join(OCCUPAT, on="DEMOCODE", how="left")
MERGEMSC = MERGEOCC.join(MASCO, on="MASCO2008", how="left")
MERGEALL1 = MERGEMSC.join(MSIC, on="MSICCODE", how="left")
MERGEALL1 = MERGEALL1.sort("ALIAS")
print(MERGEALL1.head(10))

# -----------------------------------
# Step 12: SAFEBOX2 processing
# -----------------------------------
SAFEBOX2 = SAFEBOX.with_columns([
    pl.lit(0).alias("LEDGERBAL"),
    pl.lit("3").alias("CATEGORY"),
    pl.lit("SDB").alias("APPL_CODE")
]).sort("ALIAS")
print(SAFEBOX2.head(10))

# Merge SAFEBOX with main data
MERGEALL = MERGEALL1.join(SAFEBOX2, on="ALIAS", how="left").with_columns([
    pl.when(pl.col("CUSTNAME_SDB").is_not_null()).then("YES").otherwise("NIL").alias("SDBIND"),
    pl.col("BRANCH_ABBR").fill_null("NIL").alias("SDBBRH")
])
MERGEALL = MERGEALL.unique(subset=["ACCTNOC"])
print(MERGEALL.head(10))

# -----------------------------------
# Step 13: Merge DPTRBALS and branch info
# -----------------------------------
DPTRBALS = DPTRBALS.sort("ACCTBRCH")
MERGEBRCH = DPTRBALS.join(PBBBRCH, on="ACCTBRCH", how="left").sort("ACCTNOC")

# -----------------------------------
# Step 14: Merge deposits
# -----------------------------------
MERGEDP = MERGEALL.join(MERGEBRCH, on="ACCTNOC", how="inner")
MERGEDP = MERGEDP.filter(pl.col("ACCTNOC") != "")

# -----------------------------------
# Step 15: Merge statements, post, and deposit details
# -----------------------------------
MERGEDP1 = MERGEDP.join(DPSTMT, on="ACCTNOC", how="left")
MERGEDP2 = MERGEDP1.join(DPPOST, on="ACCTNOC", how="left")
MERGEDP3 = MERGEDP2.join(DEPOSIT1, on="ACCTNOC", how="left")
MERGEDP3 = MERGEDP3.sort("ACCTNOC")
print(MERGEDP3.head(10))

# -----------------------------------
# Step 16: Merge loans
# -----------------------------------
LOANACCT = ACCTFILE.with_columns([
    pl.col("ACCTNO").apply(zero_pad_acct).alias("ACCTNOC")
])
MERGELNBRCH = LOANACCT.join(PBBBRCH, on="ACCTBRCH", how="left")
MERGELN = MERGEALL.join(MERGELNBRCH, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"])
print(MERGELN.head(10))

# -----------------------------------
# Step 17: Merge SDB, UNICARD, COMCARD
# -----------------------------------
SAFEBOX = SAFEBOX.with_columns([
    pl.lit(0).alias("LEDGERBAL"),
    pl.lit("3").alias("CATEGORY"),
    pl.lit("SDB").alias("APPL_CODE")
])
MERGESDB = MERGEALL.join(SAFEBOX, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"])

UNICD = UNICARD.sort("ACCTNOC")
MERGEUNI = MERGEALL.join(UNICD, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"])

COMCD = COMCARD.sort("ACCTNOC")
MERGECOM = MERGEALL.join(COMCD, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"])

# -----------------------------------
# Step 18: Combine all outputs
# -----------------------------------
OUTPUT = pl.concat([MERGEDP3, MERGELN, MERGESDB, MERGEUNI, MERGECOM])

# -----------------------------------
# Step 19: Generate report
# -----------------------------------
# Handle nulls
for col in ["DEMODESC", "MASCODESC", "SICCODE", "MSICDESC"]:
    if col in OUTPUT.columns:
        OUTPUT = OUTPUT.with_columns(pl.col(col).fill_null("NIL"))

# Save output file
OUTPUT.write_csv("output.csv")
print("Output file generated: output.csv")
