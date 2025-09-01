import polars as pl
from datetime import datetime

# -------------------------------------------------------------------
# 1. Load parquet datasets (adjust paths to your converted files)
# -------------------------------------------------------------------
cisfile = pl.read_parquet("CUSTDAILY.parquet")
indfile = pl.read_parquet("DAILYINDV.parquet")
demofile = pl.read_parquet("DEMOCODE.parquet")
ctrldate = pl.read_parquet("datefile.parquet")

# -------------------------------------------------------------------
# 2. Get reporting date (from CTRLDATE dataset)
# -------------------------------------------------------------------
row = ctrldate.row(0, named=True)
SRSYY, SRSMM, SRSDD = row["SRSYY"], row["SRSMM"], row["SRSDD"]

report_date = datetime(SRSYY, SRSMM, SRSDD)
YEAR = f"{SRSYY:04d}"
MONTH = f"{SRSMM:02d}"
DAY = f"{SRSDD:02d}"
TIMEX = datetime.now().strftime("%H%M%S")

# -------------------------------------------------------------------
# 3. Process CIS dataset
# -------------------------------------------------------------------
cis = (
    cisfile
    .with_columns([
        pl.col("CUSTNO").alias("CUSTNOX"),
        pl.col("PRIPHONE").cast(pl.Utf8).str.zfill(11).alias("PRIPHONEX"),
        pl.col("SECPHONE").cast(pl.Utf8).str.zfill(11).alias("SECPHONEX"),
        pl.col("MOBILEPH").cast(pl.Utf8).str.zfill(11).alias("MOBILEPHX"),
        pl.col("FAX").cast(pl.Utf8).str.zfill(11).alias("FAXX"),
        pl.col("ADDREF").cast(pl.Utf8).str.zfill(11).alias("ADDREFX"),
    ])
)

# Derive OPEN DATE
cis = cis.with_columns([
    pl.col("CUSTOPENDATE").cast(pl.Utf8).alias("CUSTOPEN")
])
cis = cis.with_columns([
    pl.when(pl.col("CUSTOPEN") == "00002000000")
      .then("20000101")
      .otherwise(
          pl.col("CUSTOPEN").str.slice(4, 4) +  # year
          pl.col("CUSTOPEN").str.slice(0, 2) +  # month
          pl.col("CUSTOPEN").str.slice(2, 2)    # day
      )
      .alias("OPENDT")
])

# HRCALL concat (HRC01..HRC20)
hrc_cols = [f"HRC{i:02d}" for i in range(1, 21)]
cis = cis.with_columns([
    pl.concat_str([pl.col(c).cast(pl.Utf8).str.zfill(3) for c in hrc_cols]).alias("HRCALL")
])

# Drop duplicates by CUSTNOX
cis = cis.unique(subset=["CUSTNOX"])

# -------------------------------------------------------------------
# 4. Process DEMOFILE (split by category)
# -------------------------------------------------------------------
sales = (
    demofile.filter(pl.col("DEMOCATEGORY") == "SALES")
    .select([
        pl.col("DEMOCODE").alias("SALES"),
        pl.col("CODEDESC").alias("SALDESC")
    ])
    .unique(subset=["SALES"])
)

restr = (
    demofile.filter(pl.col("DEMOCATEGORY") == "RESTR")
    .select([
        pl.col("DEMOCODE").alias("RESTR"),
        pl.col("CODEDESC").alias("RESDESC")
    ])
    .unique(subset=["RESTR"])
)

citzn = (
    demofile.filter(pl.col("DEMOCATEGORY") == "CITZN")
    .select([
        pl.col("DEMOCODX").alias("CITZN"),
        pl.col("CODEDESC").alias("CTZDESC")
    ])
    .unique(subset=["CITZN"])
)

# -------------------------------------------------------------------
# 5. Process INDFILE (INDVDLY)
# -------------------------------------------------------------------
indv = (
    indfile
    .filter(pl.col("CUSTNO").is_not_null())
    .with_columns([
        pl.col("CUSTNO").alias("CUSTNOX")
    ])
)

indv = indv.unique(subset=["CUSTNOX"], keep="last")  # keep last update

# -------------------------------------------------------------------
# 6. Merge datasets step by step
# -------------------------------------------------------------------
mrgcis = (
    cis.join(indv, on="CUSTNOX", how="left")
    .with_columns([
        pl.lit(YEAR + MONTH + DAY + TIMEX).alias("RUNTIMESTAMP"),
        pl.col("RESIDENCY").alias("RESTR"),
        pl.col("CORPSTATUS").alias("SALES"),
        pl.col("CITIZENSHIP").alias("CITZN"),
    ])
)

mrgres = mrgcis.join(restr, on="RESTR", how="left")
mrgsal = mrgres.join(sales, on="SALES", how="left")
mrgctz = mrgsal.join(citzn, on="CITZN", how="left")

# -------------------------------------------------------------------
# 7. Final Output (OUT2)
# -------------------------------------------------------------------
out2 = mrgctz.select([
    "RUNTIMESTAMP","CUSTNOX","ADDREFX","CUSTNAME","PRIPHONEX","SECPHONEX",
    "MOBILEPHX","FAXX","ALIASKEY","ALIAS","PROCESSTIME","CUSTSTAT","TAXCODE",
    "TAXID","CUSTBRCH","COSTCTR","CUSTMNTDATE","CUSTLASTOPER","PRIM_OFF",
    "SEC_OFF","PRIM_LN_OFF","SEC_LN_OFF","RACE","RESIDENCY","CITIZENSHIP",
    "OPENDT","HRCALL","EXPERIENCE","HOBBIES","RELIGION","LANGUAGE","INST_SEC",
    "CUST_CODE","CUSTCONSENT","BASICGRPCODE","MSICCODE","MASCO2008","INCOME",
    "EDUCATION","OCCUP","MARITALSTAT","OWNRENT","EMPNAME","DOBDOR","SICCODE",
    "CORPSTATUS","NETWORTH","LAST_UPDATE_DATE","LAST_UPDATE_TIME",
    "LAST_UPDATE_OPER","PRCOUNTRY","EMPLOYMENT_TYPE","EMPLOYMENT_SECTOR",
    "EMPLOYMENT_LAST_UPDATE","BNMID","LONGNAME","INDORG","RESDESC",
    "SALDESC","CTZDESC"
])

# Save final output
out2.write_parquet("COMBINECUSTALL.parquet")
out2.write_csv("COMBINECUSTALL.csv")

