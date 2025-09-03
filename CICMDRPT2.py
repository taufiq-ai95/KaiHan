# ================================================================
# Shared imports & base dir
# ================================================================
import polars as pl

BASE = "parquet"  # <- adjust to your actual location

# ================================================================
# Read parquet datasets (no description, just loading)
# ================================================================
ctrldate  = pl.read_parquet(f"{BASE}/CTRLDATE.parquet")
aliasfl   = pl.read_parquet(f"{BASE}/ALIAS.parquet")
brch      = pl.read_parquet(f"{BASE}/BRANCH.parquet")
masco     = pl.read_parquet(f"{BASE}/MASCOFL.parquet")
msic      = pl.read_parquet(f"{BASE}/MSICFL.parquet")
dpstmt    = pl.read_parquet(f"{BASE}/CYCLEFL.parquet")
dppost    = pl.read_parquet(f"{BASE}/POSTFL.parquet")
deposit1  = pl.read_parquet(f"{BASE}/DEPOFL.parquet")
cis       = pl.read_parquet(f"{BASE}/CISFILE_CUSTDLY.parquet")
occupat   = pl.read_parquet(f"{BASE}/OCCUPAT.parquet")  # assuming occupation lookup table
dptrbals_raw = pl.read_parquet(f"{BASE}/DPTRBALS.parquet")
dpstmt = pl.read_parquet(f"{BASE}/CYCLEFL.parquet")
dppost = pl.read_parquet(f"{BASE}/POSTFL.parquet")
deposit1 = pl.read_parquet(f"{BASE}/DEPOFL.parquet")
loanacct_raw = pl.read_parquet(f"{BASE}/ACCTFILE.parquet")
safebox_raw = pl.read_parquet(f"{BASE}/SAFEBOX.parquet")
unicd_raw   = pl.read_parquet(f"{BASE}/UNICARD.parquet")
comcd_raw = pl.read_parquet(f"{BASE}/COMCARD.parquet")

# ================================================================
# Part 1: CTRLDATE → derive LOADDATA
# ================================================================
LOADDATE = (
    ctrldate
    .select(
        pl.col("TODAYYY").cast(pl.Int32),
        pl.col("TODAYMM").cast(pl.Int32),
        pl.col("TODAYDD").cast(pl.Int32),
    )
    .with_columns(
        pl.format(
            "{:04d}-{:02d}-{:02d}",
            pl.col("TODAYYY"),
            pl.col("TODAYMM"),
            pl.col("TODAYDD")
        ).alias("LOADDATE")
    )
    .item(0, "LOADDATE")
)
print("\n=== Part 1: CTRLDATE (LOADDATE) ===")
print("LOADDATE:", LOADDATA)

# ================================================================
# Part 2: ALIASFL
# ================================================================
aliasfl = (
    aliasfl
    .select(
        pl.col("ALIASKEY").cast(pl.Utf8),
        pl.col("ALIAS").cast(pl.Utf8),
    )
    .sort(["ALIASKEY", "ALIAS"])
)
print("\n=== Part 2: ALIASFL ===")
print(aliasfl.head(5))

# ================================================================
# Part 3: BRANCH
# ================================================================
brch = (
    brch
    .select(
        pl.col("ACCTBRCH").cast(pl.Utf8),
        pl.col("BRANCH_ABBR").cast(pl.Utf8),
    )
    .unique(subset=["ACCTBRCH"], keep="first")
    .sort("ACCTBRCH")
)
print("\n=== Part 3: BRANCH ===")
print(brch.head(5))

# ================================================================
# Part 4: MASCO
# ================================================================
masco = (
    masco
    .select(
        pl.col("MASCO2008").cast(pl.Utf8),
        pl.col("MASCODESC").cast(pl.Utf8),
    )
    .sort("MASCO2008")
)
print("\n=== Part 4: MASCO ===")
print(masco.head(5))

# ================================================================
# Part 5: MSIC
# ================================================================
msic = (
    msic
    .select(
        pl.col("MSICCODE").cast(pl.Utf8),
        pl.col("MSICDESC").cast(pl.Utf8),
    )
    .sort("MSICCODE")
)
print("\n=== Part 5: MSIC ===")
print(msic.head(5))

# ================================================================
# Part 6: DPSTMT (CYCLEFL)
# ================================================================
dpstmt = (
    dpstmt
    .with_columns(
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).alias("ACCTNOC")
    )
    .select(
        "ACCTNOC",
        pl.col("ACCTNAME").cast(pl.Utf8),
        pl.col("CURR_CYC_DR").cast(pl.Int64, strict=False),
        pl.col("CURR_AMT_DR").cast(pl.Float64, strict=False),
        pl.col("CURR_CYC_CR").cast(pl.Int64, strict=False),
        pl.col("CURR_AMT_CR").cast(pl.Float64, strict=False),
        pl.col("PREV_CYC_DR").cast(pl.Int64, strict=False),
        pl.col("PREV_AMT_DR").cast(pl.Float64, strict=False),
        pl.col("PREV_CYC_CR").cast(pl.Int64, strict=False),
        pl.col("PREV_AMT_CR").cast(pl.Float64, strict=False),
    )
    .sort("ACCTNOC")
)
print("\n=== Part 6: DPSTMT ===")
print(dpstmt.head(5))

# ================================================================
# Part 7: DPPOST (POSTFL)
# ================================================================
dppost = (
    dppost
    .select(
        pl.col("ACCTNOC").cast(pl.Utf8),
        pl.col("ACCT_PST_IND").cast(pl.Utf8),
        pl.col("ACCT_PST_REASON").cast(pl.Utf8),
    )
    .sort("ACCTNOC")
)
print("\n=== Part 7: DPPOST ===")
print(dppost.head(5))

# ================================================================
# Part 8: DEPOSIT1 (DEPOFL)
# ================================================================
deposit1 = (
    deposit1
    .with_columns(
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).alias("ACCTNOC")
    )
    .select(
        "ACCTNOC",
        pl.col("SEQID_1").cast(pl.Utf8),
        pl.col("SEQID_2").cast(pl.Utf8),
        pl.col("SEQID_3").cast(pl.Utf8),
        pl.col("AMT_1").cast(pl.Float64, strict=False),
        pl.col("AMT_2").cast(pl.Float64, strict=False),
        pl.col("AMT_3").cast(pl.Float64, strict=False),
        pl.col("DESC_1").cast(pl.Utf8),
        pl.col("DESC_2").cast(pl.Utf8),
        pl.col("DESC_3").cast(pl.Utf8),
        pl.col("SOURCE_1").cast(pl.Utf8),
        pl.col("SOURCE_2").cast(pl.Utf8),
        pl.col("SOURCE_3").cast(pl.Utf8),
        pl.col("TOT_HOLD").cast(pl.Utf8),
    )
    .sort("ACCTNOC")
)
print("\n=== Part 8: DEPOSIT1 ===")
print(deposit1.head(5))

# ================================================================
# Part 9: CIS + merges → mergeall1 (including OCCUP merge)
# ================================================================
cis = (
    cis
    .with_columns(
        pl.col("OCCUP").cast(pl.Utf8).alias("DEMOCODE")
    )
    .select([
        pl.all(),
        pl.col("DEMOCODE")
    ])
    .sort(["ALIASKEY","ALIAS"])
)

# Step 1: Merge aliasfl with CIS on ALIASKEY + ALIAS
mergeals = aliasfl.join(cis, on=["ALIASKEY","ALIAS"], how="inner")

# Step 2: Merge OCCUP code with occupation lookup table
mergeocc = mergeals.join(occupat, on="DEMOCODE", how="left")

# Step 3: Merge MASCO
mergemsc = mergeocc.join(masco, on="MASCO2008", how="left")

# Step 4: Merge MSIC
mergeall1 = mergemsc.join(msic, on="MSICCODE", how="left").sort("ALIAS")

# Preview
print("\n=== Part 9: MERGEALL1 ===")
print(mergeall1.head(5))

# ================================================================
# Part 10: SAFEBOX2 + merge with MERGEALL1 (SDB flags)
# ================================================================
dptrbals = (
    dptrbals_raw
    .filter(
        (pl.col("REPTNO") == 1001) & (pl.col("FMTCODE").cast(pl.Int64).is_in([1,10,22,19,20,21]))
    )
    .with_columns(
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).alias("ACCTNOC"),
        pl.col("ACCTBRCH1").cast(pl.Int64).alias("ACCTBRCH1_i"),
        pl.col("ACCTBRCH1").cast(pl.Utf8).str.zfill(3).alias("ACCTBRCH"),
        pl.col("PRODTYPE").cast(pl.Int64),
        pl.col("PRODTYPE").cast(pl.Utf8).str.zfill(3).alias("PRODTY"),
        (pl.when((pl.col("COSTCTR") > 3000) & (pl.col("COSTCTR") < 3999))
           .then(pl.lit("I")).otherwise(pl.lit("C"))).alias("BANKINDC"),
        (pl.col("LEDGERBAL1").cast(pl.Float64, strict=False) / 100.0).alias("LEDGERBAL"),
        pl.col("ACCTNAME").cast(pl.Utf8).alias("ACCTNAME40"),
    )
    .with_columns([
        pl.when((pl.col("ACCTNOC") > "03000000000") & (pl.col("ACCTNOC") < "03999999999"))
          .then(pl.lit("CA"))
          .when((pl.col("ACCTNOC") > "06200000000") & (pl.col("ACCTNOC") < "06299999999"))
          .then(pl.lit("CA"))
          .when((pl.col("ACCTNOC") > "06710000000") & (pl.col("ACCTNOC") < "06719999999"))
          .then(pl.lit("CA"))
          .when((pl.col("ACCTNOC") > "01000000000") & (pl.col("ACCTNOC") < "01999999999"))
          .then(pl.lit("FD"))
          .when((pl.col("ACCTNOC") > "07000000000") & (pl.col("ACCTNOC") < "07999999999"))
          .then(pl.lit("FD"))
          .when((pl.col("ACCTNOC") > "04000000000") & (pl.col("ACCTNOC") < "04999999999"))
          .then(pl.lit("SA"))
          .when((pl.col("ACCTNOC") > "05000000000") & (pl.col("ACCTNOC") < "05999999999"))
          .then(pl.lit("SA"))
          .when((pl.col("ACCTNOC") > "06000000000") & (pl.col("ACCTNOC") < "06199999999"))
          .then(pl.lit("SA"))
          .when((pl.col("ACCTNOC") > "06300000000") & (pl.col("ACCTNOC") < "06709999999"))
          .then(pl.lit("SA"))
          .when((pl.col("ACCTNOC") > "06720000000") & (pl.col("ACCTNOC") < "06999999999"))
          .then(pl.lit("SA"))
          .otherwise(pl.col("APPL_CODE").cast(pl.Utf8, strict=False))
          .alias("APPL_CODE")
    ])
    .with_columns(
        pl.when(pl.col("PRODTY").is_in(
            ["371","350","351","352","353","354","355","356","357","358","359","360","361","362"]
        )).then(pl.lit("FCYFD"))
         .when(pl.col("PRODTY").is_in(
            ["400","401","402","403","404","405","406","407","408","409","410","411","413","414",
             "420","421","422","423","424","425","426","427","428","429","430","431","432","433",
             "434","440","441","442","443","444","450","451","452","453","454","460","461","473",
             "474","475","476"]
        )).then(pl.lit("FCYCA"))
         .otherwise(pl.col("APPL_CODE"))
         .alias("APPL_CODE")
    )
    .filter(pl.col("PURPOSECD").is_not_null() & (pl.col("PURPOSECD") != ""))
    .with_columns(pl.col("PURPOSECD").alias("ACCT_TYPE"))
    .with_columns(
        pl.col("OPENDATE").cast(pl.Utf8),
        pl.col("CLSEDATE").cast(pl.Utf8),
    )
    .with_columns(
        pl.col("OPENDATE").str.slice(6,2).alias("OPENDD"),
        pl.col("OPENDATE").str.slice(4,2).alias("OPENMM"),
        pl.col("OPENDATE").str.slice(0,4).alias("OPENYY"),
        pl.col("CLSEDATE").str.slice(6,2).alias("CLSEDD"),
        pl.col("CLSEDATE").str.slice(4,2).alias("CLSEMM"),
        pl.col("CLSEDATE").str.slice(0,4).alias("CLSEYY"),
    )
    .with_columns(
        (pl.col("OPENYY")+pl.col("OPENMM")+pl.col("OPENDD")).alias("DATEOPEN"),
        (pl.col("CLSEYY")+pl.col("CLSEMM")+pl.col("CLSEDD")).alias("DATECLSE"),
    )
    .with_columns(
        pl.when(pl.col("OPENIND").is_null() | (pl.col("OPENIND") == ""))
          .then(pl.lit("ACTIVE"))
          .when(pl.col("OPENIND").is_in(["B","C","P"]))
          .then(pl.lit("CLOSED"))
          .when(pl.col("OPENIND") == "Z")
          .then(pl.lit("ZERO BALANCE"))
          .otherwise(pl.lit(""))
          .alias("ACCTSTATUS")
    )
    .select([
        "ACCTNOC","ACCTBRCH","BRANCH_ABBR","ACCTNAME40","CLSEDATE","OPENDATE",
        "PURPOSECD","LEDGERBAL","PRODTY","BALHOLD","CURBAL","ODLIMIT","CURRCODE",
        "OPENIND","COSTCTR","POSTIND","APPL_CODE","ACCT_TYPE","ACCTSTATUS",
        "DATEOPEN","DATECLSE"
    ])
    .sort("ACCTBRCH")
)

print("\n=== Part 10: DPTRBALS preview ===")
print(dptrbals.head(5))

# ================================================================
# Part 11: DPTRBALS processing + merges
# ================================================================
dpstmt = (
    dpstmt
    .with_columns(
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).alias("ACCTNOC")
    )
    .select(
        "ACCTNOC",
        pl.col("ACCTNAME").cast(pl.Utf8),
        pl.col("CURR_CYC_DR").cast(pl.Int64, strict=False),
        pl.col("CURR_AMT_DR").cast(pl.Float64, strict=False),
        pl.col("CURR_CYC_CR").cast(pl.Int64, strict=False),
        pl.col("CURR_AMT_CR").cast(pl.Float64, strict=False),
        pl.col("PREV_CYC_DR").cast(pl.Int64, strict=False),
        pl.col("PREV_AMT_DR").cast(pl.Float64, strict=False),
        pl.col("PREV_CYC_CR").cast(pl.Int64, strict=False),
        pl.col("PREV_AMT_CR").cast(pl.Float64, strict=False),
    )
    .sort("ACCTNOC")
)

dppost = (
    dppost
    .select(
        pl.col("ACCTNOC").cast(pl.Utf8),
        pl.col("ACCT_PST_IND").cast(pl.Utf8),
        pl.col("ACCT_PST_REASON").cast(pl.Utf8),
    )
    .sort("ACCTNOC")
)

deposit1 = (
    deposit1
    .with_columns(
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).alias("ACCTNOC")
    )
    .select(
        "ACCTNOC",
        pl.col("SEQID_1").cast(pl.Utf8),
        pl.col("SEQID_2").cast(pl.Utf8),
        pl.col("SEQID_3").cast(pl.Utf8),
        pl.col("AMT_1").cast(pl.Float64, strict=False),
        pl.col("AMT_2").cast(pl.Float64, strict=False),
        pl.col("AMT_3").cast(pl.Float64, strict=False),
        pl.col("DESC_1").cast(pl.Utf8),
        pl.col("DESC_2").cast(pl.Utf8),
        pl.col("DESC_3").cast(pl.Utf8),
        pl.col("SOURCE_1").cast(pl.Utf8),
        pl.col("SOURCE_2").cast(pl.Utf8),
        pl.col("SOURCE_3").cast(pl.Utf8),
        pl.col("TOT_HOLD").cast(pl.Utf8),
    )
    .sort("ACCTNOC")
)

print("\n=== Part 11: DPSTMT preview ===")
print(dpstmt.head(5))

print("\n=== Part 11: DPPOST preview ===")
print(dppost.head(5))

print("\n=== Part 11: DEPOSIT1 preview ===")
print(deposit1.head(5))

# ================================================================
# Part 12: Loan accounts processing + merge with MERGEALL
# ================================================================
loanacct = (
    loanacct_raw
    .with_columns(
        pl.col("ACCTNO").cast(pl.Int64),
        pl.col("ACCTNO").cast(pl.Utf8).str.zfill(11).alias("ACCTNOC"),
        pl.col("NOTENO").cast(pl.Int64),
        pl.col("NOTENO").cast(pl.Utf8).str.zfill(5).alias("NOTENOC"),
    )
    .with_columns(
        (pl.col("ACCTNOC") + pl.lit("-") + pl.col("NOTENOC")).alias("ACCTNOTE"),
        pl.col("ACCTNAME").cast(pl.Utf8).alias("ACCTNAME40"),
        pl.col("ORGTYPE").cast(pl.Utf8).alias("ACCT_TYPE"),
        pl.when((pl.col("ACCTNOC") > "02000000000") & (pl.col("ACCTNOC") < "02999999999"))
          .then(pl.lit("LN"))
         .when((pl.col("ACCTNOC") > "08000000000") & (pl.col("ACCTNOC") < "08999999999"))
          .then(pl.lit("HP"))
         .otherwise(pl.lit(None))
         .alias("APPL_CODE"),
        pl.when((pl.col("COSTCENTER") >= 3000) & (pl.col("COSTCENTER") <= 3999))
          .then(pl.lit("I")).otherwise(pl.lit("C")).alias("BANKINDC"),
        pl.col("COSTCENTER").cast(pl.Int64).fill_null(0).alias("COSTCENTER_i")
    )
    .with_columns(
        pl.col("COSTCENTER_i").cast(pl.Utf8).str.zfill(7).str.slice(-3).alias("COSTCTR1"),
        pl.col("COSTCTR1").cast(pl.Utf8).alias("ACCTBRCH"),
        pl.col("ACCTOPENDATE").cast(pl.Utf8).alias("ACCTOPNDT"),
        (pl.col("ACCTOPENDATE").cast(pl.Utf8).str.slice(0,8)).alias("ACCTOPNDT8"),
        pl.col("LASTTRANDATE").cast(pl.Utf8).alias("LASTTRNDT"),
        (pl.col("LASTTRANDATE").cast(pl.Utf8).str.slice(0,8)).alias("LASTTRNDT8"),
    )
    .with_columns(
        pl.col("ACCTOPNDT8").str.slice(4,4).alias("OPENYY"),
        pl.col("ACCTOPNDT8").str.slice(0,2).alias("OPENMM"),
        pl.col("ACCTOPNDT8").str.slice(2,2).alias("OPENDD"),
        (pl.col("OPENYY")+pl.col("OPENMM")+pl.col("OPENDD")).alias("DATEOPEN"),
        pl.col("LASTTRNDT8").str.slice(4,4).alias("LTRNYY"),
        pl.col("LASTTRNDT8").str.slice(0,2).alias("LTRNMM"),
        pl.col("LASTTRNDT8").str.slice(2,2).alias("LTRNDD"),
        (pl.col("LTRNYY")+pl.col("LTRNMM")+pl.col("LTRNDD")).alias("DATECLSE"),
        (pl.col("NOTECURBAL").cast(pl.Float64, strict=False) / 100.0).alias("LEDGERBAL"),
    )
    .with_columns(
        pl.when((pl.col("NPLINDC") == "3") | (pl.col("ARREARDAY") > 92))
          .then(pl.lit("NPL"))
         .when((pl.col("ARREARDAY") > 1) & (pl.col("ARREARDAY") < 92))
          .then(pl.lit("ACCOUNT IN ARREARS"))
         .when(pl.col("NOTEPAID") == "P")
          .then(pl.lit("PAID-OFF"))
         .otherwise(pl.lit(""))
         .alias("ACCTSTATUS")
    )
    .with_columns(
        pl.when((pl.col("NOTECURBAL") > 0) & ((pl.col("ACCTSTATUS") == "") | pl.col("ACCTSTATUS").is_null()))
          .then(pl.lit("ACTIVE"))
         .otherwise(pl.col("ACCTSTATUS"))
         .alias("ACCTSTATUS")
    )
    .select([
        "ACCTNOC","ACCTNAME40","ACCT_TYPE","APPL_CODE","BANKINDC",
        "COSTCTR1","ACCTBRCH","DATEOPEN","DATECLSE","LEDGERBAL","ACCTSTATUS"
    ])
    .sort("ACCTBRCH")
)

# Join BRCH → MERGELNBRCH
mergelnbrch = loanacct.join(brch, on="ACCTBRCH", how="left").sort("ACCTNOC")

# MERGEALL + MERGELNBRCH on ACCTNOC (inner like SAS IF C AND D) → MERGELN
mergeln = mergeall.join(mergelnbrch, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"], keep="first")

print("\n=== Part 12: Loan Accounts preview (after merge) ===")
print(mergeln.head(5))

# ================================================================
# Part 13: SAFEBOX processing + merge with MERGEALL
# ================================================================
safebox = (
    safebox_raw
    .select(
        pl.col("CUSTNO").cast(pl.Utf8),
        pl.col("ACCTNAME40").cast(pl.Utf8),
        pl.col("BRANCH_ABBR").cast(pl.Utf8),
        pl.col("ACCTNOC").cast(pl.Utf8),
        pl.col("BANKINDC").cast(pl.Utf8),
        pl.col("ACCTSTATUS").cast(pl.Utf8),
    )
    .with_columns(
        pl.lit(0.0).alias("LEDGERBAL"),
        pl.lit("3").alias("CATEGORY"),
        pl.lit("SDB").alias("APPL_CODE"),
    )
    .sort("ACCTNOC")
)

# Inner join SAFEBOX with mergeall, keep unique ACCTNOC
mergesdb = mergeall.join(safebox, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"], keep="first")

print("\n=== Part 13: SAFEBOX Merge preview ===")
print(mergesdb.head(5))


# ================================================================
# Part 14: UNICARD processing + merge with MERGEALL
# ================================================================
unicd = (
    unicd_raw
    .select(
        pl.col("BRANCH_ABBR").cast(pl.Utf8),
        pl.col("ACCTNOC").cast(pl.Utf8),
        pl.col("ACCTSTATUS").cast(pl.Utf8),
        pl.col("DATEOPEN").cast(pl.Utf8),
        pl.col("DATECLSE").cast(pl.Utf8),
    )
    .sort("ACCTNOC")
)

# Inner join UNICARD with mergeall, keep unique ACCTNOC
mergeuni = mergeall.join(unicd, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"], keep="first")

print("\n=== Part 14: UNICARD Merge preview ===")
print(mergeuni.head(5))

# ================================================================
# Part 15: COMCARD processing + merge with MERGEALL
# ================================================================
comcd = (
    comcd_raw
    .select(
        pl.col("BRANCH_ABBR").cast(pl.Utf8),
        pl.col("ACCTNOC").cast(pl.Utf8),
        pl.col("ACCTSTATUS").cast(pl.Utf8),
        pl.col("DATEOPEN").cast(pl.Utf8),
        pl.col("DATECLSE").cast(pl.Utf8),
    )
    .sort("ACCTNOC")
)

# Inner join COMCARD with mergeall, keep unique ACCTNOC
mergecom = mergeall.join(comcd, on="ACCTNOC", how="inner").unique(subset=["ACCTNOC"], keep="first")

print("\n=== Part 15: COMCARD Merge preview ===")
print(mergecom.head(5))

# ================================================================
# Part 16: Combine all merged dataframes into final output
# ================================================================

# Union rows (like SAS SET MERGEDP MERGELN MERGESDB MERGEUNI MERGECOM)
# diagonal_relaxed allows different schemas
output_df = pl.concat(
    [df for df in [mergedp, mergeln, mergesdb, mergeuni, mergecom] if 'ACCTNOC' in df.columns],
    how="diagonal_relaxed"
)

# Select/rename to final layout (as per positional PUT in SAS)
output_df = output_df.select([
    pl.col("CUSTNO").cast(pl.Utf8),
    pl.col("ACCTNOC").cast(pl.Utf8),
    pl.col("OCCUP").cast(pl.Utf8, strict=False),
    pl.col("MASCO2008").cast(pl.Utf8, strict=False),
    pl.col("ALIASKEY").cast(pl.Utf8, strict=False),
    pl.col("ALIAS").cast(pl.Utf8, strict=False),
    pl.col("CUSTNAME").cast(pl.Utf8, strict=False),
    pl.col("DATEOPEN").cast(pl.Utf8, strict=False),
    pl.col("DATECLSE").cast(pl.Utf8, strict=False),
    pl.col("LEDGERBAL").cast(pl.Float64, strict=False),
    pl.col("BANKINDC").cast(pl.Utf8, strict=False),
    pl.col("CITIZENSHIP").cast(pl.Utf8, strict=False),
    pl.col("APPL_CODE").cast(pl.Utf8, strict=False),
    pl.col("PRODTY").cast(pl.Utf8, strict=False),
    pl.col("DEMODESC").cast(pl.Utf8, strict=False),
    pl.col("MASCODESC").cast(pl.Utf8, strict=False),
    pl.col("JOINTACC").cast(pl.Utf8, strict=False),
    pl.col("MSICCODE").cast(pl.Utf8, strict=False),
    pl.col("ACCTBRCH").cast(pl.Utf8, strict=False),
    pl.col("BRANCH_ABBR").cast(pl.Utf8, strict=False),
    pl.col("ACCTSTATUS").cast(pl.Utf8, strict=False),
    pl.col("SICCODE").cast(pl.Utf8, strict=False),
])

print("\n=== Part 16: Combined output preview ===")
print(output_df.head(5))

# ================================================================
# Part 17.1: Generate semicolon-delimited customer report
# ================================================================
report_base = pl.concat(
    [df for df in [mergedp3, mergeln, mergesdb, mergeuni, mergecom] if 'ACCTNOC' in df.columns],
    how="diagonal_relaxed"
)

report_df = (
    report_base
    .with_columns([
        # default NILs
        pl.col("DEMODESC").fill_null("").replace("", "NIL").alias("DEMODESC"),
        pl.col("MASCODESC").fill_null("").replace("", "NIL").alias("MASCODESC"),
        pl.col("SICCODE").fill_null("").replace("", "NIL").alias("SICCODE"),
        pl.col("MSICDESC").fill_null("").replace("", "NIL").alias("MSICDESC"),
        # TEMP_* per SAS logic
        pl.when(pl.col("CURBAL").is_not_null() & (pl.col("CURBAL") != 0))
          .then(pl.col("CURBAL")).otherwise(pl.lit(0.0)).alias("TEMP_CURBAL"),
        pl.when(pl.col("CURR_AMT_DR") > 0).then(pl.col("CURR_AMT_DR")).otherwise(0.0).alias("TEMP_CURR_AMT_DR"),
        pl.when(pl.col("CURR_AMT_CR") > 0).then(pl.col("CURR_AMT_CR")).otherwise(0.0).alias("TEMP_CURR_AMT_CR"),
        pl.when(pl.col("PREV_AMT_DR") > 0).then(pl.col("PREV_AMT_DR")).otherwise(0.0).alias("TEMP_PREV_AMT_DR"),
        pl.when(pl.col("PREV_AMT_CR") > 0).then(pl.col("PREV_AMT_CR")).otherwise(0.0).alias("TEMP_PREV_AMT_CR"),
        pl.when(pl.col("CURR_CYC_DR") > 0).then(pl.col("CURR_CYC_DR")).otherwise(0).alias("TEMP_CURR_CYC_DR"),
        pl.when(pl.col("CURR_CYC_CR") > 0).then(pl.col("CURR_CYC_CR")).otherwise(0).alias("TEMP_CURR_CYC_CR"),
        pl.when(pl.col("PREV_CYC_DR") > 0).then(pl.col("PREV_CYC_DR")).otherwise(0).alias("TEMP_PREV_CYC_DR"),
        pl.when(pl.col("PREV_CYC_CR") > 0).then(pl.col("PREV_CYC_CR")).otherwise(0).alias("TEMP_PREV_CYC_CR"),
        # holds
        pl.when(pl.col("AMT_1") > 0).then(pl.col("AMT_1")).otherwise(0.0).alias("TEMP_AMT_1"),
        pl.when(pl.col("AMT_2") > 0).then(pl.col("AMT_2")).otherwise(0.0).alias("TEMP_AMT_2"),
        pl.when(pl.col("AMT_3") > 0).then(pl.col("AMT_3")).otherwise(0.0).alias("TEMP_AMT_3"),
    ])
    # zero out TEMP_* if APPL_CODE not in ('FD','CA','SA')
    .with_columns([
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURBAL")).otherwise(0.0).alias("TEMP_CURBAL"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_AMT_DR")).otherwise(0.0).alias("TEMP_CURR_AMT_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_AMT_CR")).otherwise(0.0).alias("TEMP_CURR_AMT_CR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_AMT_DR")).otherwise(0.0).alias("TEMP_PREV_AMT_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_AMT_CR")).otherwise(0.0).alias("TEMP_PREV_AMT_CR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_CYC_DR")).otherwise(0).alias("TEMP_CURR_CYC_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_CURR_CYC_CR")).otherwise(0).alias("TEMP_CURR_CYC_CR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_CYC_DR")).otherwise(0).alias("TEMP_PREV_CYC_DR"),
        pl.when(pl.col("APPL_CODE").is_in(["FD","CA","SA"]))
          .then(pl.col("TEMP_PREV_CYC_CR")).otherwise(0).alias("TEMP_PREV_CYC_CR"),
    ])
    .with_row_count(name="ROW_NO", offset=1)  # mimic _N_
    .select([
        pl.col("ROW_NO"),
        pl.col("ALIASKEY"), pl.col("ALIAS"), pl.col("CUSTNAME"),
        pl.col("CUSTNO"), pl.col("DEMODESC"), pl.col("MASCODESC"),
        pl.col("SICCODE"), pl.col("MSICDESC"), pl.col("ACCTNOC"),
        pl.col("BRANCH_ABBR"), pl.col("ACCTSTATUS"),
        pl.col("DATEOPEN"), pl.col("DATECLSE"),
        pl.col("SDBIND"), pl.col("SDBBRH"),
        pl.col("TEMP_CURBAL"), pl.col("TEMP_CURR_CYC_DR"), pl.col("TEMP_CURR_AMT_DR"),
        pl.col("TEMP_CURR_CYC_CR"), pl.col("TEMP_CURR_AMT_CR"),
        pl.col("TEMP_PREV_CYC_DR"), pl.col("TEMP_PREV_AMT_DR"),
        pl.col("TEMP_PREV_CYC_CR"), pl.col("TEMP_PREV_AMT_CR"),
        pl.col("ACCT_PST_IND"), pl.col("ACCT_PST_REASON"),
        pl.col("TOT_HOLD"), pl.col("SEQID_1"), pl.col("TEMP_AMT_1"),
        pl.col("DESC_1"), pl.col("SOURCE_1"),
        pl.col("SEQID_2"), pl.col("TEMP_AMT_2"),
        pl.col("DESC_2"), pl.col("SOURCE_2"),
        pl.col("SEQID_3"), pl.col("TEMP_AMT_3"),
        pl.col("DESC_3"), pl.col("SOURCE_3"),
    ])
)

# Write semicolon-delimited report file
report_header_lines = [
    "LIST OF CUSTOMERS INFORMATION",
    ";".join([
        "NO","ID TYPE","ID NUMBER","CUST NAME","CIS NUMBER","OCCUPATION",
        "MASCO","SIC CODE","MSIC BIS TYPE","ACCT NUMBER","ACCT BRANCH",
        "ACCT STATUS","DATE ACCT OPEN","DATE ACCT CLOSED","SDB(YES/NO)",
        "BR SDB MAINTAN","CURRENT BALANCE","CURR CYC DR","CURR AMT DR",
        "CURR CYC CR","CURR AMT CR","PREV CYC DR","PREV AMT DR",
        "PREV CYC CR","PREV AMT CR","POST INDICATOR","POST INDICATOR REASON",
        "TOTAL OF HOLD","SEQ OF HOLD(1)","AMT OF HOLD(1)","DESCRIP OF HOLD(1)","SOURCE(1)",
        "SEQ OF HOLD(2)","AMT OF HOLD(2)","DESCRIP OF HOLD(2)","SOURCE(2)",
        "SEQ OF HOLD(3)","AMT OF HOLD(3)","DESCRIP OF HOLD(3)","SOURCE(3)"
    ])
]

report_body = report_df.with_columns([pl.all().cast(pl.Utf8, strict=False).fill_null("")])

with open("CMDREPORT.txt", "w", encoding="utf-8") as f:
    for line in report_header_lines:
        f.write(line + "\n")
    for row in report_body.iter_rows():
        f.write(";".join("" if v is None else str(v) for v in row) + "\n")

# ================================================================
# Part 17.2: Generate semicolon-delimited customer report
# ================================================================
# Combine datasets vertically (like SET in SAS)
df = pl.concat([mergedp3, mergeln, mergesdb, mergeuni, mergecom])

# -----------------------------
#  Replace blanks with 'NIL'
# -----------------------------
for col in ['DEMODESC', 'MASCODESC', 'SICCODE', 'MSICDESC']:
    if col in df.columns:
        df = df.with_columns(
            pl.when(pl.col(col).is_null() | (pl.col(col) == '') | (pl.col(col) == ' '))
            .then('NIL')
            .otherwise(pl.col(col))
            .alias(col)
        )

# -----------------------------
#  Function to keep positive values
# -----------------------------
def pos_val(col_name):
    return pl.when(pl.col(col_name) > 0).then(pl.col(col_name)).otherwise(0.0)

# -----------------------------
#  TEMP_CURBAL
# -----------------------------
df = df.with_columns(
    pl.when((pl.col('CURBAL') != 0) & pl.col('CURBAL').is_not_null())
    .then(pl.col('CURBAL'))
    .otherwise(0.0)
    .alias('TEMP_CURBAL')
)

# Zero out for non-FD/CA/SA accounts
mask = ~df['APPL_CODE'].is_in(['FD', 'CA', 'SA'])
df = df.with_columns(
    pl.when(mask).then(0.0).otherwise(pl.col('TEMP_CURBAL')).alias('TEMP_CURBAL')
)

# -----------------------------
#  Amount and cycle fields
# -----------------------------
amt_fields = ['CURR_AMT_DR', 'CURR_AMT_CR', 'PREV_AMT_DR', 'PREV_AMT_CR']
cycle_fields = ['CURR_CYC_DR', 'CURR_CYC_CR', 'PREV_CYC_DR', 'PREV_CYC_CR']

for f in amt_fields + cycle_fields:
    df = df.with_columns(pos_val(f).alias(f'TEMP_{f}'))

# Zero out amounts and cycles for non-FD/CA/SA accounts
for f in amt_fields + cycle_fields:
    df = df.with_columns(
        pl.when(mask).then(0.0).otherwise(pl.col(f'TEMP_{f}')).alias(f'TEMP_{f}')
    )

# -----------------------------
#  TEMP_AMT_1 to TEMP_AMT_3
# -----------------------------
for i in range(1, 4):
    col = f'AMT_{i}'
    temp_col = f'TEMP_AMT_{i}'
    if col in df.columns:
        df = df.with_columns(pos_val(col).alias(temp_col))
    else:
        df = df.with_columns(pl.lit(0.0).alias(temp_col))

# -----------------------------
#  Add row number (like _N_ in SAS)
# -----------------------------
df = df.with_row_count('NO', offset=1)

# -----------------------------
#  Select and reorder columns
# -----------------------------
report_columns = [
    'ALIASKEY', 'ALIAS', 'CUSTNAME', 'CUSTNO', 'DEMODESC', 'MASCODESC', 'SICCODE', 'MSICDESC',
    'ACCTNOC', 'BRANCH_ABBR', 'ACCTSTATUS', 'DATEOPEN', 'DATECLSE',
    'SDBIND', 'SDBBRH', 'TEMP_CURBAL',
    'TEMP_CURR_CYC_DR', 'TEMP_CURR_AMT_DR', 'TEMP_CURR_CYC_CR', 'TEMP_CURR_AMT_CR',
    'TEMP_PREV_CYC_DR', 'TEMP_PREV_AMT_DR', 'TEMP_PREV_CYC_CR', 'TEMP_PREV_AMT_CR',
    'ACCT_PST_IND', 'ACCT_PST_REASON',
    'TOT_HOLD', 'SEQID_1', 'TEMP_AMT_1', 'DESC_1', 'SOURCE_1',
    'SEQID_2', 'TEMP_AMT_2', 'DESC_2', 'SOURCE_2',
    'SEQID_3', 'TEMP_AMT_3', 'DESC_3', 'SOURCE_3'
]

# Keep only columns that exist in df
report_columns = [c for c in report_columns if c in df.columns]

# -----------------------------
#  Export to CSV (like FILE OUTFILE)
# -----------------------------
df.select(['NO'] + report_columns).write_csv(
    "PBB_REPORT.csv", sep=';', float_format="%.2f"
)
