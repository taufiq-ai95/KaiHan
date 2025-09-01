import polars as pl

# ==============================
# 1. INITIAL SETUP
# ==============================
# Input parquet datasets (already converted from mainframe datasets)
alias_df   = pl.read_parquet("ALIAS.parquet")
branch_df  = pl.read_parquet("BRANCH.parquet")
occup_df   = pl.read_parquet("OCCUP.parquet")
masco_df   = pl.read_parquet("MASCO.parquet")
msic_df    = pl.read_parquet("MSIC.parquet")
cust_df    = pl.read_parquet("CUSTOMER.parquet")
deposit_df = pl.read_parquet("DEPOSIT.parquet")
cycle_df   = pl.read_parquet("CYCLE.parquet")
post_df    = pl.read_parquet("POST.parquet")
loan_df    = pl.read_parquet("LOAN.parquet")
sdb_df     = pl.read_parquet("SDB.parquet")
unicard_df = pl.read_parquet("UNICARD.parquet")
comcard_df = pl.read_parquet("COMCARD.parquet")

# ==============================
# 2. REFERENCE FILES (alias, branch, occup, masco, msic)
# ==============================
# Ensure description fields are not null
alias_df  = alias_df.fill_null("NIL")
branch_df = branch_df.fill_null("NIL")
occup_df  = occup_df.fill_null("NIL")
masco_df  = masco_df.fill_null("NIL")
msic_df   = msic_df.fill_null("NIL")

# ==============================
# 3. DEPOSIT PROCESSING
# ==============================
# Merge deposit with branch info
deposit_enriched = (
    deposit_df
    .join(branch_df, on="branch_code", how="left")
    .join(cycle_df, on="acct_id", how="left")
    .join(post_df, on="acct_id", how="left")
)

# Keep only FD / SA / CA accounts for balances
deposit_enriched = deposit_enriched.filter(
    pl.col("acct_type").is_in(["FD", "SA", "CA"])
)

# ==============================
# 4. SAFE DEPOSIT BOX (SDB)
# ==============================
sdb_enriched = (
    sdb_df
    .join(alias_df, on="cust_id", how="left")
    .join(cust_df, on="cust_id", how="left")
)

# ==============================
# 5. LOANS
# ==============================
loan_enriched = (
    loan_df
    .join(branch_df, on="branch_code", how="left")
)

# Add derived status (Active, Paid-off, NPL, etc.)
loan_enriched = loan_enriched.with_columns([
    pl.when(pl.col("status_code") == "A").then("ACTIVE")
     .when(pl.col("status_code") == "P").then("PAID_OFF")
     .when(pl.col("status_code") == "N").then("NPL")
     .otherwise("UNKNOWN")
     .alias("loan_status")
])

# ==============================
# 6. CARDS
# ==============================
card_enriched = (
    unicard_df.join(comcard_df, on="cust_id", how="outer")
)

# ==============================
# 7. MASTER MERGE
# ==============================
# Merge everything into a single customer view
master_df = (
    cust_df
    .join(alias_df, on="cust_id", how="left")
    .join(occup_df, on="occup_code", how="left")
    .join(masco_df, on="masco_code", how="left")
    .join(msic_df, on="msic_code", how="left")
    .join(deposit_enriched, on="cust_id", how="left")
    .join(loan_enriched, on="cust_id", how="left")
    .join(sdb_enriched, on="cust_id", how="left")
    .join(card_enriched, on="cust_id", how="left")
)

# ==============================
# 8. REPORT PREPARATION
# ==============================
report_df = (
    master_df
    .with_columns([
        pl.col("cur_bal").fill_null(0).alias("TEMP_CURBAL"),
        pl.col("amt_dr").fill_null(0).alias("TEMP_CURR_AMT_DR"),
        pl.col("amt_cr").fill_null(0).alias("TEMP_CURR_AMT_CR"),
        pl.col("demodesc").fill_null("NIL").alias("DEMODESC"),
        pl.col("mascod").fill_null("NIL").alias("MASCODESC"),
        pl.col("siccode").fill_null("NIL").alias("SICCODE"),
        pl.col("msicdesc").fill_null("NIL").alias("MSICDESC")
    ])
)

# ==============================
# 9. OUTPUT
# ==============================
report_df.write_parquet("CMDREPORT.parquet")
master_df.write_parquet("CMDRAW.parquet")

print("✅ Report and master datasets generated successfully.")
