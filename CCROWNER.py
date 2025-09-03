import polars as pl

# =====================================================
# STEP 0: READ ALL INPUT FILES (equivalent to SAS INFILE)
# =====================================================
# Replace 'path_to_file' with actual parquet file paths

# Step 1 - CA Relationship
df_ca = pl.read_parquet("path_to_RLEN_CA.parquet")  # Replace with actual file

# Step 2 - CC Relationship
df_cc = pl.read_parquet("path_to_RLEN_CC.parquet")  # Replace with actual file

# Step 3 - Original CC relationship (to flip)
df_cc_orig = pl.read_parquet("path_to_CISRLCC.parquet")  # Replace with actual file

# =====================================================
# STEP 1 - GET CA RELATIONSHIP
# =====================================================
# Map ACCTNOR to ACCTCODE
df_ca = df_ca.with_columns(
    pl.when(pl.col("ACCTNOR").is_in(["01","03","04","05","06","07"]))
      .then("DP")
      .when(pl.col("ACCTNOR").is_in(["02","08"]))
      .then("LN")
      .otherwise(None)
      .alias("ACCTCODE")
)

# Filter PRISEC = '901'
df_ca = df_ca.filter(pl.col("PRISEC") == "901")

# Sort by ACCTNOC
df_ca = df_ca.sort("ACCTNOC")

# Output equivalent to TEMPOUT
df_ca_out = df_ca.select(["ACCTCODE", "ACCTNOC", "CUSTNO", "RLENCODE"])

# =====================================================
# STEP 2 - FLIP CC RELATIONSHIP
# =====================================================
# Flip CUST1/CODE1 with CUST2/CODE2
df_cc_flipped = df_cc_orig.select([
    pl.col("CUST2").alias("CUSTNO"),
    pl.col("CODE2").alias("CODE1"),
    pl.col("CUST1").alias("CUSTNO2"),
    pl.col("CODE1").alias("CODE2")
])

# =====================================================
# STEP 3 - MATCH RECORD WITH CC RELATIONSHIP
# =====================================================
# Ensure df_ca and df_cc_flipped have correct column names
df_ca_sorted = df_ca_out.sort("CUSTNO")
df_cc_sorted = df_cc_flipped.sort("CUSTNO")

# Join on CUSTNO
df_merge1 = df_ca_sorted.join(df_cc_sorted, on="CUSTNO", how="inner")

# Sort by ACCTNOC, CUSTNO, CUSTNO2
df_merge1 = df_merge1.sort(["ACCTNOC", "CUSTNO", "CUSTNO2"])

# Output equivalent to TEMPOUT
df_merge_out = df_merge1.select([
    "ACCTCODE",
    "ACCTNOC",
    "RLENCODE",
    "CUSTNO",
    "CODE1",
    "CUSTNO2",
    "CODE2"
])

# =====================================================
# STEP 4 - REMOVING DUPLICATE RECORDS
# =====================================================
# Polars drop_duplicates keeps the first occurrence by default
df_unique = df_merge_out.unique(subset=df_merge_out.columns, keep="first")

# Save final output
df_unique.write_parquet("CISOWNER.parquet")  # Replace with desired path
