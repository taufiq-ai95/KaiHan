import polars as pl

# -----------------------------
# READ SOURCE DATA (Equivalent to INFILE)
# -----------------------------
# Replace 'read_parquet' with your actual paths or CSV files if needed
df_name = pl.read_parquet("RBP2.B033.CCRIS.CISNAME.TEMP.parquet")
df_rmrk = pl.read_parquet("RBP2.B033.CCRIS.CISRMRK.LONGNAME.parquet")

# -----------------------------
# NAME DATASET PROCESSING
# -----------------------------
# Keep only relevant columns to mimic SAS INPUT statement
df_name = df_name.select([
    pl.col("CUSTNO").cast(pl.Utf8),
    pl.col("CUSTNAME").cast(pl.Utf8),
    pl.col("ADREFNO").cast(pl.Utf8),
    pl.col("PRIPHONE").cast(pl.Utf8),
    pl.col("SECPHONE").cast(pl.Utf8),
    pl.col("CUSTTYPE").cast(pl.Utf8),
    pl.col("CUSTNAME").cast(pl.Utf8).alias("CUSTNAME2"),
    pl.col("MOBILEPHONE").cast(pl.Utf8)
])

# Remove duplicates by CUSTNO
df_name = df_name.unique(subset=["CUSTNO"])

# Print first 5 rows (like PROC PRINT OBS=5)
print("NAME:")
print(df_name.head(5))

# -----------------------------
# REMARKS DATASET PROCESSING
# -----------------------------
df_rmrk = df_rmrk.select([
    pl.col("BANKNO").cast(pl.Utf8),
    pl.col("APPLCODE").cast(pl.Utf8),
    pl.col("CUSTNO").cast(pl.Utf8),
    pl.col("EFFDATE").cast(pl.Utf8),
    pl.col("RMKKEYWORD").cast(pl.Utf8),
    pl.col("LONGNAME").cast(pl.Utf8),
    pl.col("RMKOPERATOR").cast(pl.Utf8),
    pl.col("EXPIREDATE").cast(pl.Utf8),
    pl.col("LASTMNTDATE").cast(pl.Utf8)
])

# Remove duplicates by CUSTNO
df_rmrk = df_rmrk.unique(subset=["CUSTNO"])

# Print first 5 rows
print("REMARKS:")
print(df_rmrk.head(5))

# -----------------------------
# MERGE NAME AND REMARKS (Equivalent to DATA MERGE; MERGE NAME RMRK BY CUSTNO; IF A;)
# -----------------------------
df_merge = df_name.join(df_rmrk, on="CUSTNO", how="left")  # left join to mimic IF A;

# Sort by CUSTNO
df_merge = df_merge.sort("CUSTNO")

# Print first 5 rows
print("MERGE:")
print(df_merge.head(5))

# -----------------------------
# OUTPUT DETAIL REPORT (Equivalent to DATA OUT; PUT ...)
# -----------------------------
# Keep columns in the same order as SAS PUT statement
df_out = df_merge.select([
    "CUSTNO",
    "CUSTNAME",
    "ADREFNO",
    "PRIPHONE",
    "SECPHONE",
    "CUSTTYPE",
    "CUSTNAME2",
    "MOBILEPHONE",
    "LONGNAME"
])

# Write to Parquet (or CSV if needed)
df_out.write_parquet("RBP2.B033.CCRIS.CISNAME.OUT.parquet")
