import polars as pl

# -----------------------------
# Part 0: Read Parquet files
# -----------------------------
oldic = pl.read_parquet("OLDIC.parquet")      # OLDIC.GDG equivalent
newic = pl.read_parquet("NEWIC.parquet")      # NEWIC.GDG equivalent
rhold = pl.read_parquet("CISRHOLD.parquet")  # RHOLD.FULL.LIST equivalent

# -----------------------------
# Part 1: OLDIC processing
# -----------------------------
oldic = oldic.select([
    pl.col("CUSTNO").cast(pl.Utf8),
    pl.col("CODE_OLD").cast(pl.Utf8),
    pl.col("INDORG").cast(pl.Utf8),
    pl.col("OLDIC").cast(pl.Utf8),
    pl.col("CUSTBRCH").cast(pl.Utf8)
])
oldic = oldic.sort("CUSTNO")
print("OLD IC:\n", oldic.head(1))

# -----------------------------
# Part 2: NEWIC processing
# -----------------------------
newic = newic.select([
    pl.col("CUSTNO").cast(pl.Utf8),
    pl.col("CODE_NEW").cast(pl.Utf8),
    pl.col("NEWIC").cast(pl.Utf8),
    pl.col("KEYFIELD1").cast(pl.Utf8),
    pl.col("KEYFIELD2").cast(pl.Utf8)
])
newic = newic.with_columns(
    pl.col("NEWIC").str.slice(3, 20).alias("NEWIC1")  # SUBSTR(NEWIC,4,20)
)
newic = newic.sort("CUSTNO")
print("NEW IC:\n", newic.head(1))

# -----------------------------
# Part 3: RHOLD processing
# -----------------------------
rhold_alias1 = rhold.select(pl.col("ID1").alias("ALIAS")).filter(pl.col("ALIAS") != "")
rhold_alias2 = rhold.select(pl.col("ID2").alias("ALIAS")).filter(pl.col("ALIAS") != "")
rhold_all = pl.concat([rhold_alias1, rhold_alias2]).unique(subset="ALIAS").sort("ALIAS")
print("RHOLD:\n", rhold_all.head(1))

# -----------------------------
# Part 4: TAXID merge OLDIC + NEWIC
# -----------------------------
taxid = oldic.join(newic, on="CUSTNO", how="left")
taxid = taxid.with_columns(
    pl.when(pl.col("INDORG") == "O")
      .then(pl.col("NEWIC"))
      .otherwise(None)
      .alias("BUSREG")
)
taxid = taxid.sort("NEWIC1")
print("TAXID FILE:\n", taxid.head(1))

# -----------------------------
# Part 5: TAXID_NEWIC merge with RHOLD (NEWIC1)
# -----------------------------
rhold_newic = rhold_all.rename({"ALIAS": "NEWIC1"})
taxid_newic = taxid.join(rhold_newic, on="NEWIC1", how="left")
taxid_newic = taxid_newic.with_columns(
    pl.when(pl.col("NEWIC1").is_not_null() & pl.col("ALIAS").is_not_null())
      .then(1)
      .alias("C")
)
taxid_newic = taxid_newic.drop("ALIAS").sort("OLDIC")

# -----------------------------
# Part 6: TAXID_OLDIC merge with RHOLD (OLDIC)
# -----------------------------
rhold_oldic = rhold_all.rename({"ALIAS": "OLDIC"})
taxid_oldic = taxid_newic.join(rhold_oldic, on="OLDIC", how="left")
taxid_oldic = taxid_oldic.with_columns(
    pl.when(pl.col("OLDIC").is_not_null() & pl.col("ALIAS").is_not_null())
      .then(1)
      .alias("F")
)
taxid_oldic = taxid_oldic.drop("ALIAS").sort("CUSTNO")

# -----------------------------
# Part 7: OUT dataset creation
# -----------------------------
def match_type(row):
    if row["C"] == 1 and row["F"] == 1:
        return "B", "Y"
    elif row["C"] == 1 and row["F"] is None:
        return "N", "Y"
    elif row["C"] is None and row["F"] == 1:
        return "O", "Y"
    else:
        return "X", "N"

taxid_oldic = taxid_oldic.with_columns(
    pl.struct(["C", "F"]).apply(lambda x: match_type(x)).alias("MATCH_RHOLD")
)
taxid_oldic = taxid_oldic.with_columns(
    pl.col("MATCH_RHOLD").arr.get(0).alias("MATCHID"),
    pl.col("MATCH_RHOLD").arr.get(1).alias("RHOLD_IND")
).drop("MATCH_RHOLD")

# -----------------------------
# Part 8: Write output
# -----------------------------
taxid_oldic.select([
    "CUSTNO", "OLDIC", "NEWIC", "BUSREG", "CUSTBRCH", "RHOLD_IND", "MATCHID"
]).write_parquet("TAXID.parquet")
