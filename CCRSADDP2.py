import polars as pl

#--------------------------------#
# READ PARQUET FILES (ALL AT TOP)#
#--------------------------------#

aown_raw   = pl.read_parquet("ADDRAOWN_FB.parquet")
dpaddr_raw = pl.read_parquet("DP_DAILY_ADDRACC.parquet")


#--------------------------------#
# Part 1 - PROCESS AOWNFILE      #
#--------------------------------#

aown = (
    aown_raw
    .filter(
        (pl.col("O_APPL_CODE") == "DP   ") & 
        (pl.col("ACCTNO") > "010000000000")
    )
    .unique(subset=["ACCTNO"])   # PROC SORT NODUPKEY
)

print("AOWN")
print(aown.head(5))


#--------------------------------#
# Part 2 - PROCESS DEPOSIT FILE  #
#--------------------------------#

dpaddr = (
    dpaddr_raw
    .filter(pl.col("ACCTNO") > "010000000000")
    .unique(subset=["ACCTNO"])
)

print("DEPOSIT ADDRESS")
print(dpaddr.head(5))


#--------------------------------#
# Part 3 - MERGE ON ACCTNO       #
#--------------------------------#

merge = (
    dpaddr.join(aown, on="ACCTNO", how="inner")   # SAS MERGE with IN=A and IN=B
    .sort("ACCTNO")
)

print("MERGED")
print(merge.head(5))


#--------------------------------#
# Part 4 - CREATE OUTPUT FILE    #
#--------------------------------#

out = merge.select([
    pl.col("O_APPL_CODE").alias("O_APPL_CODE"),
    pl.col("ACCTNO").alias("ACCTNO"),
    pl.col("NA_LINE_TYP1"),
    pl.col("ADD_NAME_1"),
    pl.col("NA_LINE_TYP2"),
    pl.col("ADD_NAME_2"),
    pl.col("NA_LINE_TYP3"),
    pl.col("ADD_NAME_3"),
    pl.col("NA_LINE_TYP4"),
    pl.col("ADD_NAME_4"),
    pl.col("NA_LINE_TYP5"),
    pl.col("ADD_NAME_5"),
    pl.col("NA_LINE_TYP6"),
    pl.col("ADD_NAME_6"),
    pl.col("NA_LINE_TYP7"),
    pl.col("ADD_NAME_7"),
    pl.col("NA_LINE_TYP8"),
    pl.col("ADD_NAME_8"),
])

# Save to parquet (instead of fixed-length file)
out.write_parquet("DAILY_ADDRACC.parquet")

print("OUT FILE SAMPLE")
print(out.head(5))
