# Completed with correct output
import polars as pl
from reader import load_input

#-------------------------------------------------------------------#
# Original Program: CCRCCRL1                                        #
#-------------------------------------------------------------------#
#-ESMR 2011-2834                                                    #
# DISPLAY OF CUST-TO-CUST RELATIONSHIP IN CAS-IMIS                  #
# APPEND DP/LN ACCOUNT    TO CC(INDV-ORG) AND CC(ORG-ORG) ONLY      #
# TO SHOW PERSONAL ACCOUNT ONLY. JOINT ACCOUNT DROP                 #
# APPEND ORIGINAL RECORD  TO CC(ORG-INDV) AND CC(INDV-INDV)         #
#-------------------------------------------------------------------#
#-ESMR 2014-764                                                     #
# CHANGE FILE FORMAT FROM FIXED LENGTH TO DELIMITED                 #
#-------------------------------------------------------------------#

#------------------------#
# Load parquet datasets  #
#------------------------#
primary = load_input("RLENCA_NONJOINT")   # PRIMREC
ccrlen1 = load_input("RLNSHIP_RLNIND")   # Individual
ccrlen  = load_input("RLNSHIP_RLNORG")   # Organisation

#------------------------------------------#
# Personal account file (No Joint Account) #
#------------------------------------------#
primary = primary.select([
    pl.col("ACCTNO").cast(pl.Utf8),
    pl.col("ACCTCODE").cast(pl.Utf8),
    pl.col("CUSTNO").cast(pl.Utf8)
])

primary = primary.sort("CUSTNO")
print("CA RELATIONSHIP")
print(primary.head(5))

#------------------------------------------------------#
# CUSTOMER-TO-CUSTOMER RELATIONSHIP FILE  (INDIVIDUAL) #
#------------------------------------------------------#
ccrlen1 = ccrlen1.rename({
    "INDORG1":"CUSTTYPE1",
    "INDORG2":"CUSTTYPE",
    "CODE1":"RLENCODE1",
    "CODE2":"RLENCODE",
    "CUSTNO2":"CUSTNO",
    "DESC2":"DESC",
    "CUSTNAME2":"CUSTNAME",
    "ALIAS2":"ALIAS"
})

ccrlen1 = ccrlen1.select([
    pl.col("CUSTNO1").cast(pl.Utf8).str.zfill(11),
    pl.col("CUSTTYPE1").cast(pl.Utf8),
    pl.col("RLENCODE1").cast(pl.Utf8),
    pl.col("DESC1").cast(pl.Utf8),
    pl.col("CUSTNO").cast(pl.Utf8).str.zfill(11),
    pl.col("CUSTTYPE").cast(pl.Utf8),
    pl.col("RLENCODE").cast(pl.Utf8),
    pl.col("DESC").cast(pl.Utf8),
    pl.col("CUSTNAME1").cast(pl.Utf8),
    pl.col("ALIAS1").cast(pl.Utf8),
    pl.col("CUSTNAME").cast(pl.Utf8),
    pl.col("ALIAS").cast(pl.Utf8)
])

ccrlen1 = ccrlen1.sort("CUSTNO")
print("CC RELATIONSHIP")
print(ccrlen1.head(5))

#--------------------------------------------------------#
# CUSTOMER-TO-CUSTOMER RELATIONSHIP FILE  (ORGANISATION) #
#--------------------------------------------------------#
ccrlen = ccrlen.rename({
    "INDORG1":"CUSTTYPE1",
    "INDORG2":"CUSTTYPE",
    "CODE1":"RLENCODE1",
    "CODE2":"RLENCODE",
    "CUSTNO2":"CUSTNO",
    "DESC2":"DESC",
    "CUSTNAME2":"CUSTNAME",
    "ALIAS2":"ALIAS"
})

ccrlen = ccrlen.select([
    pl.col("CUSTNO1").cast(pl.Utf8).str.zfill(11),
    pl.col("CUSTTYPE1").cast(pl.Utf8),
    pl.col("RLENCODE1").cast(pl.Utf8),
    pl.col("DESC1").cast(pl.Utf8),
    pl.col("CUSTNO").cast(pl.Utf8).str.zfill(11),
    pl.col("CUSTTYPE").cast(pl.Utf8),
    pl.col("RLENCODE").cast(pl.Utf8),
    pl.col("DESC").cast(pl.Utf8),
    pl.col("CUSTNAME1").cast(pl.Utf8),
    pl.col("ALIAS1").cast(pl.Utf8),
    pl.col("CUSTNAME").cast(pl.Utf8),
    pl.col("ALIAS").cast(pl.Utf8)
])

ccrlen = ccrlen.sort("CUSTNO")
print("CC RELATIONSHIP")
print(ccrlen.head(5))

#---------------------------------------------------------#
# Merge organisation CCRLEN with PRIMARY accounts         #
# Equivalent to PROC SQL join CCRLEN, PRIMARY on CUSTNO   #
#---------------------------------------------------------#
cc_primary = (
    ccrlen.join(primary, on="CUSTNO", how="inner")
    .select([
        "CUSTNO1","CUSTTYPE1","RLENCODE1","DESC1",
        "CUSTNO","CUSTTYPE","RLENCODE","DESC",
        "CUSTNAME1","ALIAS1","CUSTNAME","ALIAS",
        "ACCTNO","ACCTCODE"
    ])
)

cc_primary = cc_primary.sort(["CUSTNO", "ACCTCODE", "ACCTNO"])
print("CCRLEN + PRIM")
print(cc_primary.head(5))

# ----------------------------------------------#
#  Union with CCRLEN1 (individual relationship) #
# ----------------------------------------------#
out = pl.concat([cc_primary, ccrlen1], how="diagonal")

out1 = (
    out.select([
        "CUSTNO1","CUSTTYPE1","RLENCODE1","DESC1",
        "CUSTNO","CUSTTYPE","RLENCODE","DESC",
        "ACCTNO","ACCTCODE","CUSTNAME1","ALIAS1",
        "CUSTNAME","ALIAS"
    ])
)

print("OUT1: ")
print(out1.head(5))

# -------------------------------------#
# Write Parquet and CSV file           #
# CSV style with quotes and \N at end  #
# -------------------------------------#
out1.write_parquet("cis_internal/output/PARTIES.parquet")
out1.write_csv("cis_internal/output/IMIS.csv", quote_style="always")
