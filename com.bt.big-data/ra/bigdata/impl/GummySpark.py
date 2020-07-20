from pyspark import SparkContext, SparkConf, Row
from pyspark import SQLContext
from pyspark.sql import functions as F

schema = ["TAG", "A_FLG", "C_FLG", "S_FLG", "C_TAG", "C_ACCT_NO", "C_INSTNSEQ_NO", "C_CLI", "C_DQSRVC_STAT",
          "C_INSTN_SD", "C_INSTN_ST", "C_ORD_STAT", "C_PROD_ID", "C_DQPROD_ID", "C_NTVPROD_NME", "C_SPID", "C_DQSP_ID",
          "C_SP_NME", "C_DQSP_NME", "C_ORD_TYP", "C_ORD_NUM", "C_INSTN_STAT", "C_INSTN_STAT_D", "C_DIST_ID",
          "C_INSTN_ED", "C_INSTN_ET", "C_CUST_TYP", "C_INSTN_ADR", "C_BILL_ADR", "C_EXCH_TYP", "C_INSTN_ADR1",
          "C_INSTN_ADR2", "C_INSTN_ADR3", "C_INSTN_ADR4", "C_INSTN_ADR5", "C_INSTN_ADR6", "C_INSTN_ADR7",
          "C_INSTN_ADR8", "C_INSTN_ADR9", "C_SRVC_CL", "C_DQSRVC_CL", "C_DQSRVC_CLID", "C_AUXCNT", "C_CR_FLG",
          "C_CRPROD_CD", "C_RCF_FLG", "C_RCFPROD_CD", "WS_TAG", "WS_RECD_DT", "WS_RECD_T", "WS_CLI", "WS_TRNSN_TYP",
          "WS_SRVC_STAT", "WS_DQSRVC_STAT", "WS_INSTN_DT", "WS_UNINST_DT", "WS_UNINST_T", "WS_AST_INTEG_ID",
          "WS_AST_TYP", "WS_BILLACCT_NUM", "WS_SPID", "WS_DQSP_NME", "WS_ADR_KEY", "WS_STREET_NUM", "WS_THRHFR_NME",
          "WS_LOCALITY", "WS_POST_TOWN", "WS_COUNTY", "WS_POST_CD", "WS_PROD_ID", "WS_PROD_CD", "WS_DQPROD_ID",
          "WS_DQPROD_NME", "WS_DIST_ID", "WS_EXCHGRP_CD", "WS_INSTN_TYP", "WS_CUP_ID", "WS_SRVC_CL", "WS_DQSRVC_CL",
          "WS_DQSRVC_CLID", "WS_LN_TYP", "WS_AUX_CNT", "CR_FLG", "CRSRVC_ID", "RCF_FLG", "RCFSRVC_ID", "A_TAG",
          "A_SRVC_ID", "A_CLI", "A_SRVC_STAT", "A_DQSRVC_STAT", "A_SPID", "A_SP_NME", "A_DQSP_NME", "A_CUST_REF_NO",
          "A_CUST_TYP", "A_SP_FSTLN_ADR", "A_SPPOST_CD", "A_PROD_ID", "A_PROD_NME", "A_BILL_ADR", "A_BILLADR_PCD",
          "A_CRD", "A_CRT", "A_CTD", "A_CTT", "A_ORORD_REF_NO", "A_CUST_ORD_NO", "A_CSS_ACCT_NO", "A_BILL_ACCT_NO",
          "A_ACCT_NME", "A_ACCT_STAT", "A_LST_BILL_DT", "A_NXT_BILL_DT", "A_BILL_CYL", "A_SPND_FBILL", "A_SRVC_CL",
          "A_DQSRVC_CL", "A_DQSRVC_CLID", "A_CLI_STAT", "A_CLI_SD", "A_CLI_ED", "A_AUX_CNT", "A_CR_FLG", "A_CRSRVC_ID",
          "A_RCF_FLG", "A_RCFSRVC_ID", "A_RCFIN_PRT", "A_THISCYCL_FLG", "SIEBEL_CL_RECN_FLG2_5", "OS_TAG", "CLI",
          "OS_SRVC_ID", "CH_SRVC_ID", "RECD_TYP", "RECD_DT", "AST_RID", "NTVSRVC_STAT", "DQSRVC_STAT", "CUP_ID",
          "DQSP_ID", "DQSP_NME", "BILL_ACCT", "RT_AST_ID", "PRT_AST_ID", "DQPROD_TYP", "PROD_CD", "PROD_ID", "LE_CD",
          "ADR_KEY", "AST_CRT_DT", "INST_DT", "UNINST_DT", "HOUSE_NO", "HOUSE_NME", "STREET_NUM", "THRHFR_NME",
          "LOCALITY", "POST_TOWN", "COUNTY", "POST_CD", "EFF_SD", "PRPSD_ED", "RETAILER_RID", "HAZARD_NOTES", "AD_FLG",
          "ATTR_RECD_DT", "AST_ATTR_RID", "CL", "DQCL", "DQSRVC_CLID", "NUMOF_DDIS", "DQSNDDI_QTY", "DQDDI_RG_QTY",
          "DQSNDDI_CLI", "DQDDI_RG", "SNDDI_CLI1", "SNDDI_CLI2", "SNDDI_CLI3", "SNDDI_CLI4", "SNDDI_CLI5", "SNDDI_CLI6",
          "SNDDI_CLI7", "SNDDI_CLI8", "SNDDI_CLI9", "SNDDI_CLI10", "SNDDI_CLI11", "SNDDI_CLI12", "SNDDI_CLI13",
          "SNDDI_CLI14", "SNDDI_CLI15", "SNDDI_CLI16", "SNDDI_CLI17", "SNDDI_CLI18", "SNDDI_CLI19", "SNDDI_CLI20",
          "SNDDI_CLI21", "SNDDI_CLI22", "SNDDI_CLI23", "SNDDI_CLI24", "SNDDI_CLI25", "SNDDI_CLI26", "SNDDI_CLI27",
          "SNDDI_CLI28", "SNDDI_CLI29", "SNDDI_CLI30", "DDI_RG1", "DDI_RG2", "DDI_RG3", "DDI_RG4", "DDI_RG5", "DDI_RG6",
          "DDI_RG7", "DDI_RG8", "DDI_RG9", "DDI_RG10", "DDI_RG11", "DDI_RG12", "DDI_RG13", "DDI_RG14", "DDI_RG15",
          "DDI_RG16", "DDI_RG17", "DDI_RG18", "DDI_RG19", "DDI_RG20", "DDI_RG21", "DDI_RG22", "DDI_RG23", "DDI_RG24",
          "DDI_RG25", "DDI_RG26", "DDI_RG27", "DDI_RG28", "DDI_RG29", "DDI_RG30", "ATT_LSTUPDT", "AST_ATTR_FLG",
          "OSCR_FLG", "OSRCF_FLG", "CR_FCLI", "CR_TCLI", "CRIN_CH", "OSCRSRVC_ID", "RCF_CLI", "RCFIN_CH",
          "OSRCFSRVC_ID", "CUST_CUP_ID", "CUSTACCT_NME", "BILL_NME", "CRFBILL_ACCT", "CUST_GRP", "CP_CNCT_NUM",
          "CUST_GRP_ID", "SAC", "ACCT_STAT", "CUST_REF_FLG", "OS_THISCYCL_FLG", "OS_FLG", "E_CLI", "E_EARLST_DT",
          "E_EARLIEST_T", "E_LATEST_DT", "E_LATEST_T", "E_CDR_CNT", "E_CSS", "E_SDP_OPTION", "E_SDP_SD", "E_SDP_ST",
          "E_SDP_ED", "E_SDP_ET", "E_NMP_OPTION", "E_NMP_SD", "E_NMP_ST", "E_NMP_ED", "E_NMP_ET", "E_SP_ID", "E_FLG",
          "ER_FLG", "THISCYCL_FLG", "E_T_CRTDT", "RACATEGORY", "CLASSIFICATION", "PGFLAG"]
path = "/home/cloudera/Documents/WLR3_PS_ALLCKT.txt"
conf = SparkConf().setAppName("Pyspark Pgm")
sc = SparkContext('spark://quickstart.cloudera:7077', conf=conf)
sqlContext = SQLContext(sc)
contentRDD = sc.textFile("file://{0}".format(path))
mappedValue = contentRDD.map(lambda x: x.split("|")).map(lambda x: x)
# print mappedValue.take(1)
dataFrame = sqlContext.createDataFrame(mappedValue, schema)
# print dataFrame.select("TAG", "PGFLAG").show(10)
print(dataFrame.groupBy("TAG", "PGFLAG").agg(F.lower(dataFrame.TAG), dataFrame.PGFLAG != 0).collect())
