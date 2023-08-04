exec app_utils.trunc_table('YM_WORK', 'DDM_PNR_2_W');
insert  /*+ append */ into DDM_PNR_2_W
(
    PNR_LOC_CD,
    PNR_CREATE_DT,
    DCR_OD_FLT_DPTR_DT,
    DCR_OD_ORIG,
    DCR_OD_DEST,
    FLT_DPTR_DT,
    FLT_ID,
    FLT_NBR,
    LEG_ORIG,
    LEG_DEST,
    ALN_CD,
    DCR_CABIN_CD,
    SRVC_CLS,
    FLT_DPTR_TMS,
    CDSHR_IND,
    LCL_FLW_TYPE_IND,
    SABRE_DPTR_DTTM,
    FCST_DPTR_DT,
    FCST_DOW,
    FCST_ID,
    FCST_CLS,
    FLW_EBP,
    EVT_OD_FARE,
    TRAFFIC_CT,
    OTH_LEG_FLW_EBP,
    OVNT_FLAG,
    INSERT_DT
)
select  /*+ parallel (8) */
    PNR_LOC_CD,
    PNR_CREATE_DT,
    DCR_OD_FLT_DPTR_DT,
    DCR_OD_ORIG,
    DCR_OD_DEST,
    FLT_DPTR_DT,
    FLT_ID,
    FLT_NBR,
    LEG_ORIG,
    LEG_DEST,
    ALN_CD,
    DCR_CABIN_CD,
    SRVC_CLS,
    FLT_DPTR_TMS,
    CDSHR_IND,
    LCL_FLW_TYPE_IND,
    SABRE_DPTR_DTTM,
    FCST_DPTR_DT,
    FCST_DOW,
    FCST_ID,
    FCST_CLS,
    FLW_EBP,
    EVT_OD_FARE,
    TRAFFIC_CT,
    0 as OTH_LEG_FLW_EBP,
    0 as OVNT_FLAG,
    trunc (sysdate) as INSERT_DT
from
    DDM_PNR_1_W
where
        LCL_FLW_TYPE_IND = 'L';
commit;
insert  /*+ append */ into DDM_PNR_2_W
(
    PNR_LOC_CD,
    PNR_CREATE_DT,
    DCR_OD_FLT_DPTR_DT,
    DCR_OD_ORIG,
    DCR_OD_DEST,
    FLT_DPTR_DT,
    FLT_ID,
    FLT_NBR,
    LEG_ORIG,
    LEG_DEST,
    ALN_CD,
    DCR_CABIN_CD,
    SRVC_CLS,
    FLT_DPTR_TMS,
    CDSHR_IND,
    LCL_FLW_TYPE_IND,
    SABRE_DPTR_DTTM,
    FCST_DPTR_DT,
    FCST_DOW,
    FCST_ID,
    FCST_CLS,
    FLW_EBP,
    EVT_OD_FARE,
    TRAFFIC_CT,
    OTH_LEG_FLT_DPTR_DT,
    OTH_LEG_FLT_ID,
    OTH_LEG_FLT_NBR,
    OTH_LEG_ORIG,
    OTH_LEG_DEST,
    OTH_LEG_ALN_CD,
    OTH_LEG_DCR_CABIN_CD,
    OTH_LEG_SRVC_CLS,
    OTH_LEG_FCST_DPTR_DT,
    OTH_LEG_FCST_ID,
    OTH_LEG_FCST_CLS,
    OTH_LEG_FLW_EBP,
    OVNT_FLAG,
    INSERT_DT
)
select  /*+ parallel (8) */
    A.PNR_LOC_CD,
    A.PNR_CREATE_DT,
    A.DCR_OD_FLT_DPTR_DT,
    A.DCR_OD_ORIG,
    A.DCR_OD_DEST,
    A.FLT_DPTR_DT,
    A.FLT_ID,
    A.FLT_NBR,
    A.LEG_ORIG,
    A.LEG_DEST,
    A.ALN_CD,
    A.DCR_CABIN_CD,
    A.SRVC_CLS,
    A.FLT_DPTR_TMS,
    A.CDSHR_IND,
    A.LCL_FLW_TYPE_IND,
    A.SABRE_DPTR_DTTM,
    A.FCST_DPTR_DT,
    A.FCST_DOW,
    A.FCST_ID,
    A.FCST_CLS,
    A.FLW_EBP,
    A.EVT_OD_FARE,
    A.TRAFFIC_CT,
    B.FLT_DPTR_DT as OTH_LEG_FLT_DPTR_DT,
    B.FLT_ID as OTH_LEG_FLT_ID,
    B.FLT_NBR as OTH_LEG_FLT_NBR,
    B.LEG_ORIG as OTH_LEG_ORIG,
    B.LEG_DEST as OTH_LEG_DEST,
    B.ALN_CD as OTH_LEG_ALN_CD,
    B.DCR_CABIN_CD as OTH_LEG_DCR_CABIN_CD,
    B.SRVC_CLS as OTH_LEG_SRVC_CLS,
    B.FCST_DPTR_DT as OTH_LEG_FCST_DPTR_DT,
    B.FCST_ID as OTH_LEG_FCST_ID,
    B.FCST_CLS as OTH_LEG_FCST_CLS,
    B.FLW_EBP as OTH_LEG_FLW_EBP,
    B.FCST_DPTR_DT - A.FCST_DPTR_DT as OVNT_FLAG,
    trunc (sysdate) as INSERT_DT
from
    DDM_PNR_1_W A,
    DDM_PNR_1_W B
where
        A.LCL_FLW_TYPE_IND = 'F' and
        B.LCL_FLW_TYPE_IND = 'F' and
        A.PNR_LOC_CD = B.PNR_LOC_CD and
        A.PNR_CREATE_DT = B.PNR_CREATE_DT and
        A.DCR_OD_FLT_DPTR_DT = B.DCR_OD_FLT_DPTR_DT and
        A.DCR_OD_ORIG = B.DCR_OD_ORIG and
        A.DCR_OD_DEST = B.DCR_OD_DEST and
    (A.LEG_ORIG = B.LEG_DEST or A.LEG_DEST = B.LEG_ORIG) and
    (B.FCST_DPTR_DT - A.FCST_DPTR_DT) between -1 and 1;
commit;
exec app_utils.table_stats('YM_WORK','DDM_PNR_2_W');
