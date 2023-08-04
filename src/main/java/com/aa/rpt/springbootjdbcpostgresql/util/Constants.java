package com.aa.rpt.springbootjdbcpostgresql.util;

public class Constants {

    private static final String SELECT_FLIGHTS_BY_MARKET =
            "select leg_orig,"+
                    "leg_dest, "+
                    "rrd_flt, "+
                    "rrd_fcst, "+
                    "fcst_dow, "+
                    "dptr_tm, "+
                    "flt_id, "+
                    "flt_nbr, "+
                    "fcst_id, "+
                    "cabin_cd, "+
                    "fcst_cls, "+
                    "lcl_flw_ind, "+
                    "mwp_now_fcst, "+
                    "mwp_now_fcst_err, "+
                    "mwp_later_fcst, "+
                    "mwp_later_fcst_err, "+
                    "lowest_fcst_cls, "+
                    "lowest_disp_cls, "+
                    "flt_dptr_dt, "+
                    "aircraft_config_s, "+
                    "min(price_point_ind) as fare_avail, "+
                    "idtd_ratio_now, "+
                    "idtd_ratio_later "+
                    "from "+
                    "( "+
                    "select A1.*, B1.aircraft_config_s,b2.disp_cls_id, b2.disp_cls, " +
                    "nvl(b2.lower_bound, b2_default.lower_bound) as lower_bound, "+
                    "nvl(b2.upper_bound, b2_default.upper_bound) as upper_bound, "+
                    "case when mkt.lcl_fare is null then 0 else 1 end as price_point_ind "+
                    "from "+
                    "( "+
                    "select "+
                    "A.LEG_ORIG,A.LEG_DEST, "+
                    "trunc(A.FLT_DPTR_DT) - (select trunc(getcollectdate) from dual) as rrd_flt, "+
                    "trunc(A.FCST_DPTR_DT) - (select trunc(getcollectdate) from dual) as rrd_fcst, "+
                    "A.FCST_DOW,A.DPTR_TM,A.FLT_ID,A.FLT_NBR,A.FCST_ID,A.CABIN_CD,A.FCST_CLS, "+
                    "A.LCL_FLW_IND,A.MWP_NOW_FCST,A.MWP_NOW_FCST_ERR,A.MWP_LATER_FCST, "+
                    "A.MWP_LATER_FCST_ERR,A.IDTD_RATIO_NOW,A.IDTD_RATIO_LATER,D.LOWEST_FCST_CLS,NVL(D.LOWEST_DISP_CLS,11) as lowest_disp_cls,A.FLT_DPTR_DT "+
                    "from "+
                    "( "+
                    "select F.LEG_ORIG_S as LEG_ORIG, F.LEG_DEST_S as LEG_DEST,F.FLT_DPTR_DATE_D as FLT_DPTR_DT, trunc(F.DPTR_TIME_D - (3 / 24)) as FCST_DPTR_DT, "+
                    "case when to_char(F.DPTR_TIME_D - (3 / 24), 'D') = 1 then 7 "+
                    "else to_char(F.DPTR_TIME_D - (3 / 24), 'D') - 1 end as FCST_DOW, "+
                    "to_char(F.DPTR_TIME_D, 'YYYY-MM-DD HH24:MI') as DPTR_TM,  "+
                    "F.FLT_ID_I as FLT_ID,F.FLT_NBR_I as FLT_NBR,B.FCST_ID as FCST_ID,B.CABIN_CD as CABIN_CD, B.FCST_CLS as FCST_CLS, "+
                    "B.LCL_FLW_TYPE_IND as LCL_FLW_IND,B.MWP_NOW_FCST as MWP_NOW_FCST,B.MWP_NOW_FCST_ERR as MWP_NOW_FCST_ERR,B.MWP_LATER_FCST as MWP_LATER_FCST, "+
                    "B.MWP_LATER_FCST_ERR as MWP_LATER_FCST_ERR, case when B.TD_NOW_FCST=0 then 0 else ROUND((B.ID_NOW_FCST/B.TD_NOW_FCST),4) end as IDTD_RATIO_NOW, case when B.TD_LATER_FCST=0 then 0 else ROUND((B.ID_LATER_FCST/B.TD_LATER_FCST),4) end as IDTD_RATIO_LATER  "+
                    "from FD F, NOW_LATER_FCST B "+
                    "where F.FLT_ID_I = B.FLT_ID and F.FLT_DPTR_DATE_D = B.FLT_DPTR_DT "+
                    "and F.LEG_ORIG_S = ? and F.LEG_DEST_S = ? and F.FLT_DPTR_DATE_D >= ? and F.FLT_DPTR_DATE_D <= ?  "+
                    //"and F.FLT_ID_I=1708 "+
                    ") A "+
                    "left join LOWEST_LOCAL_PRICE_CLASS D on A.FLT_ID = D.FLT_ID and A.FLT_DPTR_DT = D.FLT_DPTR_DT and A.CABIN_CD = D.CABIN_CD "+
                    "order by A.FLT_DPTR_DT,A.FLT_ID,A.CABIN_CD DESC,A.LCL_FLW_IND, A.FCST_CLS "+
                    ") A1 "+
                    "left join "+
                    "(select flt_dptr_date_d, flt_id_i,aircraft_config_s from FLIGHTS where AIRLINE_CODE_S = 'AA' ) B1 on A1.FLT_DPTR_DT=B1.flt_dptr_date_d and A1.flt_id=B1.flt_id_i "+
                    "left join "+
                    "( "+
                    "select dm.leg_orig, dm.leg_dest, dm.disp_cls_id, dm.cabin_cd, dc.disp_cls, dc.lower_bound, dc.upper_bound "+
                    "from legopt.disp_market_ref dm "+
                    "inner join legopt.disp_class_ref dc on dm.disp_cls_id = dc.disp_cls_id "+
                    ") b2 "+
                    "on a1.LEG_ORIG = b2.LEG_ORIG and a1.leg_dest = b2.leg_dest and a1.cabin_cd = b2.cabin_cd and a1.FCST_CLS = b2.disp_cls "+
                    //default markets
                    "left join "+
                    "(select dm.leg_orig, dm.leg_dest, dm.disp_cls_id, dm.cabin_cd, dc.disp_cls, dc.lower_bound, dc.upper_bound "+
                    "from legopt.disp_market_ref dm "+
                    "inner join legopt.disp_class_ref dc on dm.disp_cls_id = dc.disp_cls_id "+
                    "where dm.leg_orig = '*' and dm.leg_dest = '*' "+
                    ")b2_default "+
                    "on a1.cabin_cd = b2_default.cabin_cd and a1.fcst_cls = b2_default.disp_cls "+
                    "left join fcst.market_fare_w mkt "+
                    "on ( (a1.leg_orig||a1.leg_dest in ("+
                    "'ATLORD', 'ORDDEN', 'CLTDEN', 'ATLDFW', 'BWIDFW', 'ORDDFW', 'DENDFW', 'DFWDTW', 'ORDFLL', 'CLTFLL', " +
                    "'DFWFLL', 'ORDRSW', 'ORDHOU', 'ORDIAH', 'ORDLAS', 'CLTLAS', 'DFWLAS', 'ATLLAX', 'AUSLAX', 'BNALAX', " +
                    "'ORDLAX', 'ORDONT', 'DENLAX', 'DFWLAX', 'DFWONT', 'FLLLAX', 'HOULAX', 'IAHLAX', 'LASLAX', 'ATLLGA', " +
                    "'BNALGA', 'CLTLGA', 'DFWLGA', 'DTWLGA', 'HOULGA', 'IAHLGA', 'ATLMIA', 'AUSMIA', 'BNAMIA', 'BOSMIA', " +
                    "'BWIMIA', 'ORDMIA', 'CHSMIA', 'CLEMIA', 'CLTMIA', 'CMHMIA', 'CVGMIA', 'DENMIA', 'DFWMIA', 'DTWMIA', " +
                    "'EWRMIA', 'GSPMIA', 'HOUMIA', 'IAHMIA', 'INDMIA', 'LASMIA', 'LGAMIA', 'MEMMIA', 'ORDMSY', 'DFWMSY', " +
                    "'MIAMSY', 'MIAORF', 'ORDMCO', 'CLTMCO', 'DFWMCO', 'MIAMCO', 'ATLPHL', 'BNAPHL', 'BOSPHL', 'ORDPHL', " +
                    "'CLTPHL', 'CVGPHL', 'DENPHL', 'DFWPHL', 'DTWPHL', 'FLLPHL', 'RSWPHL', 'JAXPHL', 'LASPHL', 'LAXPHL', " +
                    "'ONTPHL', 'MIAPHL', 'MSYPHL', 'MCOPHL', 'PBIPHL', 'ATLPHX', 'BILPHX', 'BNAPHX', 'BOIPHX', 'ORDPHX', " +
                    "'CIDPHX', 'CVGPHX', 'DENPHX', 'DFWPHX', 'DSMPHX', 'DTWPHX', 'EUGPHX', 'FARPHX', 'FSDPHX', 'XNAPHX', " +
                    "'GEGPHX', 'GJTPHX', 'GRRPHX', 'HOUPHX', 'IAHPHX', 'IDAPHX', 'INDPHX', 'LASPHX', 'MIAPHX', 'MCIPHX', " +
                    "'MSPPHX', 'OMAPHX', 'MCOPHX', 'PDXPHX', 'PHLPHX', 'MIAPIT', 'MIAPNS', 'MIARDU', 'PHLRDU', 'MIARIC', " +
                    "'DFWSAN', 'PHXSAN', 'PHLSAV', 'MIASDF', 'PHXSEA', 'DFWSFO', 'PHXSFO', 'LAXSLC', 'PHXSLC', 'PHXSNA', " +
                    "'PHLSRQ', 'MIASTL', 'ORDTPA', 'CLTTPA', 'DFWTPA', 'MIATPA', 'PHLTPA', 'LAXTUL', 'MIATYS', 'BNADCA', " +
                    "'BNAIAD', 'JAXDCA', 'JAXIAD', 'SRQDCA', 'SRQIAD', 'ORDATL', 'DENORD', 'DENCLT', 'DFWATL', 'DFWBWI', " +
                    "'DFWORD', 'DFWDEN', 'DTWDFW', 'FLLORD', 'FLLCLT', 'FLLDFW', 'RSWORD', 'HOUORD', 'IAHORD', 'LASORD', " +
                    "'LASCLT', 'LASDFW', 'LAXATL', 'LAXAUS', 'LAXBNA', 'LAXORD', 'ONTORD', 'LAXDEN', 'LAXDFW', 'ONTDFW', " +
                    "'LAXFLL', 'LAXHOU', 'LAXIAH', 'LAXLAS', 'LGAATL', 'LGABNA', 'LGACLT', 'LGADFW', 'LGADTW', 'LGAHOU', " +
                    "'LGAIAH', 'MIAATL', 'MIAAUS', 'MIABNA', 'MIABOS', 'MIABWI', 'MIAORD', 'MIACHS', 'MIACLE', 'MIACLT', " +
                    "'MIACMH', 'MIACVG', 'MIADEN', 'MIADFW', 'MIADTW', 'MIAEWR', 'MIAGSP', 'MIAHOU', 'MIAIAH', 'MIAIND', " +
                    "'MIALAS', 'MIALGA', 'MIAMEM', 'MSYORD', 'MSYDFW', 'MSYMIA', 'ORFMIA', 'MCOORD', 'MCOCLT', 'MCODFW', " +
                    "'MCOMIA', 'PHLATL', 'PHLBNA', 'PHLBOS', 'PHLORD', 'PHLCLT', 'PHLCVG', 'PHLDEN', 'PHLDFW', 'PHLDTW', " +
                    "'PHLFLL', 'PHLRSW', 'PHLJAX', 'PHLLAS', 'PHLLAX', 'PHLONT', 'PHLMIA', 'PHLMSY', 'PHLMCO', 'PHLPBI', " +
                    "'PHXATL', 'PHXBIL', 'PHXBNA', 'PHXBOI', 'PHXORD', 'PHXCID', 'PHXCVG', 'PHXDEN', 'PHXDFW', 'PHXDSM', " +
                    "'PHXDTW', 'PHXEUG', 'PHXFAR', 'PHXFSD', 'PHXXNA', 'PHXGEG', 'PHXGJT', 'PHXGRR', 'PHXHOU', 'PHXIAH', " +
                    "'PHXIDA', 'PHXIND', 'PHXLAS', 'PHXMIA', 'PHXMCI', 'PHXMSP', 'PHXOMA', 'PHXMCO', 'PHXPDX', 'PHXPHL', " +
                    "'PITMIA', 'PNSMIA', 'RDUMIA', 'RDUPHL', 'RICMIA', 'SANDFW', 'SANPHX', 'SAVPHL', 'SDFMIA', 'SEAPHX', " +
                    "'SFODFW', 'SFOPHX', 'SLCLAX', 'SLCPHX', 'SNAPHX', 'SRQPHL', 'STLMIA', 'TPAORD', 'TPACLT', 'TPADFW', " +
                    "'TPAMIA', 'TPAPHL', 'TULLAX', 'TYSMIA', 'DCABNA', 'IADBNA', 'DCAJAX', 'IADJAX', 'DCASRQ', 'IADSRQ'" +
                    ") and a1.leg_orig||a1.leg_dest = mkt.dirmkt "+
                    "and instr(mkt.DAY_OF_WEEK_S,to_char(a1.flt_dptr_dt-1, 'D')) > 0 " +
                    "and a1.cabin_cd = mkt.cabin_cd and round(lcl_fare) between NVL(b2.lower_bound, b2_default.lower_bound) and NVL(b2.upper_bound, b2_default.upper_bound) ) "+
                    "or "+
                    "( a1.leg_orig||a1.leg_dest = mkt.dirmkt "+
                    "and a1.flt_dptr_dt between mkt.trvl_eff_date_d and mkt.trvl_dis_date_d "+
                    "and instr(mkt.DAY_OF_WEEK_S,to_char(a1.flt_dptr_dt-1, 'D')) > 0 "+
                    "and a1.cabin_cd = mkt.cabin_cd and round(lcl_fare) between NVL(b2.lower_bound, b2_default.lower_bound) and NVL(b2.upper_bound, b2_default.upper_bound)) ) ) "+
                    "group by leg_orig, leg_dest, rrd_flt, rrd_fcst, fcst_dow, dptr_tm, flt_id, flt_nbr, fcst_id, cabin_cd, fcst_cls, lcl_flw_ind, "+
                    "mwp_now_fcst, mwp_now_fcst_err, mwp_later_fcst, mwp_later_fcst_err, idtd_ratio_now, idtd_ratio_later, lowest_fcst_cls, lowest_disp_cls, flt_dptr_dt, aircraft_config_s "+
                    "order by leg_orig, leg_dest, flt_dptr_dt,flt_id, flt_nbr, cabin_cd desc, lcl_flw_ind, fcst_cls";

    private static final String SELECT_PNR =
                    "select " +
                    "PNR_LOC_CD, " +												// 1
                    "PNR_CREATE_DT, " +												// 2
                    "trunc(FLT_DPTR_DT - PNR_CREATE_DT), " +						// 3
                    "DCR_OD_ORIG, " +												// 4
                    "DCR_OD_DEST, " +												// 5
                    "DCR_OD_FLT_DPTR_DT, " +										// 6
                    "LCL_FLW_TYPE_IND, " +											// 7
                    "TRAFFIC_CT, " +												// 8
                    "EVT_OD_FARE, " +												// 9
                    "LEG_ORIG, " +													// 10
                    "LEG_DEST, " +													// 11
                    "FLT_DPTR_DT, " +												// 12
                    "FCST_DPTR_DT, " +												// 13
                    "FCST_DOW, " +													// 14
                    "FLT_ID, " +													// 15
                    "FLT_NBR, " +													// 16
                    "FCST_ID, " +													// 17
                    "OTH_LEG_ORIG, " +												// 18
                    "OTH_LEG_DEST, " +												// 19
                    "OTH_LEG_FCST_ID, " +											// 20
                    "nvl(OTH_LEG_FLW_EBP, -9), " +  					            // 21
                    "OVNT_FLAG, " +													// 22
                    "DCR_CABIN_CD "+												// 23
                    "from " +
                    "DDM_PNR_2_W " +
                    "where " +
                    "LEG_ORIG = ? and " +											// 1 in
                    "LEG_DEST = ? and " +				 							// 2 in
                    "round(EVT_OD_FARE) >= ? " 										// 3 in
            ;

    private static final String INSERT_INTO_BPC_OUTPUT =
            "INSERT INTO BPC_OUTPUT ( "+
                "FLT_ID,"+
                "FLT_DPTR_DT,"+
                "FLT_DPTR_TM,"+
                "LEG_ORIG,"+
                "LEG_DEST,"+
                "CABIN_CD,"+
                "LCL_FLW_TYPE_IND,"+
                "FLT_NBR,"+
                "FCST_ID,"+
                "FCST_DOW,"+
                "FCST_DPTR_DT,"+
                "AU_LEVEL,"+
                "CAP,"+
                "RES_HLD,"+
                "REMAIN_AU,"+
                "BID_PRICE,"+
                "BID_PRICE_INFL,"+
                "BID_PRICE_INFL_BATCH,"+
                "BPC_DEFAULT_IND,"+
                "BPC_ADJ_IND,"+
                "MF_DEFAULT_IND) "+
                "VALUES ("+
                ":FLT_ID,"+
                ":FLT_DPTR_DT,"+
                ":FLT_DPTR_TM,"+
                ":LEG_ORIG,"+
                ":LEG_DEST,"+
                ":CABIN_CD,"+
                ":LCL_FLW_TYPE_IND,"+
                ":FLT_NBR,"+
                ":FCST_ID,"+
                ":FCST_DOW,"+
                ":FCST_DPTR_DT,"+
                ":AU_LEVEL,"+
                ":CAP,"+
                ":RES_HLD,"+
                ":REMAIN_AU,"+
                ":BID_PRICE,"+
                ":BID_PRICE_INFL,"+
                ":BID_PRICE_INFL_BATCH,"+
                ":BPC_DEFAULT_IND,"+
                ":BPC_ADJ_IND,"+
                ":MF_DEFAULT_IND)";
}
