define;

alter session enable parallel dml;

variable collectDateCharYYYYMMDD char(8)
exec :collectDateCharYYYYMMDD := &1;

col collectDateCharYYYYMMDD format a30;
col collectDate format a30;
col collectDate331 format a30;

set pagesize 1000 autocommit off;

select
    :collectDateCharYYYYMMDD as collectDateCharYYYYMMDD,
    to_date(:collectDateCharYYYYMMDD, 'YYYYMMDD') as collectDate,
    to_date(:collectDateCharYYYYMMDD, 'YYYYMMDD') + 331 as collectDate331
from dual;

exec app_utils.trunc_table('YM_WORK', 'DDM_PNR_1_W');
insert  /*+ append */ into DDM_PNR_1_W
(
    pnr_loc_cd,
    pnr_create_dt,
    dcr_od_flt_dptr_dt,
    dcr_od_orig,
    dcr_od_dest,
    flt_dptr_dt,
    flt_id,
    flt_nbr,
    leg_orig,
    leg_dest,
    aln_cd,
    dcr_cabin_cd,
    srvc_cls,
    flt_dptr_tms,
    cdshr_ind,
    lcl_flw_type_ind,
    sabre_dptr_dttm,
    fcst_dptr_dt,
    fcst_dow,
    fcst_id,
    fcst_cls,
    flw_ebp,
    evt_od_fare,
    traffic_ct,
    insert_dt
)
with
    flownFlights as
        (
            select /*+ parallel (8) */
                pnr_locator_code_s,
                pnr_creation_date_d,
                dcr_od_flt_dptr_date_d,
                dcr_od_orig_s,
                dcr_od_dest_s,
                flt_dptr_date_d,
                case when dcr_cabin_code_c not in ('Y')
                    and (
                                  (service_class_c='R' and evt_od_fare_f < 200) /*R unpaid*/
                                  or rbk_od_avg_fare < 100
                                  or fare_group_subset_cd in  ('0V','54','20','21','22','23','24','26','27','50','0S','78','CSM', 'CAP', 'FRE') /*FREE*/
                                  or fare_group_subset_cd in ('55','56','51','UPG')  /*Upgrades*/
                                  or fare_group_subset_cd in ('0F','20','0G','0W','0X','18','1X','ANY', 'AMS', 'BXA', 'BXM') /*Award*/
                                  or fare_group_subset_cd in ('0S','APASS','CAP')  /*Must ride, airpass*/
                                  or (
                                              fare_group_cd in ('057', 'AWD', '57')
                                          and service_class_c not in ('F','J')
                                      ) /*No any time*/
                                  or ( substr(best_tkt_desgntr_cd,1,2) ='AL' )
                              ) /*upgrades + awards*/
                         then 0 else 1 end as redemption_ind,
                flt_id_i,
                flt_nbr_i,
                leg_orig_s,
                leg_dest_s,
                airline_code_s,
                dcr_cabin_code_c,
                service_class_c,
                flt_dptr_tms,
                codeshare_ind_c,
                lcl_flw_type_ind_c,
                sabre_dptr_datetime_d,
                fcst_cls,
                flow_ebp_f,
                evt_od_fare_f,
                traffic_ct
            from
                fcst_post_dptr_pnr
            where
                    pnr_creation_date_d >=  to_date(:collectDateCharYYYYMMDD, 'YYYYMMDD') - 2*365
              and dcr_od_flt_dptr_date_d >=  to_date(:collectDateCharYYYYMMDD, 'YYYYMMDD') - 365
              and codeshare_ind_c = 'N'
              and airline_code_s = 'AA'
              and service_class_c not in ('T', 'E','C','U','Z','X')
        ),
    futureFlights as
        (
            select /*+ parallel (8) */
                pnr_locator_code_s,
                pnr_creation_date_d,
                dcr_od_flt_dptr_date_d,
                dcr_od_orig_s,
                dcr_od_dest_s,
                flt_dptr_date_d,
                case when dcr_cabin_code_c not in ('Y')
                    and (
                                  (service_class_c='R' and evt_od_fare_f < 200) /*R unpaid*/
                                  or rbk_od_avg_fare < 100
                                  or fare_group_subset_cd in  ('0V','54','20','21','22','23','24','26','27','50','0S','78','CSM', 'CAP', 'FRE') /*FREE*/
                                  or fare_group_subset_cd in ('55','56','51','UPG')  /*Upgrades*/
                                  or fare_group_subset_cd in ('0F','20','0G','0W','0X','18','1X','ANY', 'AMS', 'BXA', 'BXM') /*Award*/
                                  or fare_group_subset_cd in ('0S','APASS','CAP')  /*Must ride, airpass*/
                                  or (
                                              fare_group_cd in ('057', 'AWD', '57')
                                          and service_class_c not in ('F','J')
                                      ) /*No any time*/
                                  or ( substr(best_tkt_desgntr_cd,1,2) ='AL' )
                              ) /*upgrades + awards*/
                         then 0 else 1 end as redemption_ind,
                flt_id_i,
                flt_nbr_i,
                leg_orig_s,
                leg_dest_s,
                airline_code_s,
                dcr_cabin_code_c,
                service_class_c,
                flt_dptr_tms,
                codeshare_ind_c,
                lcl_flw_type_ind_c,
                sabre_dptr_datetime_d,
                fcst_cls,
                flow_ebp_f,
                evt_od_fare_f,
                traffic_ct
            from
                fcst_curr_pnr
            where
                    pnr_creation_date_d >=  to_date(:collectDateCharYYYYMMDD, 'YYYYMMDD') - 2*365
              and trunc (sabre_dptr_datetime_d) > snapshot_date_d + 1
              and codeshare_ind_c = 'N'
              and airline_code_s = 'AA'
              and service_class_c not in ('T', 'E','C','U','Z','X')
        ),
    flightsWithTwoLegsOrLess as
        (
            select /*+ parallel (8) */
                futureFlights.*
            from futureFlights, pnr_flt_key_w
            where
                    futureFlights.pnr_locator_code_s     = pnr_flt_key_w.pnr_loc_cd
              and futureFlights.pnr_creation_date_d    = pnr_flt_key_w.pnr_create_dt
              and futureFlights.dcr_od_flt_dptr_date_d = pnr_flt_key_w.dcr_od_flt_dptr_dt
              and futureFlights.dcr_od_orig_s          = pnr_flt_key_w.dcr_od_orig
              and futureFlights.dcr_od_dest_s          = pnr_flt_key_w.dcr_od_dest
              and futureFlights.redemption_ind         = 1
            union all
            select /*+ parallel (8) */
                flownFlights.*
            from flownFlights, pnr_flt_key_w
            where
                    flownFlights.pnr_locator_code_s     = pnr_flt_key_w.pnr_loc_cd
              and flownFlights.pnr_creation_date_d    = pnr_flt_key_w.pnr_create_dt
              and flownFlights.dcr_od_flt_dptr_date_d = pnr_flt_key_w.dcr_od_flt_dptr_dt
              and flownFlights.dcr_od_orig_s          = pnr_flt_key_w.dcr_od_orig
              and flownFlights.dcr_od_dest_s          = pnr_flt_key_w.dcr_od_dest
              and flownFlights.redemption_ind         = 1
        )
select /*+ parallel (flightsWithTwoLegsOrLess,8) parallel(fcst_id_ref,8) */
    flightsWithTwoLegsOrLess.pnr_locator_code_s,
    flightsWithTwoLegsOrLess.pnr_creation_date_d,
    flightsWithTwoLegsOrLess.dcr_od_flt_dptr_date_d,
    flightsWithTwoLegsOrLess.dcr_od_orig_s,
    flightsWithTwoLegsOrLess.dcr_od_dest_s,
    flightsWithTwoLegsOrLess.flt_dptr_date_d,
    flightsWithTwoLegsOrLess.flt_id_i,
    flightsWithTwoLegsOrLess.flt_nbr_i,
    flightsWithTwoLegsOrLess.leg_orig_s,
    flightsWithTwoLegsOrLess.leg_dest_s,
    flightsWithTwoLegsOrLess.airline_code_s,
    flightsWithTwoLegsOrLess.dcr_cabin_code_c,
    flightsWithTwoLegsOrLess.service_class_c,
    flightsWithTwoLegsOrLess.flt_dptr_tms,
    flightsWithTwoLegsOrLess.codeshare_ind_c,
    flightsWithTwoLegsOrLess.lcl_flw_type_ind_c,
    flightsWithTwoLegsOrLess.sabre_dptr_datetime_d,
    trunc (flightsWithTwoLegsOrLess.flt_dptr_tms - (3 / 24)),
    case
        when to_char (flightsWithTwoLegsOrLess.flt_dptr_tms - (3 / 24), 'D') = 1 then 7
        else to_char (flightsWithTwoLegsOrLess.flt_dptr_tms - (3 / 24), 'D') - 1
        end,
    nvl (fcst_id_ref.fcst_id, 0),
    flightsWithTwoLegsOrLess.fcst_cls,
    flightsWithTwoLegsOrLess.flow_ebp_f,
    flightsWithTwoLegsOrLess.evt_od_fare_f,
    flightsWithTwoLegsOrLess.traffic_ct,
    trunc (sysdate)
from flightsWithTwoLegsOrLess, fcst_id_ref
where
        case
            when to_char (flightsWithTwoLegsOrLess.flt_dptr_tms - (3 / 24), 'D') = 1 then 7
            else to_char (flightsWithTwoLegsOrLess.flt_dptr_tms - (3 / 24), 'D') - 1
            end = fcst_id_ref.dow(+)
  and
        floor (
                    (to_date (to_char (flightsWithTwoLegsOrLess.flt_dptr_tms, 'yyyymmddhhmissam'), 'yyyymmddhhmissam')
                        - trunc(flightsWithTwoLegsOrLess.flt_dptr_tms - (3 / 24))) * 24 * 60
            )
        >= fcst_id_ref.time_band_start(+)
  and
        floor (
                    (to_date (to_char (flightsWithTwoLegsOrLess.flt_dptr_tms, 'yyyymmddhhmissam'), 'yyyymmddhhmissam')
                        - trunc(flightsWithTwoLegsOrLess.flt_dptr_tms - (3 / 24))) * 24 * 60)
        <= fcst_id_ref.time_band_end(+)
  and flightsWithTwoLegsOrLess.leg_orig_s = fcst_id_ref.leg_orig_s(+)
  and flightsWithTwoLegsOrLess.leg_dest_s = fcst_id_ref.leg_dest_s(+)
;

commit;
