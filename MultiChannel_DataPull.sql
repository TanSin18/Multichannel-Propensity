CREATE OR REPLACE TABLE `dw-bq-data-d00.SANDBOX_ANALYTICS.LR_Reactive` as
WITH TRANS AS
(
select
customer_id,
transaction_booked_date,
src_atg_ecom_order_id,
coupon_flag, 
same_day_delivery_line_indicator, 
ecom_order_source_code,
transaction_guid
from `dw-bq-data-p00.ANALYTICAL.sales_datamart_sales_transaction_sum` a
join dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_TXN_XREF b 
on a.transaction_guid=b.trans_id 
and b.customer_id > 0
where concept_format_id = 1  
group by 1,2,3,4,5,6,7
), 
DATE_DIFF1 AS
(       
select 
customer_id,
transaction_guid,
transaction_booked_date,
src_atg_ecom_order_id,
coupon_flag, 
same_day_delivery_line_indicator, 
ecom_order_source_code,
LAG(transaction_booked_date) OVER (PARTITION BY customer_id ORDER BY transaction_booked_date ) as previous_buy,
ABS(date_diff(LAG(transaction_booked_date) OVER (PARTITION BY customer_id ORDER BY transaction_booked_date ),transaction_booked_date,day)) as time_interval,
ABS(date_diff(LAG(transaction_booked_date) OVER (PARTITION BY customer_id ORDER BY transaction_booked_date ),transaction_booked_date,month)) as month_interval
from 
TRANS 
where transaction_booked_date between date_sub(
          current_date(), 
          interval 2 YEAR) 
        and current_date() 
)

select a.*, 
b.transaction_guid as previous_trans_id, 
b.coupon_flag as pre_coupon_flag, 
b.ecom_order_source_code as pre_src_order, 
b.src_atg_ecom_order_id as pre_ecom_order_id
from DATE_DIFF1 a
left join
TRANS b
on 
a.customer_id=b.customer_id
and a.previous_buy=b.transaction_booked_date
where month_interval>=6;

CREATE OR REPLACE TABLE dw-bq-data-d00.SANDBOX_ANALYTICS.Reactive as
WITH 
Ecomm_trans as 
    (
      SELECT 
        last_touch_channel,
        transaction_guid, 
        src_atg_ecom_order_id,
        transaction_booked_date
      FROM 
        `dw-bq-data-p00.ANALYTICAL.sales_datamart_sales_transaction_sum` a
        inner join
        `dw-bq-data-p00.INGRESS_OMNITURE.bed_bath_us_seek_category` b
      on a.src_atg_ecom_order_id=b.orders
        AND concept_format_id = 1
        
    ), 
DM_step1 AS 
  (
   SELECT 
    Coupon_barcode_id,
    IN_HOME_DT
    FROM `dw-bq-data-p00.EDW_MCF_VW.COUPON_EVENT_MASTER`
    WHERE 
    CONCEPT_FORMAT_ID =1 
    AND UPPER(COUPON_EVENT_KEY_DESC) LIKE '%MAIN EVENT%DIRECT MAIL%'
    AND IN_HOME_DT between date_sub(
          current_date(), 
          interval 2 YEAR) 
        and current_date() 
  ),
step2 as 
  (
  select
  A.COUPON_BARCODE_ID1,
  A.COUPON_BARCODE1,
  c.current_customer_id AS customer_id,
  B.IN_HOME_DT
  from
  (select * from
  dw-bq-data-p00.EDW_MCF_VW.CAMPAIGN_PROMO_HIST_DM
  where COUPON_BARCODE_ID1 in (select COUPON_BARCODE_ID from DM_step1)) A
  inner join
  DM_step1 B
  ON A.COUPON_BARCODE_ID1 = B.COUPON_BARCODE_ID
  JOIN
  (SELECT
  DISTINCT CUST_HHLD_ADDR_SKID,
  CURRENT_ADDRESS_ID,
  CURRENT_CUSTOMER_ID
  FROM dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_SKID
  ) C
  on C.CUST_HHLD_ADDR_SKID=a.CUST_HHLD_ADDR_SKID
  WHERE
  A.concept_id = 1
  and DLVRY_STATUS_CD is not null
  AND A.COUPON_BARCODE_ID1> 0
  AND B.COUPON_BARCODE_ID> 0
  AND C.CURRENT_CUSTOMER_ID> 0
  ),
  ecomm as 
  (  
    SELECT 
    a.*,
    g.FISCAL_PERIOD_ID,
    z.FISCAL_PERIOD_ID as PRE_FISCAL_PERIOD_ID,
    b.last_touch_channel,
    Case when d.COUPON_BARCODE1 is not null then 1 else 0 end as DM_Flag
    from 
    `dw-bq-data-d00.SANDBOX_ANALYTICS.LR_Reactive` a
    left join
    Ecomm_trans b
    on a.transaction_guid=b.transaction_guid
    and a.transaction_booked_date=b.transaction_booked_date
    and a.src_atg_ecom_order_id=b.src_atg_ecom_order_id
    left join 
    step2 d
    on a.customer_id=d.customer_id
    and a.transaction_booked_date between IN_HOME_DT and date_add(
    IN_HOME_DT, 
    interval 14 DAY) 
    left join
    `dw-bq-data-p00.DW_MCF_VW.DWPTIMDIM` g
    ON a.transaction_booked_date=g.GREGORIAN_CALENDAR_DATE
    left join
    `dw-bq-data-p00.DW_MCF_VW.DWPTIMDIM` z
    ON a.previous_buy=z.GREGORIAN_CALENDAR_DATE
    where a.customer_id is not null
    
  )
    select 
    a.*,
    Q.address_id,
     CASE
         WHEN last_touch_channel IN ( 'Social Marketing', 'Social Networks' )
       THEN
         'Social'
         WHEN last_touch_channel IS NULL
              AND dm_flag = 1 THEN 'DM'
         WHEN last_touch_channel IN ( 'Paid Search','None', 'Natural Search', 'QR',
                                        'CSE',
                                        'Retargeting', 'Internal', 'Display',
                                        'Direct',
                                        'Affiliate', 'Referring Domains' )
              AND dm_flag = 1 THEN 'DM'
         WHEN last_touch_channel IN ( 'Paid Search','None', 'Natural Search', 'QR',
                                        'CSE',
                                        'Retargeting', 'Internal', 'Display',
                                        'Direct',
                                        'Affiliate', 'Referring Domains' )
              AND dm_flag = 0 THEN 'Others'
         WHEN last_touch_channel IS NULL
              AND dm_flag = 0
             THEN 'UNKNOWN'
         ELSE last_touch_channel
       END AS reactivation_channel
FROM   ecomm a
LEFT JOIN 
    (SELECT customer_id, address_id FROM
    dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_CURR
    WHERE customer_purge_ind = 'N'
    AND customer_id > 0
    AND address_id > 0) Q ON a.customer_id = Q.customer_id
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
ORDER  BY customer_id,dm_flag DESC; 

create or replace table `dw-bq-data-d00.SANDBOX_ANALYTICS.multichannel_cust_attributes` as
select base.*,
case when TRIM(UPPER(TERRITORY_CD)) in ('IA','IL','IN','KS','MI','MN','MO','ND','NE','OH','SD','WI') then "Midwest"
      when TRIM(UPPER(TERRITORY_CD)) in ('ME','NH','NJ','NY','PA','RI','VT') then "Northeast"      
      when TRIM(UPPER(TERRITORY_CD)) in ('AL','AR','DC','DE','FL','GA','KY','LA','MD','MS','NC','OK','SC','TN','TX','VA','WV') then "South" 
      when TRIM(UPPER(TERRITORY_CD)) in ('AK','AZ','CA','CO','HI','ID','MT','NM','NV','OR','UT','WA','WY') then "West" 
      when TRIM(TERRITORY_CD) is null then "UNKNOWN"
      else "UNKNOWN"
      end as State_Division, 
case when A8688_GENDER_INP is null then 'UNKNOWN' else A8688_GENDER_INP end as gender,
 case when EXTRACT(year FROM cast(previous_buy as date)) - DOB_YR <17 then  'AGE_LESS_17'
when EXTRACT(year FROM cast(previous_buy as date)) - DOB_YR >=18 and EXTRACT(year FROM cast(previous_buy as date)) - DOB_YR <=24 then 'AGE_18_24' 
when EXTRACT(year FROM cast(previous_buy as date)) - DOB_YR >=25 and EXTRACT(year FROM cast(previous_buy as date)) - DOB_YR <=40 then 'AGE_25_40' 
when EXTRACT(year FROM cast(previous_buy as date)) - DOB_YR >=41 and EXTRACT(year FROM cast(previous_buy as date)) - DOB_YR <=54 then 'AGE_41_54' 
when EXTRACT(year FROM cast(previous_buy as date)) - DOB_YR >=55 then 'AGE_ABOVE_55' 
else 'UKNOWN' end as age_bucket,
case when ethnicity is null then 'UNKNOWN' else ethnicity end as ethnicity ,
case when INC_ESTMT_HHLD_DESC is null then 'UNKNOWN' else INC_ESTMT_HHLD_DESC end as INCOME,
  f.beyond_recency_two_years AS BBB_R_2Y,
  j.A_A2779N_PROPEN_SCR_INTNT_01_03,
  j.A_A2779N_PROPEN_SCR_INTNT_04_07,
  j.A_A2779N_PROPEN_SCR_INTNT_08_10,
  j.A_A7467N_RECNT_HOME_BUYER_YES,
  j.A_A7478N_NEW_MOVER_YES,
  j.A_A7779N_CHILD_INTRST_YES,
  j.A_A7830N_HM_IMPROVE_GRP_YES,
  j.A_A7851N_HM_IMPROVE_DIY_YES,
  j.A_A8271N_HM_LIVING_IND_YES,
  j.A_A8588N_HM_SQR_FT,
  j.A_A8589N_HM_LOT_SQR_FT,
  j.A_A8592N_HM_YR_BUILT,
  j.A_A8606N_HM_OWN_RENT_HOME_OWNER,
  j.A_A8606N_HM_OWNER_RENTER_RENTER,
  j.A_A8607N_HM_LNTH_RES_01_02,
  j.A_A8607N_HM_LNTH_RES_03_05,
  j.A_A8607N_HM_LNTH_RES_06_10,
  j.A_A8607N_HM_LNTH_RES_11_14,
  j.A_A8609N_MARITAL_STAT_HH_M_A,
  j.A_A8609N_MARITAL_STAT_HH_S_B,
  j.A_A8641_INC_ESTMT_HHLD_1,
  j.A_A8641_INC_ESTMT_HHLD_2,
  j.A_A8641_INC_ESTMT_HHLD_3,
  j.A_A8641_INC_ESTMT_HHLD_4,
  j.A_A8641_INC_ESTMT_HHLD_5,
  j.A_A8641_INC_ESTMT_HHLD_6,
  j.A_A8641_INC_ESTMT_HHLD_7,
  j.A_A8641_INC_ESTMT_HHLD_8,
  j.A_A8641_INC_ESTMT_HHLD_9,
  j.A_A8642_HM_MKT_VAL_LT_75,
  j.A_A8642_HM_MKT_VAL_75_149,
  j.A_A8642_HM_MKT_VAL_150_299,
  j.A_A8642_HM_MKT_VAL_300_499,
  j.A_A8642_HM_MKT_VAL_500_999,
  j.A_A8642_HM_MKT_VAL_1M_PLUS,
  j.A_A9153N_RPC_FURNITURE_BUYR_YES,
  j.A_A9153N_RPC_HM_IMPROVE_YES,
  j.A_A9509N_EDU_1ST_1,
  j.A_A9509N_EDU_1ST_2,
  j.A_A9509N_EDU_1ST_3,
  j.A_A9509N_EDU_1ST_4,
  j.A_A3101N_RACE_AFRAM,
  j.A_A3101N_RACE_ASIAN,
  j.A_A3101N_RACE_HISP,
  j.A_A3101N_RACE_WHITE,
  j.A_A9350N_ECONOMIC_STB_01_10,
  j.A_AAP000447N_ASET_PRPN_DIS_INC,
  f.beyond_instore_frequency AS BBB_INSTORE_F,
  f.beyond_instore_frequency_two_years AS BBB_INSTORE_F_2Y,
  f.beyond_oncoupon_recency_decile_two_years AS BBB_ONCOUPON_R_DECILE_2Y,
  g.BUYS_Q_01,
  g.BUYS_Q_02,
  g.BUYS_M_01,
  g.BUYS_M_06,
  g.BUYS_Q_04,
  g.BUYS_Q_08,
  total_coupon_price_reduction_amount AS COUPON_ANY_AMT,
  g.COUPON_Q_01,
  g.COUPON_Q_04,
  g.COUPON_SALES_Q_05,
  g.COUPON_SALES_Q_08,
  b.sales_in_last_12_months_at_HARMON_concept AS HARMON_SALES_L12M,
  h.number_of_periods_purchased AS NUM_PERIODS,
  h.number_of_quarters_purchased AS NUM_QUARTERS,
  h.number_of_items_purchased AS NUM_TOTAL_ITEMS,
  h.number_of_transactions_involving_sales AS NUM_TXNS,
  h.BBBY_CLUSTER_NUMBER,
  d.PH_CFREQ90D,
  d.PH_CREDEEM365D,
  d.PH_CREDEEM90D,
  d.PH_CSTACK_90D,
  d.PH_MREDEEM182D_PERC,
  d.PH_MREDEEM730D_PERC,
  d.PH_NMFREQ182D,
  d.PH_PREDEEM365D,
  d.PH_PREDEEM548D,
  d.PH_PREDEEM730D,
  h.number_of_days_since_last_purchase AS RECENCY,
  b.total_sales_in_last_12_months AS TOTAL_SALES_L12M,
  b.total_transactions_in_last_12_months AS TOTAL_TXNS_L12M,
  d.PH_PSTACK_182D,
  d.PH_PFREQ182D,
  d.PH_PSTACK_90D,
  d.PH_PFREQ90D,
  d.PH_PSTACK_365D,
  d.PH_MSTACK_182D,
  d.PH_STACK_182D,
  d.PH_PFREQ365D,
  d.PH_MSTACK_90D,
  d.PH_MFREQ182D,
  d.PH_STACK_90D,
  d.PH_MFREQ90D,
  d.PH_FREQ182D,
  d.PH_FREQ90D,
  d.PH_DM_RECENCY,
  f.beyond_monetary_decile_two_years AS BBB_M_DECILE_2Y,
  f.beyond_monetary_decile AS BBB_M_DECILE,
  f.beyond_oncoupon_monetary_decile AS BBB_ONCOUPON_M_DECILE,
  f.beyond_oncoupon_monetary_decile_two_years AS BBB_ONCOUPON_M_DECILE_2Y,
  f.beyond_instore_monetary_decile_two_years AS BBB_INSTORE_M_DECILE_2Y,
  f.beyond_instore_monetary_decile AS BBB_INSTORE_M_DECILE,
  f.beyond_recency_frequency_monetary_decile_two_years AS BBB_RFM_DECILE_2Y,
  f.beyond_offcoupon_frequency_decile_two_years AS BBB_OFFCOUPON_F_DECILE_2Y,
  f.beyond_recency_frequency_monetary_decile AS BBB_RFM_DECILE,
  f.beyond_offcoupon_recency_frequency_monetary_decile_two_years AS BBB_OFFCOUPON_RFM_DECILE_2Y,
  f.beyond_offcoupon_monetary_decile_two_years AS BBB_OFFCOUPON_M_DECILE_2Y,
  f.beyond_oncoupon_recency_frequency_monetary_decile_two_years AS BBB_ONCOUPON_RFM_DECILE_2Y,
  f.beyond_oncoupon_recency_frequency_monetary_decile AS BBB_ONCOUPON_RFM_DECILE,
  f.beyond_frequecy_decile_two_years AS BBB_F_DECILE_2Y,
  f.beyond_instore_recency_frequency_monetary_decile_two_years AS BBB_INSTORE_RFM_DECILE_2Y,
  f.beyond_frequency_decile AS BBB_F_DECILE,
  f.beyond_instore_recency_frequency_monetary_decile AS BBB_INSTORE_RFM_DECILE,
  f.beyond_monetary_two_years AS BBB_M_2Y,
  avg_net_sales_per_transaction AS AVG_NET_SALES_PER_TXN,
  avg_sales_per_transaction AS AVG_SALES_PER_TXN,
  number_of_departments_purchased AS NUM_DEPARTMENTS,
  number_of_item_categories_purchased AS NUM_ITEM_CATEGORIES,
  pct_sales_on_transaction_pct_off_coupons AS PCT_SALES_ON_TXN_PCT_COUPON,
  pct_sales_on_transaction_USD_off_coupons AS PCT_SALES_ON_TXN_DOL_COUPON,
  pct_transaction_USD_off_coupon_depth AS PCT_TXN_DOL_COUPON_DEPTH,
  pct_items_sold_on_item_pct_off_coupons AS PCT_ITEMS_ON_ITEM_PCT_COUPON,
  avg_unique_items_per_transaction AS AVG_UNIQUE_ITEMS_PER_TXN,
  number_of_sub_departments_purchased AS NUM_SUB_DEPARTMENT,
  avg_items_per_transaction AS AVG_TOTAL_ITEMS_PER_TXN,
  pct_transactions_on_item_pct_off_coupons AS PCT_TXNS_ON_ITEM_PCT_COUPON,
  pct_transactions_on_transaction_USD_off_coupons AS PCT_TXNS_ON_TXN_DOL_COUPON,
  pct_coupon_depth_for_item_pct_off_coupons AS PCT_ITEM_PCT_COUPON_DEPTH,
  pct_sales_on_item_pct_off_coupons AS PCT_SALES_ON_ITEM_PCT_COUPON,
  pct_transactions_on_transaction_pct_off_coupons AS PCT_TXNS_ON_TXN_PCT_COUPON,
  pct_items_sold_on_transaction_pct_off_coupons AS PCT_ITEMS_ON_TXN_PCT_COUPON,
  pct_transaction_pct_off_coupon_depth AS PCT_TXN_PCT_COUPON_DEPTH,
  h.number_of_channels_purchased AS NUM_CHANNELS,
  h.number_of_classes_purchased AS NUM_CLASSES,
  h.number_of_merchandise_divisions_purchased AS NUM_MERCH_DIVISIONS,
  h.number_of_product_groups_purchased AS NUM_PRODUCT_GROUPS
 
from `dw-bq-data-d00.SANDBOX_ANALYTICS.Reactive` base
left join (SELECT customer_id, cast(DOB_YR_INDIV_1 as integer) as DOB_YR 
FROM `dw-bq-data-d00.EDW_MCF_VW.CDG_INFO` WHERE DOB_YR_INDIV_1 IS NOT NULL) age_var
on base.customer_id = age_var.customer_id
left join (select distinct customer_guid, address_guid, A8688_GENDER_INP from `dw-bq-data-p00.ANALYTICAL.customer_analytics_residence_demographics_individual`) gender
on base.customer_id =gender.customer_guid
and base.address_id =gender.address_guid
left join (select customer_guid, address_guid, A3101N_RACE_WHITE, A3101N_RACE_HISP, A3101N_RACE_AFRAM, A3101N_RACE_ASIAN,
case when A3101N_RACE_WHITE = 1 then 'WHITE'
when A3101N_RACE_HISP = 1 then 'HISP'
when A3101N_RACE_AFRAM = 1 then 'AFRAM'
when A3101N_RACE_ASIAN = 1 then 'ASIAN' else 'UNKNOWN' end as ethnicity from `dw-bq-data-p00.ANALYTICAL.customer_analytics_residence_demographics_individual`) ethnicity_data -- not consider more than one race
on base.customer_id =ethnicity_data.customer_guid
and base.address_id =ethnicity_data.address_guid
left join  (SELECT distinct customer_id, a.INC_ESTMT_HHLD_CD,INC_ESTMT_HHLD_DESC
 from 
 ( (select distinct customer_id, INC_ESTMT_HHLD_CD
  FROM `dw-bq-data-p00.EDW_MCF_VW.CDG_INFO`) a
  join `dw-bq-data-p00.EDW_MCF_VW.DEM_INC_ESTMT_HHLD` b
  on a.INC_ESTMT_HHLD_CD = b.INC_ESTMT_HHLD_CD
  )) income
on base.customer_id =income.customer_id
LEFT JOIN
  `dw-bq-data-p00.ANALYTICAL.model_factory_address_contact_strategy_history` b
ON
  base.address_id = b.address_guid
  AND base.PRE_FISCAL_PERIOD_ID = b.fiscal_period_id
LEFT JOIN
  `dw-bq-data-p00.ANALYTICAL.model_factory_address_merfm` c
ON
  base.address_id = c.address_guid
  AND base.PRE_FISCAL_PERIOD_ID = c.fiscal_period_id
LEFT JOIN
  `dw-bq-data-p00.ANALYTICAL.model_factory_address_promo_history_variables` d
ON
  base.address_id = d.address_guid
  AND base.PRE_FISCAL_PERIOD_ID = d.fiscal_period_id
LEFT JOIN
  `dw-bq-data-p00.ANALYTICAL.model_factory_address_zipsummary_history` e
ON
  base.address_id = e.address_guid
  AND base.FISCAL_PERIOD_ID = e.fiscal_period_id
LEFT JOIN
  `dw-bq-data-p00.ANALYTICAL.model_factory_address_rfm` f
ON
  base.address_id = f.address_guid
  AND base.PRE_FISCAL_PERIOD_ID = f.fiscal_period_id
LEFT JOIN
  `dw-bq-data-p00.ANALYTICAL.model_factory_address_seasonality` g
ON
  base.address_id = g.address_guid
  AND base.PRE_FISCAL_PERIOD_ID = g.fiscal_period_id
LEFT JOIN
  `dw-bq-data-p00.ANALYTICAL.model_factory_shopping_metrics_history` h
ON
  base.address_id = h.address_guid
  AND base.PRE_FISCAL_PERIOD_ID = h.fiscal_period_id
LEFT JOIN
  `dw-bq-data-p00.ANALYTICAL.model_factory_addr_prodaff` i
ON
  base.address_id = i.address_guid
  AND base.PRE_FISCAL_PERIOD_ID = i.fiscal_period_id
LEFT JOIN
  `dw-bq-data-p00.ANALYTICAL.model_factory_address_demographics_history` j
ON
  base.address_id = j.address_guid
  AND base.FISCAL_PERIOD_ID = j.fiscal_period_id
   /* BCG Personas */
LEFT JOIN 
     (
        SELECT 
        s.customer_id,
        s.scoring_year_and_period,
        t.Persona
        FROM
        (SELECT
        scoring_year_and_period,
        customer_id,
        Persona 
        FROM
        `dw-bq-data-p00.ANALYTICAL.bcg_history`
        ) t
        INNER JOIN 
        (
          SELECT
          customer_id,
          max(scoring_year_and_period) as scoring_year_and_period
        FROM
        `dw-bq-data-p00.ANALYTICAL.bcg_history`
        group by 1) s
        on s.customer_id=t.customer_id and s.scoring_year_and_period=t.scoring_year_and_period
    ) Z
    on base.CUSTOMER_ID=Z.customer_id 
     LEFT JOIN 
    (
        SELECT CUSTOMER_ID,TERRITORY_CD 
        FROM 
        `dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_ADDRESS` a
        LEFT JOIN
        `dw-bq-data-p00.EDW_MCF_VW.ADDRESS_MSTR` b
        on a.ADDRESS_ID=b.ADDRESS_ID
        LEFT JOIN
        `dw-bq-data-p00.EDW_MCF_VW.MAILING_ADDRESS` c
        on b.MAILING_ADDRESS_ID=c.MAILING_ADDRESS_ID
        where  PREFD_CUST_IND = 'Y'
    ) U
    on base.CUSTOMER_ID=U.CUSTOMER_ID
    ;
CREATE OR REPLACE TABLE dw-bq-data-d00.SANDBOX_ANALYTICS.ME_CUST as
select customer_id,address_id
from
(
select
customer_id,address_id,
Case when Check_ADDRESS_ID is null then 0 else 1 end as ME_LINK
from
(
select a.customer_id,C.address_id,z.ADDRESS_ID as Check_ADDRESS_ID,a.previous_buy,z.SEGMENT_ID from
(select customer_id,previous_buy from 
`dw-bq-data-d00.SANDBOX_ANALYTICS.multichannel_cust_attributes` ) a
left join
(SELECT
  ADDRESS_ID,
  CUSTOMER_ID
  FROM `dw-bq-data-p00.EDW_MCF_VW.CUSTOMER_CURR`
  ) C
on a.CUSTOMER_ID=C.CUSTOMER_ID
left join
(
select ADDRESS_ID,SEGMENT_ID,scoring_year_and_period from
`dw-bq-data-p00.ANALYTICAL.ma_addr_segments` ) z 
on C.ADDRESS_ID=z.ADDRESS_ID
) a
) b
where ME_LINK=1
group by 1,2;

CREATE OR REPLACE TABLE dw-bq-data-d00.SANDBOX_ANALYTICS.ME_CUST_POP as
select * from 
`dw-bq-data-d00.SANDBOX_ANALYTICS.Multichannel_MasterTable`
where customer_id in (select customer_id from dw-bq-data-d00.SANDBOX_ANALYTICS.ME_CUST)

