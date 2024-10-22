
- [ ] WELL_EVENT_TRACKER should be updated with a procedure/task
- [ ] GL_DATA_W_ROW_NUMBER

I am having an issue with the row_number window functions in these dynamic tables using a ton of compute in the snowflake warehouse and constraining the system. Can you look at the dynamic table logic, and provide an alternative solution for the row_number window function issue and dependent changes that will need to be made in the child tables to ensure the logic works as intended?

Here is one of the table paths:
1. RP_WELL_STATE_DATA
2. RP_WELL_STATE_DATA_W_ROW_NUM_V2
3. RP_EVENT_TRACKER
4. RP_DOWN_EVENTS_V2


GL_DATA
GL_DATA_W_ROW_NUMBER
CURRENT_INJECTION_RATES_SITES, , GAS_LIFT_START_DATES
GL_PADS_CURRENTLY_UNDERPERFORMING, CURRENT_LOW_INJECTION_WELLS




Table Definitions:
RP_WELL_STATE_DATA
create or replace dynamic table OPERATIONS.PROD.RP_WELL_STATE_DATA(
	NAME,
	PIQ,
	CONTAINERNAME,
	DATE,
	VALUE,
	WELLNUMBER,
	SITENUMBER,
	TYPE
) target_lag = 'DOWNSTREAM' refresh_mode = AUTO initialize = ON_CREATE warehouse = OPERATIONS_WH
 as
    SELECT
      NAME,
      PIQ,
      CONTAINERNAME,
      date,
      VALUE,
      WELLNUMBER,
      SITENUMBER,
      TYPE,
    FROM
      RAW.IRONIQ.SCADA
    WHERE
      (piq = 'STAT.RUN.RT') AND NAME ILIKE '%Well State%'
      AND date IS NOT NULL
      AND value IS NOT NULL
      AND wellnumber IS NOT NULL;

RP_WELL_STATE_DATA_W_ROW_NUM_V2
create or replace dynamic table OPERATIONS.PROD.RP_WELL_STATE_DATA_W_ROW_NUM_V2(
	NAME,
	PIQ,
	CONTAINERNAME,
	DATE,
	VALUE,
	WELLNUMBER,
	SITENUMBER,
	TYPE,
	RN,
	PREV_VALUE
) target_lag = 'DOWNSTREAM' refresh_mode = AUTO initialize = ON_CREATE warehouse = OPERATIONS_WH
 as
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY WELLNUMBER ORDER BY DATE DESC) AS RN,
    LAG(VALUE) OVER (PARTITION BY WELLNUMBER ORDER BY DATE) AS PREV_VALUE
FROM RP_WELL_STATE_DATA;


RP_EVENT_TRACKER
create or replace dynamic table OPERATIONS.PROD.RP_EVENT_TRACKER(
	WELLNUMBER,
	VALUE,
	START_DATE,
	END_DATE,
	EVENT_TYPE
) target_lag = 'DOWNSTREAM' refresh_mode = AUTO initialize = ON_CREATE warehouse = OPERATIONS_WH
 as
WITH events AS (
    SELECT
        WELLNUMBER,
        VALUE,
        DATE,
        CASE
            WHEN VALUE NOT ILIKE '%Pumping Normal State%'
            AND VALUE NOT ILIKE '%Reserved%'
            AND VALUE NOT ILIKE '%Downtime Pumpoff Setpoint State%'
            AND VALUE NOT ILIKE '%Downtime Secondary Pumpoff Setpoint State%'
            AND VALUE NOT ILIKE '%Downtime Secondary Pumpoff%'
            THEN 'Down'
            ELSE 'Normal'
        END AS EVENT_TYPE,
        SUM(CASE WHEN VALUE != PREV_VALUE THEN 1 ELSE 0 END) OVER (PARTITION BY WELLNUMBER ORDER BY DATE) AS EVENT_ID
    FROM
        RP_WELL_STATE_DATA_W_ROW_NUM_V2
),
event_rows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY WELLNUMBER, EVENT_ID ORDER BY DATE) AS RN
    FROM
        events
),
grouped_events AS (
    SELECT
        WELLNUMBER,
        EVENT_ID,
        FIRST_VALUE(DATE) OVER (PARTITION BY WELLNUMBER, EVENT_ID ORDER BY DATE) AS START_DATE,
        VALUE,
        EVENT_TYPE
    FROM
        event_rows
    WHERE
        RN = 1
)
SELECT
    WELLNUMBER,
    VALUE,
    START_DATE,
    LEAD(START_DATE) OVER (PARTITION BY WELLNUMBER ORDER BY START_DATE) AS END_DATE,
    EVENT_TYPE
FROM
    grouped_events

    ;



RP_DOWN_EVENTS_V2
create or replace dynamic table OPERATIONS.PROD.RP_DOWN_EVENTS_V2(
	AREA,
	ROUTE,
	SITE_NAME,
	SITE_ID,
	WELL_NAME,
	WELLNUMBER,
	RP_VALUE,
	START_DATE,
	OIL_TARGET,
	RELIABILITY_PCT,
	EXCLUDE_FLAG
) target_lag = 'DOWNSTREAM' refresh_mode = AUTO initialize = ON_CREATE warehouse = OPERATIONS_WH
 as
select 
      w.area,
      s.route,
      s.site_name,
      S.SITE_ID,
      w.well_name,
      c.WELLNUMBER,
      c.value AS rp_value,
      C.START_DATE AS start_date,
      lot.OIL_TARGET,
      wr.reliability_pct,
      lds.exclude_flag
FROM RP_EVENT_TRACKER C
    -- LEFT JOIN rp_start_date sd ON c.WELLNUMBER = sd.WELLNUMBER AND sd.rn = 1
    LEFT JOIN ANALYTICS.ENTERPRISE.WELL w ON w.well_id = c.WELLNUMBER
    LEFT JOIN ANALYTICS.ENTERPRISE.SITE s ON s.site_id = w.site_id
    LEFT JOIN latest_oil_targets lot ON lot.well_id = c.WELLNUMBER
    LEFT JOIN well_reliability wr ON wr.well_id = c.WELLNUMBER
    LEFT JOIN latest_deferment_status lds ON lds.well_id = c.WELLNUMBER
    WHERE
      COALESCE(lds.exclude_flag, 0) = 0
      AND lot.OIL_TARGET > 1
      AND C.START_date > CURRENT_DATE -30
AND C.END_DATE IS NULL
AND C.EVENT_TYPE = 'Down'

;