Built dictionary tables for RP, RTU and compressor events. 
- [x] TODO: Does it make sense to combine these into one singular dictionary table or leave them as 3 separate entities?

## MIDSTREAM_STATUS_LOG
Currently have a placeholder table for Midstream statuses. It currently has no data in it. 
create or replace TABLE OPERATIONS.PROD.EVENT_LOG_MIDSTREAM (
    LOG_ID INTEGER,
    WELL_ID INTEGER,
    STATUS_ID INTEGER,
    START_DT TIMESTAMP_NTZ,
    END_DT TIMESTAMP_NTZ,
    STATUS_COMMENT VARCHAR
)

## RP_STATUS_LOG
create or replace TABLE OPERATIONS.PROD.EVENT_LOG_RP (
    LOG_ID INTEGER,
    WELL_ID INTEGER,
    STATUS_ID INTEGER,
    START_DT TIMESTAMP_NTZ,
    END_DT TIMESTAMP_NTZ,
    STATUS_COMMENT VARCHAR
)
AS
SELECT 
concat(rpl.well_number,rp.status_id, datediff(seconds, '2023-01-01'::date, CONVERT_TIMEZONE('UTC', 'America/Chicago', RPL.state_first_timestamp))) as log_id,
RPL.well_number as well_id, 
RP.STATUS_ID,
CONVERT_TIMEZONE('UTC', 'America/Chicago', RPL.state_first_timestamp) as start_dt,
CONVERT_TIMEZONE('UTC', 'America/Chicago', RPL.state_last_timestamp) as end_dt,
NULL AS STATUS_COMMENT

FROM INBOUND_IRONIQ_HISTORY.GALILEO.RP_STATUS_LOG RPL
LEFT JOIN OPERATIONS.PROD.STATUS_DICTIONARY_RP RP ON UPPER(RPL.RP_STATUS_LAST) = RP.STATUS_NAME
WHERE 1=1 
order by start_dt DESC

## RTU_STATUS_LOG
create or replace TABLE OPERATIONS.PROD.EVENT_LOG_RP (
    LOG_ID INTEGER,
    WELL_ID INTEGER,
    STATUS_ID INTEGER,
    START_DT TIMESTAMP_NTZ,
    END_DT TIMESTAMP_NTZ,
    STATUS_COMMENT VARCHAR
)
AS
SELECT 
concat(rpl.well_number,rp.status_id, datediff(seconds, '2023-01-01'::date, CONVERT_TIMEZONE('UTC', 'America/Chicago', RPL.state_first_timestamp))) as log_id,
RPL.well_number as well_id, 
RP.STATUS_ID,
CONVERT_TIMEZONE('UTC', 'America/Chicago', RPL.state_first_timestamp) as start_dt,
CONVERT_TIMEZONE('UTC', 'America/Chicago', RPL.state_last_timestamp) as end_dt,
NULL AS STATUS_COMMENT

FROM INBOUND_IRONIQ_HISTORY.GALILEO.RP_STATUS_LOG RPL
LEFT JOIN OPERATIONS.PROD.STATUS_DICTIONARY_RP RP ON UPPER(RPL.RP_STATUS_LAST) = RP.STATUS_NAME
WHERE 1=1 
order by start_dt DESC

## Comp:
Updated Detechtion mapping table (OPERATIONS.PROD.DETECHTION_SITE_MAP)

- [x] TODO: figure out where comp MC1817 is located. It currently has the VW CPF which doesnt make much sense given that there are no wells on the VW CPF. Waiting on a response from Mase to get this sorted out.  
	This unit is actually a Flash Gas Comp on the VW CPF and is currently getting left off the analysis. 
- [ ] TODO: filter out any other Flash Gas units across the field. 
- [ ] TODO: Need a way to combine pads that have more than one compressor on each site down events into a log that can only have one event per time. On pads with more than one compressor, if both are down at the same time, they have overlapping compressor events. Does it make sense to aggregate comp events into an event table? Basically, have an intermediary table that checks:
		- Does the pad have more than one unit. 
		- If yes, find and compare overlapping compressor events on the same pad. Combine overlapping events into the same event where they overlap. On events on multi-compressor pads that have a down compressor but not every compressor is down, set the is_down col = False, and add in the comments the unit #(s) that went down. 
		- If a pad only has one compressor, treat it as is since one unit going down equates to everything going down. 
		- Just need a way of making sure compressor events do not overlap on the same pad. Combining overlapping events should alleviate this issue. 

- [x] TODO: Need to have a compressor/site to well mapping that only cascades compressor events to wells that are on GL, GAPL, or PAGL on that pad. 
For now, I have the following table created as a way of "cascading events" down to each well:
create or replace table operations.prod.status_well_comp_map(
    well_id NUMBER(38,0),
    site_id NUMBER(38,0)
)
as 
select distinct 
    giv.wellnumber as well_id,
    w.property_number as site_id, 
from OPERATIONS.PROD.GL_INJ_VOL_YEST giv
left join analytics.enterprise.well_header w on giv.wellnumber = w.short_well_header_number
where adjusted_date > current_date - 30
order by site_id, giv.wellnumber

This Table gives us matches for 388 wells out of the 538 wells with inj meters and encompasses all the detechtion units. 

- [ ] TODO: Figure out what datetime col corresponds directly with the event start time. Current options are ORIGINAL_REPORTING_DATE_CST, INSERTED_DATE_CST, REPORTING_DATE_CST for the start_dt of an event and CLOSING_REPORTING_DATE_CST & DATE_CLOSED_CST for the end_dt of an event. 
		For now, I will use REPORTING_DATE_CST for start_dt and DATE_CLOSED_CST for end_dt
	

## Precedence Event Processing Query Documentation

### Overview
This query processes event data from multiple sources (Midstream, RTU, Compressor, and Rod Pump) and applies a hierarchical precedence logic to handle overlapping events. It includes validation, adjustment of overlapping time periods, and proper event sequencing.

### Query Structure

#### 1. Base Events CTE (Common Table Expression)
Purpose: Establishes the foundation by combining events from all sources with priority assignments.

Key Features:
- Combines events from four sources: Midstream, RTU, Compressor, and Rod Pump
- Assigns priority levels (1-4) based on source type
- Performs initial validation of timestamps
- Preserves original timestamps for audit purposes
- Validates for:
    - Invalid time ranges (start_dt >= end_dt)
    - Missing timestamps (NULL values)

Priority Assignment:
1. Midstream (highest priority)
2. RTU (when is_down = TRUE)
3. Compressor
4. Rod Pump (lowest priority)

#### 2. Event Windows CTE
Purpose: Identifies temporal relationships between events using window functions.

Key Features:
- Uses LEAD/LAG window functions to look at adjacent events
- Partitions by well_id to process each well independently
- Orders by priority_level and start_dt
- Captures:
    - next_start: Start time of next event
    - next_priority: Priority of next event
    - prev_end: End time of previous event
    - prev_priority: Priority of previous event
#### 3. Adjustments CTE
Purpose: Determines what adjustments are needed for overlapping events.

Handles Three Cases:
1. Truncated End (Case A):
    - When a lower-priority event starts before and ends during a higher-priority event
    - Action: Truncates the end time to the start of the higher-priority event
2. Adjusted Start (Case B):
    - When a lower-priority event starts during and ends after a higher-priority event
    - Action: Adjusts start time to the end of the higher-priority event
3. Split Event (Case C):
    - When a lower-priority event completely spans a higher-priority event
    - Action: Splits into two events (before and after the higher-priority event)

#### 4. Final Events CTE
Purpose: Generates the final event set with all adjustments applied.

Contains Three Parts:

4. Non-split events:
    - Original events with truncated ends or adjusted starts
    - Events requiring no adjustment
5. Pre-split portions:
    - First part of split events
    - From original start to higher-priority event start
6. Post-split portions:
    - Second part of split events
    - From higher-priority event end to original end

#### 5. Final Selection
Purpose: Returns the final, processed event set.

Output Columns:
- well_id: Identifies the well
- log_id: Unique identifier for the event
- status_id: Event status identifier
- start_dt: Adjusted start timestamp
- end_dt: Adjusted end timestamp
- original_start_dt: Original start timestamp
- original_end_dt: Original end timestamp
- adjustment_flag: Indicates type of adjustment made
- event_source: Origin of the event
- priority_level: Assigned priority

Ordering:
- Primarily by well_id
- Secondary by start_dt (descending)
- Tertiary by priority_level

##### Data Quality Controls
- Excludes zero-length intervals (WHERE start_dt < end_dt)
- Filters out events with validation errors
- Maintains audit trail through original timestamps
- Tracks adjustments through adjustment_flag

##### Important Notes
7. Event priority is determined by source and is_down status
8. Time ranges are adjusted to prevent overlaps while preserving event information
9. Original timestamps are preserved for auditing
10. Each well is processed independently
11. Only valid events (passing timestamp validation) are processed

This query ensures that for any given time period, each well has at most one active event, with higher-priority events taking precedence over lower-priority ones while maintaining data integrity and traceability.

