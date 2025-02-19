Built dictionary tables for RP, RTU and compressor events. 
- [ ] TODO: Does it make sense to combine these into one singular dictionary table or leave them as 3 separate entities?

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

WITH 
-- Step 1: Base events with explicit priority levels and validation
base_events AS (
    SELECT 
        well_id,
        log_id,
        status_id,
        start_dt,
        end_dt,
        start_dt AS original_start_dt,
        end_dt AS original_end_dt,
        1 AS priority_level,
        'midstream' AS event_source,
        CASE 
            WHEN start_dt >= end_dt THEN 'Invalid time range'
            WHEN start_dt IS NULL OR end_dt IS NULL THEN 'Missing timestamps'
            ELSE NULL 
        END AS validation_error
    FROM event_log_midstream
    
    UNION ALL
    
    SELECT 
        rtu.well_id,
        rtu.log_id,
        rtu.status_id,
        rtu.start_dt,
        rtu.end_dt,
        rtu.start_dt,
        rtu.end_dt,
        2 AS priority_level,
        'rtu' AS event_source,
        CASE 
            WHEN rtu.start_dt >= rtu.end_dt THEN 'Invalid time range'
            WHEN rtu.start_dt IS NULL OR rtu.end_dt IS NULL THEN 'Missing timestamps'
            ELSE NULL 
        END
    FROM event_log_rtu rtu
    JOIN STATUS_DICTIONARY_RTU dict
        ON rtu.status_id = dict.status_id
    WHERE dict.is_down = TRUE
    
    UNION ALL
    
    SELECT 
        comp.well_id,
        comp.log_id,
        comp.status_id,
        comp.start_dt,
        comp.end_dt,
        comp.start_dt,
        comp.end_dt,
        3 AS priority_level,
        'comp' AS event_source,
        CASE 
            WHEN comp.start_dt >= comp.end_dt THEN 'Invalid time range'
            WHEN comp.start_dt IS NULL OR comp.end_dt IS NULL THEN 'Missing timestamps'
            ELSE NULL 
        END
    FROM event_log_comp comp
    JOIN STATUS_DICTIONARY_COMP dict
        ON comp.status_id = dict.status_id
    
    UNION ALL
    
    SELECT 
        rp.well_id,
        rp.log_id,
        rp.status_id,
        rp.start_dt,
        rp.end_dt,
        rp.start_dt,
        rp.end_dt,
        4 AS priority_level,
        'rp' AS event_source,
        CASE 
            WHEN rp.start_dt >= rp.end_dt THEN 'Invalid time range'
            WHEN rp.start_dt IS NULL OR rp.end_dt IS NULL THEN 'Missing timestamps'
            ELSE NULL 
        END
    FROM event_log_rp rp
    JOIN STATUS_DICTIONARY_RP dict
        ON rp.status_id = dict.status_id
),

-- Step 2: Filter out invalid events and identify overlaps using window functions
event_windows AS (
    SELECT 
        *,
        LEAD(start_dt) OVER (
            PARTITION BY well_id 
            ORDER BY priority_level, start_dt
        ) AS next_start,
        LEAD(priority_level) OVER (
            PARTITION BY well_id 
            ORDER BY priority_level, start_dt
        ) AS next_priority,
        LAG(end_dt) OVER (
            PARTITION BY well_id 
            ORDER BY priority_level, start_dt
        ) AS prev_end,
        LAG(priority_level) OVER (
            PARTITION BY well_id 
            ORDER BY priority_level, start_dt
        ) AS prev_priority
    FROM base_events
    WHERE validation_error IS NULL
),

-- Step 3: Determine required adjustments
adjustments AS (
    SELECT 
        e.*,
        CASE 
            -- Case A: Current event needs end truncation
            WHEN e.end_dt > e.next_start 
                 AND e.start_dt < e.next_start 
                 AND e.priority_level > e.next_priority 
            THEN 'truncated_end'
            
            -- Case B: Current event needs start adjustment
            WHEN e.start_dt < prev_end 
                 AND e.end_dt > prev_end 
                 AND e.priority_level > prev_priority 
            THEN 'adjusted_start'
            
            -- Case C: Current event needs splitting
            WHEN e.start_dt < e.next_start 
                 AND e.end_dt > e.next_start 
                 AND e.priority_level > e.next_priority 
                 AND e.end_dt > 
                    LEAD(end_dt) OVER (
                        PARTITION BY well_id 
                        ORDER BY priority_level, start_dt
                    )
            THEN 'split_event'
            
            ELSE NULL
        END AS adjustment_type,
        next_start AS higher_start,
        LEAD(end_dt) OVER (
            PARTITION BY well_id 
            ORDER BY priority_level, start_dt
        ) AS higher_end
    FROM event_windows e
),

-- Step 4: Generate final events including splits
final_events AS (
    -- Non-split events
    SELECT 
        well_id,
        log_id,
        status_id,
        CASE 
            WHEN adjustment_type = 'adjusted_start' THEN higher_end
            ELSE start_dt
        END AS start_dt,
        CASE 
            WHEN adjustment_type = 'truncated_end' THEN higher_start
            ELSE end_dt
        END AS end_dt,
        original_start_dt,
        original_end_dt,
        adjustment_type AS adjustment_flag,
        event_source,
        priority_level
    FROM adjustments
    WHERE adjustment_type IS NULL 
       OR adjustment_type IN ('truncated_end', 'adjusted_start')
    
    UNION ALL
    
    -- Pre-split portions
    SELECT 
        well_id,
        log_id,
        status_id,
        start_dt,
        higher_start AS end_dt,
        original_start_dt,
        original_end_dt,
        'split_event_pre' AS adjustment_flag,
        event_source,
        priority_level
    FROM adjustments
    WHERE adjustment_type = 'split_event'
    
    UNION ALL
    
    -- Post-split portions
    SELECT 
        well_id,
        log_id,
        status_id,
        higher_end AS start_dt,
        end_dt,
        original_start_dt,
        original_end_dt,
        'split_event_post' AS adjustment_flag,
        event_source,
        priority_level
    FROM adjustments
    WHERE adjustment_type = 'split_event'
)

-- Final selection with validation and ordering
SELECT 
    well_id,
    log_id,
    status_id,
    start_dt,
    end_dt,
    original_start_dt,
    original_end_dt,
    adjustment_flag,
    event_source,
    priority_level
FROM final_events
WHERE 1=1
and start_dt < end_dt  -- Exclude zero-length intervals
ORDER BY 
    well_id,
    start_dt desc,
    priority_level;

If we want these broken out by dates, the following query added after step 4 should work:
-- Generate date series using recursive CTE
date_series AS (
    SELECT 
        well_id,
        log_id,
        status_id,
        start_dt,
        end_dt,
        original_start_dt,
        original_end_dt,
        adjustment_flag,
        event_source,
        priority_level,
        is_down,
        DATE_TRUNC('day', start_dt) as day_date
    FROM final_events
    
    UNION ALL
    
    SELECT 
        well_id,
        log_id,
        status_id,
        start_dt,
        end_dt,
        original_start_dt,
        original_end_dt,
        adjustment_flag,
        event_source,
        priority_level,
        is_down,
        DATEADD(day, 1, day_date)
    FROM date_series
    WHERE DATEADD(day, 1, day_date) <= DATE_TRUNC('day', end_dt)
),

-- Split events at midnight
date_split_events AS (
    SELECT 
        well_id,
        log_id,
        status_id,
        -- For start time, use original if it's the first day, otherwise use midnight
        CASE 
            WHEN day_date = DATE_TRUNC('day', start_dt) THEN start_dt
            ELSE day_date
        END AS start_dt,
        -- For end time, use original if it's the last day, otherwise use midnight of next day
        CASE 
            WHEN day_date = DATE_TRUNC('day', end_dt) THEN end_dt
            ELSE DATEADD(day, 1, day_date)
        END AS end_dt,
        original_start_dt,
        original_end_dt,
        adjustment_flag,
        event_source,
        priority_level,
        is_down
    FROM date_series
)

-- Final selection
SELECT 
    well_id,
    log_id,
    status_id,
    start_dt,
    end_dt,
    original_start_dt,
    original_end_dt,
    adjustment_flag,
    event_source,
    priority_level,
    is_down,
    DATEDIFF('second', start_dt, end_dt) as duration_seconds
FROM date_split_events
WHERE start_dt < end_dt  -- Exclude zero-length intervals
ORDER BY 
    well_id,
    start_dt DESC,
    priority_level;