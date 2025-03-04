
Template
## Date: 
### What did I get done yesterday?
- 
### Plan for today:



## Date: 3/5/25
### What did I get done yesterday?
- Fixed an issue with the precedence tables where it was filtering out event logs that had null end dates (ongoing). 
	- Added in a coalesce function to the precedence table logic. Increased the time it takes the table to run from ~3-4 min to ~10 min. 
	- Added in some code to the rp and rtu event log tables that move the coalesce stmt on the end dt to these tables. If we move the logic up to the event_log tables, it should reduce the load time for the precedence table back down to the 3-4 min range. 
- Watched some tutorials on dbt
- 
### Plan for today:
