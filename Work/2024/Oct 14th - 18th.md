- [ ] start getting notes into github projects
- Restore OneDrive to pre-OCT 5th issues 
	- [x] Keith is running a powershell script that should sync it back up
	- [x] All synced up again 


 Detechtion Data
 - [x] Get data into snowflake prod env (Jared)
 - [x] Create Detechtion asset ID to INEOS site id mapping table
 - [ ] Incorporate data into existing PBI tools


 ## Rod Pumps
 - [x] Register with Lufkin for Scada documentation
 - [ ] Adding pump off time setpoint to IIQ data. 
	 - [ ] Find pump off time piq in lufkin docs
 - [x] Work with Jeremy to get the current RL wells not sending cards to send them
	 - [ ] Jeremy sees the cards in the UI on the few he checked. Ryan is going to dig into where the disconnect is happening. 
 - [ ] Table to find the BHA type for RL well and incorporate the BHA types into algo logic for setpoints
 - [ ] Flag Development 
 
 
 Miscellaneous
 - [ ] Matt brought up the McCarley Light new wells not being in FF and PBI reports. 
 - [x] AFL Tool Use Log
 - [ ] Surface Csg Tool for SHER
 - [x] Review the well_test table Ryan put together prior to the 1:30 call so we can give him some feedback on it? I would compare it to the values in IIQ front end and validate accuracy.

Flare Report
- [ ] Incorporate production vols into the flare flagging. This will filter out false positives when a well has a cold flare and is not producing. 
- [x] Update the plunger tool data again (Kade) - Got this recovered and set up to update daily. 
- [ ] Setting up TAM file queries to automate data gathering for fluid level shots 


- [ ] Dan temperature query


- [x] Down well query 
	- [ ] Need to make the query more permanent, but the query is built out for testing 


PDFs for every route 



We need a more accurate current oil vol and projected oil vol. Right now, I am looking at the last time the oil_vol_tdy was less than the previous value (this should return the timestamp just after rtu clock changes to a new day) and getting the value and timestamp of the most recent oil_vol_tdy measurement. By knowing what time the value reset at, we can calculate a projected 24 hour rate and compare that to the oil goal. 
A further improvement would be to get the avg shrink factor from the oam vols vs allocated vols to scale the oam values automatically. This will held clean up noise on meters that consistently have water flowing through their meters. 
To get bulk and test wells vols, we would ideally sum the test and bulk oams and divy the production by the last test data we have on file - or divvy the prod by the oil forecasts to get a rough idea how much each well is contributing.  
