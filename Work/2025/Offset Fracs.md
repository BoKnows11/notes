Time Commitment: 1/2-1 hour once a week depending on how smoothly the code runs
General Process:
Go to FracX and download the schedule as a csv file.

Copy and paste this data along with all the misc scedules from emails into the excel. 
Compile all the sources into one main table. Copy and paste the API col into the column on the right of the table. Follow the process until the API 14 (in yellow) is filled out. Copy and paste the yellow values back into the main table's api col. Copy and paste this entire table into Offsest Fracs Sched Current.xmls since that is the file the code references. Close this once copied and saved so that the python file can read it. Then copy and paste the API with the dashes into enverus's filter menu. Export/unzip the shape files of this API filtered well list to the directory:
C:\Users\bob20000\OneDrive - INEOS USA LLC\Offset Fracs\EnverusData\Offset Fracs

Rename the legacy files named "offsetfracs" with the date of their creation at the end of their name (for ex, "offsetfracs_2-19"). Rename then recently exported shape files from enverus to offsetfracs.
All 4 files (.dbf, .prj, .shp, & .shx) will need to be renamed. 

Close out Offsest Fracs Sched Current.xmls so the python code can read it. 
Open up the the python code and run it. Maps will generate and dfs will be copied that need to be pasted inside the Offsest Fracs Sched Current.xmls 
Resave the Offsest Fracs Sched Current.xmls to Offsest Fracs Sched MM/DD/YYYY.xmls once the dfs from the code are copied into the two other tabs. 