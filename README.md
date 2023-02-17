# MDR_Importer
Transfers session data into accumulated data tables.

The program takes the data in the session data or sd tables in each source database (the 'session data' being created by the most recent harvest operation), and compares it with the accumulated data for each source, which is stored in the accumulated data (ad) tables. New and revised data are then transferred to the ad tables.<br/>
The program represents the third stage in the 5 stage MDR extraction process:<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Download => Harvest => **Import** => Coding => Aggregation<br/><br/>
For a much more detailed explanation of the extraction process,and the MDR system as a whole, please see the project wiki, <br/>
(landing page at https://ecrin-mdr.online/index.php/Project_Overview).<br/>
In particular, for the Import process, please see:<br/>
https://ecrin-mdr.online/index.php/Importing_Data 
and linked pages.

## Parameters and Usage
The system can take the following parameters:<br/>
**-s:** expects to be followed by a comma separated list of MDR source integer ids, each representing a data source within the system.<br/>
**-T:** as a flag. If present, forces the (re)creation of a new set of ad tables.<br/>
**-F:** as a flag. If present, operates on the sd / ad tables in the test database.<br/>
**-G:** as a flag. If present, compares and reports on adcomp and expected tables but does not recreate those tables. Often used in conjunction with -F.<br/>
Routine usage, as in the scheduled extraction process, is to use -s followed by a list of one or more source ids.<br/>
Using the -T flag will result n the ad tables being replaced by the sd tables. It therefore only makes if the preceding harvest has been a 'full' harvest, of all available json source files.

## Dependencies
The program is written in .Net 7.0. <br/>
It uses the following Nuget packages:
* CommandLineParser 2.9.1 - to carry out initial processing of the CLI arguments
* Npgsql 7.0.0, Dapper 2.0.123 and Dapper.contrib 2.0.78 to handle database connectivity
* Microsoft.Extensions.Configuration 7.0.0, .Configuration.Json 7.0.0, and .Hosting 7.0.0 to read the json settings file and support the initial host setup.

## Provenance
* Author: Steve Canham
* Organisation: ECRIN (https://ecrin.org)
* System: Clinical Research Metadata Repository (MDR)
* Project: EOSC Life
* Funding: EU H2020 programme, grant 824087
