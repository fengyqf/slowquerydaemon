# slowquerydaemon
monitor slow mysql query, and kill then if you need

kill slow mysql query,filter by:
-  longer than a defined time, 
-  query match defined pattern

run it as a python process, like a daemon

## configure
### server
declare the server to monitor


### moniters
slow querys filters, and what to do (kill, log) with them

in section [moniter_n]
use * for wield match all these fields: user, host, db, command, state

moniters used order by the order defined in config.ini, the "n" in [moniter_n] will be ignored
