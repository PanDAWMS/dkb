map_one_table.sh table filename
Generates mapping of one table and stores into filename

map_important_tables.sh
Creates mapping named mapping.ttl for the following tables: T_INPUT_DATASET,T_PRODUCTION_DATASET,T_PRODUCTION_CONTAINER,T_PRODMANAGER_REQUEST,T_PRODUCTION_TASK,ATLAS_DEFT.T_HASHTAG,ALTAS_DEFT.T_T_HT_TO_TASK

map_everything.sh
Attemppts to build a full DB map. Failed due to timeout in 100% cases so far.


!!WARNING!! Before use, replace [USER] and [PASSWORD] with real username and password for the DB.