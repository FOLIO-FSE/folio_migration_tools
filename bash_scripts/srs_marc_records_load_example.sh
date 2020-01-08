#!/bin/bash
# A script for importing Inventory data

set -e
set -u

export DBHOST=$1
export USER=$2
export INSTANCES_PATH=$3
RUN_ON_MYDB="psql -X -U $USER -h $DBHOST --set ON_ERROR_STOP=on --set AUTOCOMMIT=off folio"

$RUN_ON_MYDB <<SQL
\copy TENANT_ID_mod_source_record_storage.marc_records(_id, jsonb) from '$INSTANCES_PATH' csv quote e'\x01' delimiter E'\t';
commit;
vacuum verbose analyze TENANT_ID_mod_source_record_storage.marc_records;
commit;
