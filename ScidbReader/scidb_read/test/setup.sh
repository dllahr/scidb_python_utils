#!/bin/bash

array_name=$1

#setup to use scidb iquery
export SCIDB_VER=12.10
export PATH=/opt/scidb/$SCIDB_VER/bin:/opt/scidb/$SCIDB_VER/share/scidb:$PATH
export LD_LIBRARY_PATH="/opt/scidb/$SCIDB_VER/lib:/usr/lib/instantclient_11_2/:$LD_LIBRARY_PATH"


#remove raw if present
iquery -naq "remove(raw)"

#create and load into raw
iquery -naq "create array raw <i:int64, j:int64, val1:uint16, val2:uint16> [line=0:*,1000000,0]"

dir=`pwd`
data_file=$dir"/test_data_scidb_read.scidb"
iquery -naq "load(raw, '$data_file')"

#remove array if already present then transfer from raw
iquery -naq "remove($array_name)"
        
iquery -naq "create array $array_name <val1:uint16, val2:uint16> [i=0:3,2,0, j=0:3,1,0]"
iquery -naq "redimension_store(raw, $array_name)"
