#Copyright (C) 2012 David L. Lahr
#
#Permission is hereby granted, free of charge, to any person obtaining a copy of 
#this software and associated documentation files (the "Software"), to deal in the 
#Software without restriction, including without limitation the rights to use, copy, 
#modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
#and to permit persons to whom the Software is furnished to do so, subject to the
#following conditions:
#
#The above copyright notice and this permission notice shall be included in all copies
#or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
#INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
#PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
#FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR 
#OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
#DEALINGS IN THE SOFTWARE.

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
