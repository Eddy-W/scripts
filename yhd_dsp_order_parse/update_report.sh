#!/bin/bash
#yougou_creative_parse.sh

debug="0"

dirname=`dirname $0`
cd $dirname


dd=$(date -d "$1 - 1 day" "+%Y-%m-%d")
dd_t=$(date -d "$1 - 1 day" "+%m%d")
dd_pre=$(date -d "$1 -2 day" "+%Y-%m-%d")


if [ -n "$1" ]; then
  dd=$1
  dd_t=$(date -d "$1 +0 day" "+%m%d")
  dd_pre=$(date -d "$1 -1 day" "+%Y-%m-%d")
fi


outputfile=res-$dd

echo "#######################UPDATE ORDER STATUS#########################"
python2.6 updateorder.py res-$dd $dd_t $dd

 
echo "#######################SAVE DATE TO FILE#########################"
python2.6 savedata2xls.py res-$dd $dd_t $dd