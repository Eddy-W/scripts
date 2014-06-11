#! /bin/sh
# -*- coding: utf-8 -*-
set -x
if [ $# -lt 5 ]; then
	echo "Usage: $0 taskspec tmpdir dfsdir tasktype output taskid  "
	exit 1
fi
 

BINDIR=$(dirname "$0")
SPEC=$1
TMP=$2
DIR=$3
TASKTYPE=$4
OUTPUT=$5
TASKID=$6
 
DATE=0
FREQUENCY=5
CID=00000
DOMAIN=0

if [ -r $BINDIR/env.sh ]; then
	. $BINDIR/env.sh
fi

# load custom parameters from taskspec
while read line; do
	key=$(expr "$line" : '\(.*\) *= *.* *')
	val=$(expr "$line" : '.* *= *\(.*\) *')
	for p in DATE FREQUENCY CID DOMAIN; do
		if [ "$p" = "$key" ]; then
			eval "$key=$val"
		fi
	done
done < $SPEC

STARTDATE=$(expr "$DATE" : '\(.*\) *, *.* *')
ENDDATE=$(expr "$DATE" : '.* *, *\(.*\) *')

#D2="$D1,$(date -d @$s +%Y-%m-%d)"


fail() {
	python2.6 $BINDIR/checkCookie.py done $SPEC 4 "$1"
	exit 0
}



 
python2.6 $BINDIR/run_checkcookie.py  $DATE $FREQUENCY $CID $DOMAIN $TASKID $OUTPUT


# cleanup
if [ -r $OUTPUT ]; then
 python2.6 $BINDIR/checkCookie.py done $SPEC 3 '完成'
else
 python2.6 $BINDIR/checkCookie.py done $SPEC 4 '失败'
fi
#任务完成
