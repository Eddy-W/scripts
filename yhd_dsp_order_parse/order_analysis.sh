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

db='ifc'
product='ifc'
advertiserid='49'
sitecode='T-000049-01' 
order_table='user_wangwentao_yhd_ordertx_'$dd_t

 

sql="

add file trans_order_ex.py;
 
create table if not exists $order_table as 
select transform(*) using 'trans_order_ex.py' as (cookie,ts,bid,sid,orderid,ordervalue,ref) from
( 
select cookie,ts,bid,sid,orderid,ordervalue,ref
from
(
select ct.cookie cookie,ct.ts ts, cast(ct.bid as string) bid, cast(ct.sid as string) sid, cast('0' as string) orderid,cast('0' as string) ordervalue,cast(ct.ref as string) ref from
(select sawlog_.rawlog_.allyesid_ cookie,
 from_unixtime(sawlog_.rawlog_.timestamp_) ts,
sawlog_.rawlog_.bannerid_ bid,sawlog_.rawlog_.solutionid_ sid, parse_url(sawlog_.rawlog_.referrer_,'HOST') ref
from pb_clicklog where dd<='$dd' and dd>='$dd_pre' and product='$product' and db='$db' and sawlog_.advertiserid_='$advertiserid')ct join  
(
 
select
distinct rawlog_.allyesid_ cookie
from pb_idigger_tracklog_partial where
sitecode_= '$sitecode' and dd='$dd' and product='idigger'  
and  db='$db'  and length(split(parse_url(rawlog_.requesturl_,'QUERY','ecm'),'%60')[2])>0 

)ot on ct.cookie=ot.cookie

union all

 select cookie,ts,cast('' as string) bid,cast('' as string) sid, cast(orderid as string) orderid, cast(ordervalue as string) ordervalue,cast('' as string) ref from
(select
rawlog_.allyesid_ cookie, 
from_unixtime(rawlog_.timestamp_) ts,
split(parse_url(rawlog_.requesturl_,'QUERY','ecm'),'%60')[2] orderid,
split(parse_url(rawlog_.requesturl_,'QUERY','ecm'),'%60')[3] ordervalue
from pb_idigger_tracklog_partial where
sitecode_= '$sitecode' and dd='$dd' and product='idigger'  
and  db='$db'  and length(split(parse_url(rawlog_.requesturl_,'QUERY','ecm'),'%60')[2])>0 order by orderid,ts

)ot2

)xa order by cookie,ts

)ordertmp
"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi


 
 
