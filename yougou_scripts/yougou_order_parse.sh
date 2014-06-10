#!/bin/bash
#yougou_order_parse.sh


#E_ARGERROR=85

#if [ -z "$1" ]
#then
#  echo "Usage: `basename $0` Filename-to-upload"
#  exit $E_ARGERROR
#fi

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

echo "查询订单日期$dd"

timestamp=$(date "+%Y%m%d_%H%M%S")

echo "####################$dd####################"

sitecode="T-000436-01"
db="mso"
product='idigger'

###生成landingpage临时表，orderpage临时表
sql="
drop table if exists user_wangwentao_yougou_landingpage;
create table user_wangwentao_yougou_landingpage as 
select from_unixtime(rawLog_.timestamp_) ts,rawLog_.allyesid_ cookie, pageurl_ url,dd ,rawLog_.referrer_ ref
from pb_idigger_tracklog_partial
where sitecode_ = '$sitecode' and
dd >= '$dd_pre' and dd<='$dd'  and product='$product' and 
pageurl_ like '%utm_source=%' ;

drop table if exists user_wangwentao_yougou_orderpage;
create table user_wangwentao_yougou_orderpage as 
select 
   from_unixtime(rawLog_.timestamp_) ts,
   rawLog_.allyesid_ cookie, 
   pageurl_  url,
   rawLog_.referrer_ ref,
   ecominfo_.transaction_.id_ orderid,
   ecominfo_.transaction_.value_ ordervalue,
   dd    
from
    pb_idigger_tracklog_partial
where
    sitecode_ = '$sitecode' and
    dd = '$dd' and db = '$db'
    and product='$product' and  length(ecominfo_.transaction_.id_)>0;

"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi


###通过cookie来join临时表landingpage和临时表orderpage
sql="
drop table if exists user_wangwentao_yougou_join;
create table user_wangwentao_yougou_join as

select ts,cookie,url,ref,orderid,ordervalue,dd
from
(
select a1.ts, a1.cookie, a1.url,a1.ref, '' orderid,cast(-1 as bigint) ordervalue,a1.dd   from 
(select  ts,cookie,url,dd,ref from user_wangwentao_yougou_landingpage) a1 join
(select  distinct cookie from user_wangwentao_yougou_orderpage) a2 on a1.cookie=a2.cookie

union all

select b2.ts, b2.cookie ,b2.url,b2.ref,b2.orderid,b2.ordervalue,b2.dd from
(select  distinct cookie from user_wangwentao_yougou_landingpage) b1 join
(select  ts,cookie,url,ref, orderid,ordervalue,dd from user_wangwentao_yougou_orderpage
) b2 on b1.cookie=b2.cookie
)a
order by cookie,ts;
"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi


###通过transform生成结果表
sql="
add file  yougou_utils.py;
add file  yougou_order_parse.py;
drop table if exists user_wangwentao_yougou_order_res_s_$dd_t;
create table user_wangwentao_yougou_order_res_s_$dd_t as
select cookie,order_source,landing_ts,order_ts,order_id,order_value from 
(select transform(*) using  'yougou_order_parse.py' as (ts,cookie,url,ref,idiggerorderid,landing_ts,order_ts,order_id,order_value,order_source,utm_source) 
 from   (select ts,cookie,url,ref,orderid,ordervalue,dd from user_wangwentao_yougou_join)a)b;
"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi
 

