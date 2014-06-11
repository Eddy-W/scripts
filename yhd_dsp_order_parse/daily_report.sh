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
sidbid_bid_table='user_wangwentao_yhd_bidt_'$dd_t
order_table='user_wangwentao_yhd_ordert_'$dd_t

sql="
create table if not exists $sidbid_bid_table as
select a.sid,
if(a.bidn is null,0,a.bidn) bidn,
if(b.winn is null,0,b.winn) winn,
if(b.ws is null,0,b.ws) ws,
if(c.clk is null,0,c.clk) clk,
if(a.bidprice is null,0,a.bidprice) bidprice,
if(a.bidfloor is null,0,a.bidfloor) bidfloor
from 

(select sid,count(*) bidn,sum(bidprice)/pow(10,6) bidprice,sum(bidfloor)/pow(10,6) bidfloor from
(select 
vsolutionid_ sid,
ifcbids_.pricebidfloor_ bidfloor,
ifcbids_.pricebid_   bidprice
from  pb_ifc_bidlog where dd='$dd' and ifcbids_.pricebid_>0) rawbidt join
(select id from b_solution where advertiser_id=$advertiserid and campaign_id=132) bsolution1 on rawbidt.sid=bsolution1.id  group by sid)a left outer join
(select sid,count(*) winn, sum(winprice)/pow(10,9)  ws
from
(select sawlog_.rawlog_.solutionid_ sid,sawlog_.rawlog_.sellingprice_ winprice
from pb_showlog where dd='$dd' and product='$product' and db='$db') showt join
(select id from b_solution where advertiser_id=$advertiserid and campaign_id=132) bsolution2 on showt.sid=bsolution2.id group by sid) b on a.sid=b.sid left outer join
(select sid,count(*) clk
from
(select sawlog_.rawlog_.solutionid_ sid
from pb_clicklog where dd='$dd' and product='$product' and db='$db') clickt join
(select id from b_solution where advertiser_id=$advertiserid and campaign_id=132) bsolution2 on clickt.sid=bsolution2.id group by sid)c on a.sid=c.sid
;
"

cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi


sql="

add file trans_order.py;
 
create table if not exists $order_table as 
select transform(*) using 'trans_order.py' as (cookie,ts,bid,sid,orderid,ordervalue) from
( 
select cookie,ts,bid,sid,orderid,ordervalue
from
(
select ct.cookie cookie,ct.ts ts, cast(ct.bid as string) bid, cast(ct.sid as string) sid, cast('0' as string) orderid,cast('0' as string) ordervalue from
(select sawlog_.rawlog_.allyesid_ cookie,
 from_unixtime(sawlog_.rawlog_.timestamp_) ts,
sawlog_.rawlog_.bannerid_ bid,sawlog_.rawlog_.solutionid_ sid
from pb_clicklog where dd<='$dd' and dd>='$dd_pre' and product='$product' and db='$db' and sawlog_.advertiserid_='$advertiserid')ct join  
(
 
select
distinct rawlog_.allyesid_ cookie
from pb_idigger_tracklog_partial where
sitecode_= '$sitecode' and dd='$dd' and product='idigger'  
and  db='$db'  and length(split(parse_url(rawlog_.requesturl_,'QUERY','ecm'),'%60')[2])>0 

)ot on ct.cookie=ot.cookie

union all

 select cookie,ts,cast('' as string) bid,cast('' as string) sid, cast(orderid as string) orderid, cast(ordervalue as string) ordervalue from
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



outputfile=res-$dd
if [ $debug != "1" ]; then
  eval "mkdir $outputfile"
fi


#统计  分策略
sql=" 
insert overwrite  directory '/user/group_dataanalysis/yhd_dsp/order_parse/$outputfile'
select a.sid sid,c.name,bidn,winn,winrate,clk,ctr,ws,cpm,cpc,avgbidfloor,avgbidprice,ordernum,ordervaluesum
from
(select sid, bidn, winn,winn/bidn winrate, clk, clk/winn ctr, ws,
ws*1000/winn cpm,
ws/clk cpc,
bidfloor/bidn avgbidfloor,
bidprice/bidn avgbidprice 
from $sidbid_bid_table )a left outer join
(select sid,count(*) ordernum,sum(ordervalue) ordervaluesum from $order_table group by sid)b on a.sid=b.sid
left outer join
(select id,name from b_solution where advertiser_id='$advertiserid')c on a.sid=c.id order by sid
"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

cmd="hadoop fs -text   /user/group_dataanalysis/yhd_dsp/order_parse/$outputfile/* > /home/group_dataanalysis/yhd_Report/$outputfile/output-1"  
echo "$cmd"
if [ $debug != "1" ]; then
 eval "$cmd"
fi 

#统计 订单-创意
sql="
  
insert overwrite  directory '/user/group_dataanalysis/yhd_dsp/order_parse/$outputfile'

select a.sid,b.name,a.bid,c.name,ts,orderid,ordervalue from
(select sid,bid,ts,orderid,ordervalue from $order_table )a left outer join
(select id,name from b_solution where advertiser_id='$advertiserid')b on a.sid=b.id left outer join
(select id,name from b_banner where advertiser_id='$advertiserid')c on a.bid=c.id order by ts


;"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

cmd="hadoop fs -text   /user/group_dataanalysis/yhd_dsp/order_parse/$outputfile/* > /home/group_dataanalysis/yhd_Report/$outputfile/output-2"  
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

echo "#######################DROP HIVE TABLE#########################"



sql="
drop table if exists $sidbid_bid_table;
drop table if exists $order_table; 
 "
#cmd="hive -e \"$sql\""
#echo "$cmd"
#if [ $debug != "1" ]; then
#	  eval "$cmd"
#fi
 


echo "#######################UPDATE ORDER STATUS#########################"
python2.6 updateorder.py res-$dd $dd_t $dd

 
echo "#######################SAVE DATE TO FILE#########################"
python2.6 savedata2xls.py res-$dd $dd_t $dd

 

echo "#######################MAIL FILE#########################"
python2.6 mailxls.py res-$dd $dd_t $dd


echo $(date "+%Y%m%d_%H%M%S")
 
 
