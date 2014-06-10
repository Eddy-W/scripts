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

 
echo "#################  YOUGOU KPI PARSE--$dd  ##########################"
echo $(date "+%Y%m%d_%H%M%S")
  
db="ifc"
product='ifc'
media_str_1_1='bid string, bname string, sid string,sname string, ex string, size string, type1 string, type2 string'
media_str_2_1='bid,bname,sid,sname,ex,size,type1,type2'
media_str_3_1='a.bid,a.bname,a.sid,a.sname,a.ex,a.size,a.type1,a.type2'
creative_file_name_1='b_creative.in'

media_str_1_2='bid string, bname string,
creativeWidth string,creativeHight string,creativeType string,creativeExtension string,
 sid string,sname string, adNetwork string'
media_str_2_2="bid,bname,sid,sname,adNetwork ex,concat(creativeHight,'x',creativeHight) size ,creativeType type1, creativeExtension type2"
media_str_3_2='a.bid,a.bname,a.sid,a.sname,a.ex,a.size,a.type1,a.type2'
creative_file_name_2='b_creative_2.in'

media_str_1_3='bid string, bname string,sid string,sname string, aid string, aname string, size string,adNetwork string, isDynamic string,
dynamictype string  '
media_str_2_3="bid,bname,sid,sname,adNetwork ex, size , isDynamic type1, dynamictype type2"
media_str_3_3='a.bid,a.bname,a.sid,a.sname,a.ex,a.size,a.type1,a.type2'
creative_file_name_3='base_media'
 

creative_file_name=$creative_file_name_3
media_str_1=$media_str_1_3
media_str_2=$media_str_2_3
media_str_3=$media_str_3_3
 



sidbid_bid_table='user_wangwentao_yougou_sidbid_tmp_'$dd_t
sidbid_media_table='user_wangwentao_yougou_dsp_media_'$dd_t
sidbid_res_table='user_wangwentao_yougou_sidbid_res_'$dd_t
sidbid_order_table='user_wangwentao_yougou_order_res_'$dd_t
sitecode="T-000436-01"
db_order="mso"
product_order='idigger'
order_landingpage_table='user_wangwentao_yougou_landingpage_'$dd_t
order_orderpage_table='user_wangwentao_yougou_orderpage_'$dd_t
order_join_table='user_wangwentao_yougou_join_'$dd_t 
 


cmd="hadoop fs -text   /user/group_ifc/data4wt/* > /home/group_dataanalysis/wangwentao/yougou_order_parse/base_media"  
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi 

sidstr=''
while read LINE
do
	arr=(${LINE//	/ })  
	#echo ${arr[0]},${arr[2]}
	bidstr+=${arr[0]},
done<  $creative_file_name
bidstrl=${#bidstr}-1

#bid="find_in_set(rawlog_.bannerid_,'"${bidstr:0:$bidstrl}"')>0"
bid=" find_in_set(rawlog_.bannerid_,'7394,7395,7396,7397,7398,7399,7400')>0 "

#sql="
#drop table if exists $sidbid_media_table;
#CREATE TABLE if not exists $sidbid_media_table  ($media_str_1)
#ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' ;  
#LOAD DATA LOCAL INPATH './$creative_file_name' OVERWRITE INTO TABLE $sidbid_media_table;
#"

 
sql=" 
CREATE TABLE if not exists $sidbid_media_table  ($media_str_1)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' ; 
insert overwrite table $sidbid_media_table
select b.id,b.name,a.id,a.name,'32','优购',b.size,'','','' from
(select id,name from b_solution where advertiser_id=32 and find_in_set(id,'1785,1784,257,256')>0)a join  
(select id,name,concat(width,'x',height)size from b_banner where advertiser_id=32)b 

"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi
 
 


echo '###生成landingpage临时表，orderpage临时表'
#drop table if exists $order_landingpage_table;
#drop table if exists $order_orderpage_table;
sql=" 

create table if not exists  $order_landingpage_table as 
select from_unixtime(rawLog_.timestamp_) ts,rawLog_.allyesid_ cookie, pageurl_ url,dd ,rawLog_.referrer_ ref
from pb_idigger_tracklog_partial
where sitecode_ = '$sitecode' and
dd >= '$dd_pre' and dd<='$dd'  and product='$product_order' and db = '$db_order' and
pageurl_ like '%utm_source=%' ;


create table if not exists $order_orderpage_table as 
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
    dd = '$dd' and db = '$db_order'
    and product='$product_order' and  length(ecominfo_.transaction_.id_)>0;

"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi


echo '###通过cookie来join临时表landingpage和临时表orderpage'
#drop table if exists $order_join_table;
sql="

create table if not exists $order_join_table as

select ts,cookie,url,ref,orderid,ordervalue,dd
from
(
select a1.ts, a1.cookie, a1.url,a1.ref, '' orderid,cast(-1 as bigint) ordervalue,a1.dd   from 
(select  ts,cookie,url,dd,ref from $order_landingpage_table) a1 join
(select  distinct cookie from $order_orderpage_table) a2 on a1.cookie=a2.cookie

union all

select b2.ts, b2.cookie ,b2.url,b2.ref,b2.orderid,b2.ordervalue,b2.dd from
(select  distinct cookie from $order_landingpage_table) b1 join
(select  ts,cookie,url,ref, orderid,ordervalue,dd from $order_orderpage_table
) b2 on b1.cookie=b2.cookie
)a
order by cookie,ts;
"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi


echo '###通过transform生成结果表'
 

 
#drop table if exists $sidbid_order_table; 
sql="
add file  yougou_utils.py;
add file  yougou_order_parse.py;
add file  yougou_creative_order_parse.py;
add  jar myhiveudf_new.jar;
CREATE TEMPORARY FUNCTION row_number  As'com.example.hive.udf.RowNumber';

create table if not exists $sidbid_order_table as

select bid,bname,sid,sname,orderid,orderts,ordervalue,impid from
(select 
ob.bid,if (ob.bname is null,'-',ob.bname) bname, 
ob.sid,if (ob.sname is null,'-',ob.sname) sname,
oa.orderid,oa.orderts,oa.ordervalue,if(oa.impid is null,'-1',oa.impid) impid from 
(
select transform(*) using  'yougou_creative_order_parse.py' as (bid,sid,orderid,orderts,ordervalue,impid) from
(
select  if(bid is null,0,bid) bid,if(sid is null,0,sid) sid,orderid,orderts,ordervalue,tt,impid from
(
select b1.ts clkts,b1.bid,b1.sid,b2.order_source source,b2.order_ts orderts,b2.order_id orderid, b2.order_value ordervalue
, if(b1.ts is null,0, unix_timestamp(b2.order_ts)-unix_timestamp(b1.ts)) tt,b1.impid
 from
(
select sawlog_.rawlog_.allyesid_ cookie,
 from_unixtime(sawlog_.rawlog_.timestamp_) ts,
sawlog_.rawlog_.bannerid_ bid,sawlog_.rawlog_.solutionid_ sid,
sawlog_.rawlog_.impressionid_ impid
from pb_clicklog where dd<='$dd' and dd>='$dd_pre' and product='$product' and db='$db'
) b1  right outer join 
(select cookie,order_source,landing_ts,order_ts,order_id,order_value from  
(select transform(*) using  'yougou_order_parse.py' as (ts,cookie,url,ref,idiggerorderid,landing_ts,order_ts,order_id,order_value,order_source,utm_source) 
 from   (select ts,cookie,url,ref,orderid,ordervalue,dd from $order_join_table)a11)b11
 
)b2
on b1.cookie=b2.cookie )bx1  order by orderid,tt) bx2 
)oa left outer join
(select $media_str_2 from  $sidbid_media_table )ob on oa.bid=ob.bid and oa.sid=ob.sid
where substr(oa.orderid,4,4)='$dd_t' order by orderid,orderts) xx where row_number(orderid)=1 
;
"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi



 

#echo $bid
#drop table if exists $sidbid_bid_table;
sql="
create table if not exists $sidbid_bid_table as
select sid,bid,count(*) bidn, count(wintag) winn,sum(winprice)/1000000000  ws, count(clktag) clk
from 
(select ca.impid, ca.sid, ca.bid, ca.bidprice,
if(cb.winprice is null,0,cb.winprice) winprice,cb.wintag,cc.clktag
from
(select rawlog_.impressionid_ impid,rawlog_.allyesid_ cookie,
rawlog_.solutionid_ sid, rawlog_.bannerid_ bid,
rawlog_.exchangeid_ exid,
ifcbids_.pricebid_   bidprice
from  pb_ifc_bidlog where dd='$dd' and ifcbids_.pricebid_>0  and find_in_set( rawlog_.solutionid_,'1785,1784,257,256')>0 )ca
left outer join (
select sawlog_.rawlog_.impressionid_ impid, sawlog_.rawlog_.allyesid_ cookie,
sawlog_.rawlog_.bannerid_ bid,sawlog_.rawlog_.solutionid_ sid, sawlog_.rawlog_.sellingprice_ winprice, '1' wintag
from pb_showlog where dd='$dd' and product='$product' and db='$db'
)cb on (ca.impid=cb.impid and ca.sid=cb.sid and ca.bid=cb.bid and ca.cookie=cb.cookie)
left  outer join
(
select sawlog_.rawlog_.impressionid_ impid, sawlog_.rawlog_.allyesid_ cookie,

sawlog_.rawlog_.bannerid_ bid,sawlog_.rawlog_.solutionid_ sid, '1' clktag
from pb_clicklog where dd='$dd' and product='$product' and db='$db'
)cc on (ca.impid=cc.impid and ca.sid=cc.sid and ca.bid=cc.bid))cx group by sid,bid;
"

cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi





echo '###################### Parse Order to Bid&Sid ########################'
#drop table if exists $sidbid_res_table;
sql="
create table if not exists $sidbid_res_table as
select a.bid,d.name bname,a.sid,c.name sname,'',d.size,'','',a.bidn,a.winn,a.winrate,
a.ws,a.clk,a.ctr,a.cpm,a.cpc,b.ordern,b.ordersum
,b.ordersum/b.ordern orderpervalue,b.ordersum/a.ws roi
from
(
select sid,bid, bidn,winn,winn/bidn winrate,ws,  clk,clk/winn ctr, 1000*ws/winn cpm, 
if(clk=0,0,ws/clk) cpc from $sidbid_bid_table 
)a left outer join
(
select bid,sid,count(*) ordern,floor(sum(ordervalue)/10000) ordersum from 
$sidbid_order_table  group by bid,sid
) b on a.bid=b.bid and a.sid=b.sid left outer join
(select distinct id,name from b_solution) c on a.sid=c.id left outer join
(select distinct id,name,concat(width,'x',height) size from b_banner) d on a.bid=d.id ;
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

#统计1 分创意
sql="
insert overwrite  directory '/user/group_dataanalysis/yougou_dps/order_parse/$outputfile'
select * from $sidbid_res_table;"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

cmd="hadoop fs -text   /user/group_dataanalysis/yougou_dps/order_parse/$outputfile/* > /home/group_dataanalysis/wangwentao/yougou_order_parse/$outputfile/output-1"  
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi 

#统计2 分策略
sql="
insert overwrite  directory '/user/group_dataanalysis/yougou_dps/order_parse/$outputfile'
select
sid,sname,sum(bidn),sum(winn), sum(winn)/sum(bidn) winrate, sum(ws) ws,sum(clk) clk, sum(clk)/sum(winn) ctr, 1000*sum(ws)/sum(winn) cpm,
sum(ws)/sum(clk) cpc,sum(ordern) ordern,sum(ordersum) ordersum, sum(ordersum)/sum(ordern) orderpervalue,sum(ordersum)/sum(ws) roi
 from $sidbid_res_table group by sid,sname;"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

cmd="hadoop fs -text   /user/group_dataanalysis/yougou_dps/order_parse/$outputfile/* > /home/group_dataanalysis/wangwentao/yougou_order_parse/$outputfile/output-2"  
echo "$cmd"
if [ $debug != "1" ]; then
 eval "$cmd"
fi 

#统计 订单-创意
sql="
add  jar myhiveudf_new.jar;
CREATE TEMPORARY FUNCTION row_number  As'com.example.hive.udf.RowNumber';
insert overwrite  directory '/user/group_dataanalysis/yougou_dps/order_parse/$outputfile'

select bid,bname,sid,sname,orderid,orderts,ordervalue,domain from
(
select xa1.bid,xa1.bname,xa1.sid,xa1.sname,xa1.orderid,xa1.orderts,xa1.ordervalue,xb1.domain from 
(
select bid,bname, sid, sname,orderid,orderts,ordervalue,impid   from $sidbid_order_table
)xa1 left outer join 
(
select rawlog_.impressionid_ impid, 
if( rawlog_.exchangeid_=1, get_json_object(rawlog_.postdata_, '$.site.domain') ,concat('http://',parse_url(rawlog_.referrer_,'HOST'))) as domain
from  pb_ifc_bidlog orderid where dd<='$dd' and dd>='$dd_pre' and ifcbids_.pricebid_>0
)xb1 on xa1.impid!='-1' and xa1.impid=xb1.impid order by xa1.orderid 

)xc 
where row_number(orderid)=1   

;"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

cmd="hadoop fs -text   /user/group_dataanalysis/yougou_dps/order_parse/$outputfile/* > /home/group_dataanalysis/wangwentao/yougou_order_parse/$outputfile/output-3"  
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

echo "#######################DROP HIVE TABLE#########################"



sql="
drop table if exists $sidbid_bid_table;
drop table if exists $sidbid_res_table;
drop table if exists $sidbid_order_table;
drop table if exists $order_landingpage_table;
drop table if exists $order_orderpage_table;
drop table if exists $order_join_table;
drop table if exists $sidbid_media_table;
 "
#cmd="hive -e \"$sql\""
#echo "$cmd"
#if [ $debug != "1" ]; then
#	  eval "$cmd"
#fi
 

echo "#######################SAVE DATE TO FILE#########################"
python2.6 savedata2xls.py res-$dd $dd_t $dd


echo "#######################MAIL FILE#########################"
python2.6 mailxls.py res-$dd $dd_t $dd


echo $(date "+%Y%m%d_%H%M%S")
 


 

 


