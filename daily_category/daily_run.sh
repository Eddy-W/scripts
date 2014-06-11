#!/bin/bash

dirname=`dirname $0`
cd $dirname



dd=" dd='"$(date -d "$1 - 2 day" "+%Y-%m-%d")"' "
dd_t=$(date -d "$1 - 2 day" "+%m%d")
 
#TRUNCATE TABLE user_wangwentao_test_res PARTITION ( dd='2012-12-12' ,type= 'domain');

if [ -n "$1" ]; then
  dd=" dd='"$(date -d "$1 +0 day" "+%Y-%m-%d")"' "
  dd_t=$(date -d "$1 +0 day" "+%m%d")
fi


debug="0"

req_table='user_wangwentao_adx_analysis_'$dd_t'_req'
req_table_trans='user_wangwentao_adx_analysis_'$dd_t'_req_trans'
bid_table='user_wangwentao_adx_analysis_'$dd_t'_bid'
bid_table_trans='user_wangwentao_adx_analysis_'$dd_t'_bid_trans'
 

#############################################################################
createsql="
create table user_wangwentao_adx_detectvertical (
province string,
domain string,
smallcategory string,
bigcategory string,
nativecode int,
reqns bigint,
bidns bigint,
impns bigint,
clkns bigint,
wp bigint,
bidfloor bigint,
bidprice bigint,
info_category struct<c0:bigint, c1:bigint, c2:bigint, c3:bigint, c4:bigint>
) partitioned by (dd string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'    ;


create table user_wangwentao_adx_traffic (
province string,
domain string,
size string,
reqns bigint,
bidns bigint,
impns bigint,
clkns bigint,
wp bigint,
bidfloor bigint,
bidprice bigint,
info_bidfloor struct<b0:bigint, b1:bigint, b2:bigint, b3:bigint, b4:bigint, b5:bigint>
) partitioned by (dd string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'; "
#################################REQUEST######################################

sql="
add jar allyes-hive-udf-1.4.0.jar; 
create temporary function url2domain as 'com.allyes.hive.udf.Url2domain';

create table if not exists  $req_table  as
select 
get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.geo_criteria_id') as geoid,
url2domain(raw_log.referrer,get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.mobile.platform'),
get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.mobile.app_id'),
get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.anonymous_id')) domain,
ifcbids.price_bidfloor bidfloor,
concat(ifcbids.adslot_width,'x',ifcbids.adslot_height,':',
lower(get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.adslot[0].slot_visibility'))) as sz,
 
get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.detected_vertical.id') as cateid,
get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.detected_vertical.weight') as cateweg
from pb_ifc_bidlog
where $dd and raw_log.exchange_id=2
"

cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi





sql="
create table if not exists  $req_table_trans  as
select
geomapt.province,if(domainnstopt.domain is null,'other',domainnstopt.domain) domain,bidfloor,sz,cateid,cateweg
from
(select geoid,domain,bidfloor,sz,cateid,cateweg from $req_table)reqt left outer join
(select domain from
(select domain,count(*) ns from $req_table group by domain)domainnst where ns>1000)domainnstopt on domainnstopt.domain=reqt.domain join
(select province,geoid from user_wangwentao_adx_geo_map)geomapt on reqt.geoid=geomapt.geoid
"

cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi
 

 
 

##################################BID#########################

sql="

add jar allyes-hive-udf-1.4.0.jar; 
create temporary function url2domain as 'com.allyes.hive.udf.Url2domain';

create table if not exists $bid_table as
select bid_t.geoid,bid_t.domain,
bid_t.sz,
bid_t.bidfloor bidfloor,
bid_t.cateid,bid_t.cateweg,
bid_t.bidprice bidprice,
if(show_t.impid is null, 0,1) imptag,
if(show_t.winprice is null,0,show_t.winprice) winprice,
if(clk_t.impid is null, 0,1) clktag
from
(
select 
 
raw_log.impression_id as impid,  
get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.geo_criteria_id') as geoid,
url2domain(raw_log.referrer,get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.mobile.platform'),
get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.mobile.app_id'),
get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.anonymous_id')) domain,
ifcbids.price_bid bidprice,
ifcbids.price_bidfloor bidfloor,
concat(ifcbids.adslot_width,'x',ifcbids.adslot_height,':',
lower(get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.adslot[0].slot_visibility'))) as sz,
get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.detected_vertical.id') as cateid,
get_json_object(bidrequest(raw_log.post_data_type,raw_log.post_data), '$.detected_vertical.weight') as cateweg
from pb_ifc_bidlog
where $dd and raw_log.exchange_id=2 and ifcbids.price_bid>0
)bid_t
LEFT OUTER JOIN
(
select 
dd,
saw_log.raw_log.selling_price as winprice, saw_log.raw_log.impression_id as impid
from
pb_showlog
where product='ifc' and db='ifc' and $dd and saw_log.raw_log.exchange_id=2
)show_t ON(bid_t.impid=show_t.impid)
LEFT OUTER JOIN
(
select  
dd,
saw_log.raw_log.impression_id as impid
from
pb_clicklog
where product='ifc' and db='ifc' and $dd and saw_log.raw_log.exchange_id=2
)clk_t ON(show_t.impid = clk_t.impid)
"

cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi
 

sql="
create table if not exists  $bid_table_trans  as
select
geomapt.province,if(domainnstopt.domain is null,'other',domainnstopt.domain) domain,bidfloor,sz,cateid,cateweg,bidprice,imptag,winprice,clktag
from
(select geoid,domain,bidfloor,sz,cateid,cateweg,bidprice,imptag,winprice,clktag from $bid_table)reqt left outer join
(select domain from
(select domain,count(*) ns from $req_table group by domain)domainnst where ns>1000)domainnstopt on domainnstopt.domain=reqt.domain join
(select province,geoid from user_wangwentao_adx_geo_map)geomapt on reqt.geoid=geomapt.geoid
"

cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

 


################################TRANS ALL#########################################

sql=" 
insert overwrite table user_wangwentao_adx_traffic partition( $dd )     
select
reqt.province,reqt.domain,reqt.sz,reqt.reqns,
bidns,impns,clkns,wp,bidfloor,bidprice,
named_struct('b0',b0,
'b1',b1,
'b2',b2,
'b3',b3,
'b4',b4,
'b5',b5)
from
(select
province,domain,sz,count(*) reqns,
sum(if(bidfloor<=50000,1,0)) b0,
sum(if(bidfloor>50000 and bidfloor <=100000,1,0)) b1,
sum(if(bidfloor>100000 and bidfloor<=500000,1,0)) b2,


sum(if(bidfloor>500000 and bidfloor<=1000000,1,0)) b3,
sum(if(bidfloor>1000000 and bidfloor<=2000000,1,0)) b4,
sum(if(bidfloor>2000000,1,0)) b5
from $req_table_trans  
group by province,domain,sz)reqt  left outer join 
(select
province,domain,sz,count(*) bidns,sum(imptag) impns,sum(clktag) clkns,sum(winprice) wp,sum(bidfloor) bidfloor,sum(bidprice) bidprice
from  $bid_table_trans  
group by province,domain,sz)bidt on reqt.province=bidt.province and reqt.domain=bidt.domain and reqt.sz=bidt.sz

"

cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi



sql="
add file nativecode2category_bid.py;
add file nativecode2category_req.py;
insert overwrite table user_wangwentao_adx_detectvertical partition($dd)     
select
req_extt.province,req_extt.domain,req_extt.self,req_extt.parent,req_extt.nativecode,req_extt.reqns,
bidns,impns,clkns,wp,bidfloor,bidprice,
named_struct(
'c0',c0,
'c1',c1,
'c2',c2,
'c3',c3,
'c4',c4)
from
(select
province,domain,self,parent,reqt.nativecode nativecode,count(*) reqns, 
sum(if(c0>0,1,0)) c0,
sum(if(c1>0,1,0)) c1,
sum(if(c2>0,1,0)) c2,
sum(if(c3>0,1,0)) c3,
sum(if(c4>0,1,0)) c4
from
(select transform(*) using 'nativecode2category_req.py' as (province,domain,bidfloor,nativecode,c0,c1,c2,c3,c4)
from  (select province,domain,bidfloor,sz,cateid,cateweg from $req_table_trans )reqtmp)reqt join  
(select nativecode,self,parent from user_wangwentao_adx_category_map)categoryt1 on reqt.nativecode=categoryt1.nativecode
group by province,domain,self,parent,reqt.nativecode)req_extt left outer join
(select
province,domain,self,parent,bidt.nativecode nativecode,count(*) bidns,sum(imptag) impns,sum(clktag) clkns,sum(winprice) wp,sum(bidfloor) bidfloor,sum(bidprice) bidprice
from
(select transform(*) using 'nativecode2category_bid.py' as (province,domain,bidfloor,nativecode,bidprice,imptag,winprice,clktag)
from  (select province,domain,sz,bidfloor,cateid,cateweg,bidprice,imptag,winprice,clktag from $bid_table_trans )bidtmp)bidt join  
(select nativecode,self,parent from user_wangwentao_adx_category_map)categoryt2 on bidt.nativecode=categoryt2.nativecode
group by province,domain,self,parent,bidt.nativecode)bid_extt on req_extt.province=bid_extt.province and req_extt.domain=bid_extt.domain
and req_extt.nativecode=bid_extt.nativecode 
"

cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi



################################DROP TABLE#########################################

req_table='user_wangwentao_adx_analysis_'$dd_t'_req'
req_table_trans='user_wangwentao_adx_analysis_'$dd_t'_req_trans'
bid_table='user_wangwentao_adx_analysis_'$dd_t'_bid'
bid_table_trans='user_wangwentao_adx_analysis_'$dd_t'_bid_trans'




sql="
drop table $req_table;
drop table $req_table_trans;
drop table $bid_table;
drop table $bid_table_trans;
"

cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

