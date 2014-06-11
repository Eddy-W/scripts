#!/bin/bash

dirname=`dirname $0`
cd $dirname

dd=" dd='"$(date -d "$1 - 2 day" "+%Y-%m-%d")"' "
dd_t=$(date -d "$1 - 2 day" "+%m%d")
  

if [ -n "$1" ]; then
  dd=" dd='"$(date -d "$1 +0 day" "+%Y-%m-%d")"' "
  dd_t=$(date -d "$1 +0 day" "+%m%d")
  
fi

if [ -n "$2" ]; then
 
  
  dd=" dd>='"$(date -d "$1 +0 day" "+%Y-%m-%d")"' and dd<='"$(date -d "$2 +0 day" "+%Y-%m-%d")"' "
  dd_t=$(date -d "$1 +0 day" "+%m%d")'_'$(date -d "$2 +0 day" "+%m%d")
  echo $dd
  echo $dd_t
   
fi
debug="0"




##############SIZE###################
sql="
add jar allyes-hive-udf-1.4.0.jar;
create temporary function top_group as 'com.allyes.hive.udf.Top4GroupBy';

insert overwrite  directory '/user/group_dataanalysis/adx_daily_report/size' 
select split(size,':')[0],split(size,':')[1],
sum(reqns) reqns,
top_group(domain,cast( if(reqns is null,0,reqns) as int),5) topreq,
top_group(domain,cast( if(clkns is null, 0 ,clkns) as int),5) topclk,
sum(bidns)bidns,sum(impns) impns,sum(clkns)clkns,
sum(wp)/pow(10,9) wp,
sum(bidns)/sum(reqns) bidrate,
sum(impns)/sum(bidns) winrate,
sum(clkns)/sum(impns) ctr,
sum(wp)/(sum(impns)*pow(10,6)) cpm,
sum(wp)/(sum(clkns)*pow(10,9)) cpc,
sum(bidfloor)/(pow(10,6)*sum(bidns)) avgbidfloor,
sum(bidprice)/(pow(10,6)*sum(bidns)) avgbidprice,
sum(info_bidfloor.b0) b0,
sum(info_bidfloor.b1) b1,
sum(info_bidfloor.b2) b2,
sum(info_bidfloor.b3) b3,
sum(info_bidfloor.b4) b4,
sum(info_bidfloor.b5) b5
from user_wangwentao_adx_traffic where $dd group by size 
"

cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi


##################REGION#######################
sql="
add jar allyes-hive-udf-1.4.0.jar;
create temporary function top_group as 'com.allyes.hive.udf.Top4GroupBy';

insert overwrite  directory '/user/group_dataanalysis/adx_daily_report/region'
select province,
sum(reqns) reqns,
top_group(domain,cast( if(reqns is null,0,reqns) as int),5) topreq,
top_group(domain,cast( if(clkns is null, 0 ,clkns) as int),5) topclk,
sum(bidns)bidns,sum(impns) impns,sum(clkns)clkns,
sum(wp)/pow(10,9) wp,
sum(bidns)/sum(reqns) bidrate,
sum(impns)/sum(bidns) winrate,
sum(clkns)/sum(impns) ctr,
sum(wp)/(sum(impns)*pow(10,6)) cpm,
sum(wp)/(sum(clkns)*pow(10,9)) cpc,
sum(bidfloor)/(pow(10,6)*sum(bidns)) avgbidfloor,
sum(bidprice)/(pow(10,6)*sum(bidns)) avgbidprice,
sum(info_bidfloor.b0) b0,
sum(info_bidfloor.b1) b1,
sum(info_bidfloor.b2) b2,
sum(info_bidfloor.b3) b3,
sum(info_bidfloor.b4) b4,
sum(info_bidfloor.b5) b5
from user_wangwentao_adx_traffic where $dd group by province order by reqns desc 
"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

#######################DOMAIN########################

sql="
add jar allyes-hive-udf-1.4.0.jar;
create temporary function top_group as 'com.allyes.hive.udf.Top4GroupBy';

insert overwrite  directory '/user/group_dataanalysis/adx_daily_report/domain' 
select
traffict.domain, traffict.reqns treqns,
bidns,impns,clkns,wp,bidrate,winrate,ctr,cpm,cpc,avgbidfloor,avgbidprice,b0,b1,b2,b3,b4,b5,

detectverticalt.reqns dreqns,topreq,topclk
from
(select domain,sum(reqns) reqns,
sum(bidns)bidns,sum(impns) impns,sum(clkns)clkns,
sum(wp)/pow(10,9) wp,
sum(bidns)/sum(reqns) bidrate,
sum(impns)/sum(bidns) winrate,
sum(clkns)/sum(impns) ctr,
sum(wp)/(sum(impns)*pow(10,6)) cpm,
sum(wp)/(sum(clkns)*pow(10,9)) cpc,
sum(bidfloor)/(pow(10,6)*sum(bidns)) avgbidfloor,
sum(bidprice)/(pow(10,6)*sum(bidns)) avgbidprice,
sum(info_bidfloor.b0) b0,
sum(info_bidfloor.b1) b1,
sum(info_bidfloor.b2) b2,
sum(info_bidfloor.b3) b3,
sum(info_bidfloor.b4) b4,
sum(info_bidfloor.b5) b5
from user_wangwentao_adx_traffic where $dd
group by domain order by reqns desc limit 200) traffict left outer join
(select domain,
sum(reqns) reqns,
top_group(bigcategory,cast( if(reqns is null,0,reqns) as int),5) topreq,
top_group(bigcategory,cast( if(clkns is null, 0 ,clkns) as int),5) topclk
from user_wangwentao_adx_detectvertical where $dd
group by domain )detectverticalt on traffict.domain=detectverticalt.domain order by treqns desc

"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi


#######################SMALLCATEGORY#####################

sql="
add jar allyes-hive-udf-1.4.0.jar;
create temporary function top_group as 'com.allyes.hive.udf.Top4GroupBy';

insert overwrite  directory '/user/group_dataanalysis/adx_daily_report/smallcategory' 
select
smallcategory,bigcategory,nativecode,
sum(reqns) reqns,
sum(bidns)bidns,sum(impns) impns,sum(clkns)clkns,
sum(wp)/pow(10,9) wp,
sum(bidns)/sum(reqns) bidrate,
sum(impns)/sum(bidns) winrate,
sum(clkns)/sum(impns) ctr,
sum(wp)/(sum(impns)*pow(10,6)) cpm,
sum(wp)/(sum(clkns)*pow(10,9)) cpc,
sum(bidfloor)/(pow(10,6)*sum(bidns)) avgbidfloor,
sum(bidprice)/(pow(10,6)*sum(bidns)) avgbidprice,
sum(info_category.c0) c0,
sum(info_category.c1) c1,
sum(info_category.c2) c2,
sum(info_category.c3) c3,
sum(info_category.c4) c4,
top_group(domain,cast( if(reqns is null,0,reqns) as int),5) dtopreq,
top_group(domain,cast( if(clkns is null, 0 ,clkns) as int),5) dtopclk,
top_group(province,cast( if(reqns is null, 0 ,reqns) as int),5) ptopreq
from user_wangwentao_adx_detectvertical where $dd group by smallcategory,bigcategory,nativecode
order by reqns desc
"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

#####################BIGCATEGORY#######################
sql="
add jar allyes-hive-udf-1.4.0.jar;
create temporary function top_group as 'com.allyes.hive.udf.Top4GroupBy';
insert overwrite  directory '/user/group_dataanalysis/adx_daily_report/bigcategory' 
select
bigcategory,
sum(reqns) reqns,
sum(bidns)bidns,sum(impns) impns,sum(clkns)clkns,
sum(wp)/pow(10,9) wp,
sum(bidns)/sum(reqns) bidrate,
sum(impns)/sum(bidns) winrate,
sum(clkns)/sum(impns) ctr,
sum(wp)/(sum(impns)*pow(10,6)) cpm,
sum(wp)/(sum(clkns)*pow(10,9)) cpc,
sum(bidfloor)/(pow(10,6)*sum(bidns)) avgbidfloor,
sum(bidprice)/(pow(10,6)*sum(bidns)) avgbidprice,
sum(info_category.c0) c0,
sum(info_category.c1) c1,
sum(info_category.c2) c2,
sum(info_category.c3) c3,
sum(info_category.c4) c4,
top_group(domain,cast( if(reqns is null,0,reqns) as int),5) dtopreq,
top_group(domain,cast( if(clkns is null, 0 ,clkns) as int),5) dtopclk,
top_group(province,cast( if(reqns is null, 0 ,reqns) as int),5) ptopreq
from user_wangwentao_adx_detectvertical where $dd group by bigcategory
order by reqns desc
"
cmd="hive -e \"$sql\""
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

cmd="
hadoop fs -text   /user/group_dataanalysis/adx_daily_report/size/* > /home/group_dataanalysis/wangwentao/daily_category/res/size.txt
hadoop fs -text   /user/group_dataanalysis/adx_daily_report/region/* > /home/group_dataanalysis/wangwentao/daily_category/res/region.txt
hadoop fs -text   /user/group_dataanalysis/adx_daily_report/domain/* > /home/group_dataanalysis/wangwentao/daily_category/res/domain.txt
hadoop fs -text   /user/group_dataanalysis/adx_daily_report/smallcategory/* > /home/group_dataanalysis/wangwentao/daily_category/res/smallcategory.txt
hadoop fs -text   /user/group_dataanalysis/adx_daily_report/bigcategory/* > /home/group_dataanalysis/wangwentao/daily_category/res/bigcategory.txt

"
echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi

########################SAVE2XLS#######################

cmd="python2.7 savedata2xls.py $dd_t"

echo "$cmd"
if [ $debug != "1" ]; then
  eval "$cmd"
fi
