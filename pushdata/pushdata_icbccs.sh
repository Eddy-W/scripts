#!/bin/bash

dirname=`dirname $0`
cd $dirname


cmdx="crontab -e"
cmdx="
0 8 * * * source /home/group_dataanalysis/.bash_profile;/home/group_dataanalysis/wangwentao/pushwinmax/pushdata_icbccs.sh >> /home/group_dataanalysis/wangwentao/pushwinmax/out.log 2>>/home/group_dataanalysis/wangwentao/pushwinmax/out.log
"


tablename="user_wangwentao_icbccs_"$(date +%Y%m%d)
echo $tablename


createsql="
create table if not exists $tablename
(cookie string,tag string,content string, ts1 string, ts2 string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' create table if not exists $tablename
(cookie string,tag string,content string, ts1 string, ts2 string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' 
"
cmd="hive -e \"$createsql\""
echo "$cmd"
eval "$cmd"

 
dd=$(date -d "$1 - 1 day" "+%Y-%m-%d") 

sql="
insert overwrite table $tablename 
select cookie,tag,content,startts,stopts from
(select distinct  sawlog_.rawlog_.allyesid_ cookie,'MTR8NTh8VC0wMDAwNTgtMDF8Y2xpY2s=' tag,'' content,'20140303000000' startts,'20140425000000' stopts
from pb_clicklog
where product='ifc' and db='ifc' and dd='$dd' 
and sawlog_.advertiserid_='58'
and sawlog_.rawlog_.allyesid_ rlike '^[\\x21\\x23-\\x2B\\x2D-\\x3A\\x3C-\\x5B\\x5D-\\x7E]+$' 
union all
select distinct a.cookie cookie, 'MTR8NTh8VC0wMDAwNTgtMDF8RmluYW5jZQ==' tag,'' content,'20140303000000' startts,'20140425000000' stopts from
(select sawlog_.rawlog_.allyesid_ cookie from pb_clicklog where dd='$dd' and db='mso' and product='idigger' and sawlog_.vsolutionid_='178775')a join
(select distinct rawlog_.allyesid_ cookie from pb_idigger_tracklog where dd='$dd'  and
db='ifc' and product='idigger' and sitecode_='T-000058-01')b on a.cookie=b.cookie where a.cookie rlike '^[\\x21\\x23-\\x2B\\x2D-\\x3A\\x3C-\\x5B\\x5D-\\x7E]+$'
union all

select distinct sawlog_.rawlog_.allyesid_ cookie,'MTR8NTh8VC0wMDAwNTgtMDF8ZmluYW5jZWNsaWNr' tag,'' content,'20140313000000' startts,'20140425000000' stopts
 from pb_clicklog where dd='$dd' and db='mso' and product='idigger' and sawlog_.vsolutionid_='178775' 
and sawlog_.rawlog_.allyesid_ rlike '^[\\x21\\x23-\\x2B\\x2D-\\x3A\\x3C-\\x5B\\x5D-\\x7E]+$' 
)x


"

cmd="hive -e \"$sql\""
echo "$cmd"
eval "$cmd"

cmd="/home/targeting/ifc/retarget/update/updatepathlist.sh  /user/group_dataanalysis/warehouse/$tablename"
echo "$cmd"
eval "$cmd"

