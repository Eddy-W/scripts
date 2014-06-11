#!/bin/bash

dirname=`dirname $0`
cd $dirname


cmdx="crontab -e"
cmdx="
0 15 * * * source /home/group_dataanalysis/.bash_profile;/home/group_dataanalysis/wangwentao/pushwinmax/pushdata_mediamaxgame.sh >> /home/group_dataanalysis/wangwentao/pushwinmax/out.log 2>>/home/group_dataanalysis/wangwentao/pushwinmax/out.log
"



dd=$(date -d "$1 - 1 day" "+%Y-%m-%d")
dd_t=$(date -d "$1 - 1 day" "+%Y%m%d") 

if [ -n "$1" ]; then
  dd=$1
  dd_t=$(date -d "$1 +0 day" "+%Y%m%d") 
fi

tablename="user_wangwentao_mediamaxgame_"$dd_t



createsql="
create table if not exists $tablename
(cookie string,tag string,content string, ts1 string, ts2 string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' 
"
cmd="hive -e \"$createsql\""
echo "$cmd"
eval "$cmd"



sql="
insert overwrite table user_wangzhen_game_channel_impression partition( dd='$dd' )     

select distinct cookie from
(select adspaceid,allyesid cookie from ssp_hive_db.factclicksandshows where logtype=11 and  (linux_time-substr(impressionid,1,10))<=300  
and logdate is not null and adspaceid is not null and dt ='$dd_t' )a join
(select adspaceid,channelid,channelname,medianame from ssp_hive_db.ssp_dimmedia
where find_in_set(channelid,'652,282,67,114,136,154')>0)b on a.adspaceid=b.adspaceid;

"
cmd="hive -e \"$sql\""
echo "$cmd"
eval "$cmd"


sql="
insert overwrite table user_wangzhen_game_creative_click partition( dd='$dd' )     

select distinct cookie from
(select creativeid,allyesid cookie from ssp_hive_db.factclicksandshows where logtype=9 
and distribution=0 and logdate is not null and adspaceid is not null and
dt ='$dd_t')a join
(select creativeid from user_wangwentao_creativetag)b on a.creativeid=b.creativeid;

"
cmd="hive -e \"$sql\""
echo "$cmd"
eval "$cmd"



 

sql="
insert overwrite table $tablename select  distinct  cookie,'MTR8NjB8VC0wMDAwNjAtMDF8U1NQVHJhY2s=','',
'20140228000000','20140530000000' from 
(select cookie from user_wangzhen_game_channel_impression where dd='$dd'
union all
select cookie from user_wangzhen_game_creative_click where dd= '$dd')x
where cookie rlike '^[\\x21\\x23-\\x2B\\x2D-\\x3A\\x3C-\\x5B\\x5D-\\x7E]+$';

"

cmd="hive -e \"$sql\""
echo "$cmd"
eval "$cmd"

cmd="/home/targeting/ifc/retarget/update/updatepathlist.sh  /user/group_dataanalysis/warehouse/$tablename"
echo "$cmd"
eval "$cmd"

