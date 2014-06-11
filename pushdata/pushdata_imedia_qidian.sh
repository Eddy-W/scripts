create table if not exists user_cookiemapping_qidian
(data_type string,key string,value string, expiry_time string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY  '\t' ;

add jar allyes-hive-udf-1.4.0.jar;
CREATE TEMPORARY FUNCTION encodebase64 as 'com.allyes.hive.udf.EncodeBase64';
insert overwrite table  user_cookiemapping_qidian 
select '2',encodebase64( cookie),case when tag='IA' then encodebase64('135') when tag='IB' then encodebase64('136') when tag='IIA' then encodebase64('137') end,''   from 
(select allyesid cookie,split(split(extdata,'}')[0],':')[1] tag from cookie_mapping where   dd>='2014-03-10' and db='qidian' and
(split(split(extdata,'}')[0],':')[1] ='IA' or split(split(extdata,'}')[0],':')[1] ='IB' or split(split(extdata,'}')[0],':')[1] ='IIA')
group by allyesid,split(split(extdata,'}')[0],':')[1])x where encodebase64( cookie) is not null