add jar hivemall.jar;
source define-all.hive;

Create external table user_wangwentao_a9atrain (
  rowid int,
  label float,
  features ARRAY<STRING>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/user/group_dataanalysis/testdata/a9atrain';

Create external table user_wangwentao_a9atest (
  rowid int, 
  label float,
  features ARRAY<STRING>
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' COLLECTION ITEMS TERMINATED BY "," STORED AS TEXTFILE LOCATION '/user/group_dataanalysis/testdata/a9atest';

create table user_wangwentao_a9atrain_exploded as
select 
  rowid,
  label, 
  cast(split(feature,":")[0] as int) feature,
  cast(split(feature,":")[1] as float) as value
from 
  user_wangwentao_a9atrain LATERAL VIEW explode(addBias(features)) t AS feature;

create table user_wangwentao_a9atest_exploded as
select 
  rowid,
  label,
  cast(split(feature,":")[0] as int) as feature,
  cast(split(feature,":")[1] as float) as value
from 
  user_wangwentao_a9atest LATERAL VIEW explode(addBias(features)) t AS feature;


add jar hivemall.jar;
source define-all.hive;
set hivevar:total_steps=32561;
set hivevar:num_test_instances=16281;
create table user_wangwentao_a9a_model1 
as
select 
 cast(feature as int) as feature,
 cast(avg(weight) as float) as weight
from 
 (
select logress(addBias(features),label,"-total_steps ${total_steps}") as (feature,weight)
from user_wangwentao_a9atrain
 ) t  group by feature;
 
 
 
 create or replace view user_wangwentao_a9a_predict1 
as
select
  t.rowid, 
  sigmoid(sum(m.weight)) as prob,
  sum(m.weight) as total_weight,
  CAST((case when sum(m.weight) > 0.0 then 1.0 else 0.0 end) as FLOAT) as label
from 
  user_wangwentao_a9atest_exploded t LEFT OUTER JOIN
  user_wangwentao_a9a_model1 m ON (t.feature = m.feature)
group by
  t.rowid;
  
  
  
create or replace view user_wangwentao_a9a_submit1 as
select 
  t.label as actual, 
  pd.label as predicted, 
  pd.prob as probability
from 
  user_wangwentao_a9atest t JOIN user_wangwentao_a9a_predict1 pd 
    on (t.rowid = pd.rowid);

  
select count(1) / ${num_test_instances} from user_wangwentao_a9a_submit1 
where actual == predicted;