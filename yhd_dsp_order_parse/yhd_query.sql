#order_info

select
rawlog_.allyesid_ cookie, 
from_unixtime(rawlog_.timestamp_) ts,
split(parse_url(rawlog_.requesturl_,'QUERY','ecm'),'%60')[2] orderid,
split(parse_url(rawlog_.requesturl_,'QUERY','ecm'),'%60')[3] ordervalue
from pb_idigger_tracklog_partial where
sitecode_= 'T-000049-01' and dd='2014-01-23' and product='idigger'  
and  db='ifc'  and length(split(parse_url(rawlog_.requesturl_,'QUERY','ecm'),'%60')[2])>0 order by orderid,ts

 
#winmax
db="ifc"
product='ifc'


 create table if not exists $sidbid_bid_table as
 select sid,bid,count(*) bidn, count(wintag) winn,sum(winprice)/pow(10,6)  ws, count(clktag) clk
 from 
 ()cx group by sid,bid;
 
 create table if not exists $sidbid_bid_table as
 select ca.impid, ca.sid, ca.bid, ca.bidprice,
 if(cb.winprice is null,0,cb.winprice) winprice,cb.wintag,cc.clktag
 from
 (
 select rawlog_.impressionid_ impid,
 rawlog_.allyesid_ cookie,
 vsolutionid_ sid, vbannerid_ bid,
 rawlog_.exchangeid_ exid,
 ifcbids_.pricebid_   bidprice
 from  pb_ifc_bidlog where dd='$dd' and ifcbids_.pricebid_>0  and advertiserid_=49  ) ca
 left outer join (
 select sawlog_.rawlog_.impressionid_ impid,
 sawlog_.rawlog_.allyesid_ cookie,
 vsolutionid_ sid, vbannerid_ bid,
 sawlog_.rawlog_.sellingprice_ winprice, '1' wintag
 from pb_showlog where dd='$dd' and product='$product' and db='$db'
 )cb on (ca.impid=cb.impid and ca.sid=cb.sid and ca.bid=cb.bid and ca.cookie=cb.cookie)
 left  outer join
 (
 select sawlog_.rawlog_.impressionid_ impid, sawlog_.rawlog_.allyesid_ cookie,
 
 sawlog_.rawlog_.bannerid_ bid,sawlog_.rawlog_.solutionid_ sid, '1' clktag
 from pb_clicklog where dd='$dd' and product='$product' and db='$db'
 )cc on (ca.impid=cc.impid and ca.sid=cc.sid and ca.bid=cc.bid)
 
 
 
 
 create table if not exists $order_table as 
select cookie,ts,bid,sid,orderid,ordervalue
from
(

(select sawlog_.rawlog_.allyesid_ cookie,
 from_unixtime(sawlog_.rawlog_.timestamp_) ts,
sawlog_.rawlog_.bannerid_ bid,sawlog_.rawlog_.solutionid_ sid,'',''
from pb_clicklog where dd<='$dd' and dd>='$dd_pre' and product='$product' and db='$db')ct join on
(
select distinct rawlog_.allyesid_ cookie
from pb_idigger_tracklog where
sitecode_= 'T-000049-01' and dd='2014-01-23' and product='idigger' 
and  db='ifc' and   length(ecominfo_.transaction_.id_)>0
)ot on ct.cookie=ot.cookie

union all

select distinct rawlog_.allyesid_ cookie, 
 from_unixtime(rawlog_.timestamp_) ts,'','',
   ecominfo_.transaction_.id_ orderid,
   ecominfo_.transaction_.value_ ordervalue
from pb_idigger_tracklog where
sitecode_= 'T-000049-01' and dd='2014-01-23' and product='idigger' 
and  db='ifc' and   length(ecominfo_.transaction_.id_)>0) order by cookie,ts


 
 
 
 
 
 
 
 

