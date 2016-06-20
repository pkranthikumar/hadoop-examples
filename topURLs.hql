create external table if not exists clicks(server_ts string,client_ts string,ip string,visitor_id string,session_id string,location string,referer string,user_agent string) row format delimited fields terminated by ',' location '/user/cloudera/dataset/web-logs.csv';
select location,count(1) as count from clicks group by location order by count desc  limit 10;
