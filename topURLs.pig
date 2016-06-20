raw = LOAD 'dataset/web-logs.csv' using PigStorage(',') AS (server_ts:chararray,client_ts:chararray,ip:chararray,visitor_id:chararray,session_id:chararray,location:chararray,referer:chararray,agent:chararray);
grouped = GROUP raw by location;
counts = FOREACH grouped  GENERATE group as URL, COUNT(raw) as count;
ordered_counts = order counts by count desc;
result = limit ordered_counts 25;
dump result;
