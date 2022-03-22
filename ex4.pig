RAW_DATA = LOAD 'hdfs://huutrac-master:54310/Pig_Data/100linee.txt' AS (ts:long, sport, dport, sip, dip,l3proto, l4proto, flags,phypkt, netpkt, overhead,phybyte, netbyte:long);

DATA = FOREACH RAW_DATA GENERATE sip, netbyte;

DATA_UP = GROUP DATA BY sip;
FLOW_UP = FOREACH DATA_UP GENERATE group as IP, SUM(DATA.netbyte) as upload;

SUMMARY_SORTED = ORDER FLOW_UP BY upload DESC;
SUMMARY_TOP100 = LIMIT SUMMARY_SORTED 100;

STORE SUMMARY_TOP100 INTO 'output/ex4';