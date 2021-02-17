/* Not sure what 'live to date' means because it seems the example given shows a global distinct number
   of IP addresses per customerid. But we might assume 'live to date' is something more like the number
   of distinct IP addresses until the order_datetime ( like running count), so let's do this
*/

CREATE VIEW schematransactionView AS
SELECT *,
       sum(amount_eur) over(PARTITION BY customerid
                            ORDER BY order_datetime) AS cumulativeAmount,
       count(distinct(ip_address)) over(PARTITION BY customerid
                                        ORDER BY order_datetime) AS dateToLiveIPs
FROM schematransaction
ORDER BY customerid,
         order_datetime;

SELECT * FROM schematransactionview LIMIT 20;


/* SIDE QUESTIONS

1) 

The table created in this exercice has been persisted as a textfile, thus a simple file without specific 
compression, data format. The initial table would take around 4KB

Nothing prevent us from persisting it using a specific data format ( Paquet for instance), which might be
optimized in term of size. 
PS : the query above is a view ( it wont be persisted anyway)


2)

Hadoop data fault tolerance is achieved using replication 
Files that go into hdfs cluster will be divided in blocks, each blocks will be replicated over the data 
node according to the replication factor
*/
