"""
SIDE QUESTIONS

1)

As join in distributed cluster require shuffling the data ( move the data to different nodes, same partition, same node),
broadcast is used here to share the small dataframe (dim_user) to all nodes in the cluster.
That way, the overall data wont have to be shuffled because each node will have its own copy of the dataframe.


2)

First, would be nice to get rid of udf because they are hard/impossible to optimize ( the optimizer is not supposed
to know what the udf does)

The code would then look like this:

"""

from pyspark.sql.functions import size

schemaTransaction\
.join(F.broadcast(dim_user), on="customerid", how="full")\
.groupby("customerid").agg(
F.min("amount_eur").alias("min_amount_eur"),
F.max("amount_eur").alias("max_amount_eur"),
size(F.collect_list("order_date")).alias("nb_purchases")
).show()

"""

Actually, when we think about it, there is no need to do a join here for this specific feature

So the code would then be:

"""

schemaTransaction\
.groupby("customerid").agg(
F.min("amount_eur").alias("min_amount_eur"),
F.max("amount_eur").alias("max_amount_eur"),
size(F.collect_list("order_date")).alias("nb_purchases")
).show()

