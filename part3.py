# Some dates in the sample are not healthy ( hour > 23) and thus lead to some null values
# I'm not handling those cases ( might have a value for amountLast10MinsExclusive when we
# dont expect one). One solution would be to sanitize the data first instead of handling
# this here

from pyspark.sql.functions import col, when, coalesce, lit, sum
from pyspark.sql.window import Window

w = (
    Window.partitionBy("customerid")
    .orderBy(col("order_datetime").cast("long"))
    .rangeBetween(-60 * 10, -1)
)
amount_cumul = coalesce(sum("amount_eur").over(w), lit(0)).alias(
    "amountLast10MinsExclusive"
)
schemaTransaction.withColumn(
    "order_datetime", col("order_datetime").cast("timestamp")
).select("*", amount_cumul).orderBy("order_datetime").show()


"""
SIDE QUESTIONS

1)

It depends on the kind of queries we want to do on this dataframe.
If lot of analytical aggregations, we could save schemaTransaction as parquet file ( columnar),
space and time might be optimized. Schema should be preserved as well


==> schemaTransaction.write.parquet("path/schemaTransaction.parquet")

==> val df = sqlContext.read.parquet("path/schemaTransaction.parquet")

"""
