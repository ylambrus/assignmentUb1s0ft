# Not sure if possible to do that without udf.. So I'll use one, the optimizer
# might not be happy : p


from pyspark.sql.functions import col, countDistinct, when, udf

@udf("int")
def distinct_length_udf(s):
  return len(set(s.replace(" ",""))) # skipping spaces

  schemaTransaction.withColumn(
      "distinctAddressCharsCount", distinct_length_udf(col("address"))
  ).withColumn(
      "isParisStreet",
      when(schemaTransaction.address.endswith("rue de Paris"), "TRUE").otherwise("FALSE"),
  ).select(
      "*"
  ).show()



"""
SIDE QUESTIONS

1)

RDD abstraction does not provide query optimisation ( catalyst , tungsten)

"""
