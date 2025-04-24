# Databricks notebook source
spark

# COMMAND ----------

stac_name = "tokiyostoragedata"
con_name = "tokiyo-olympic-data"
access_key = ""

spark.conf.set(f"fs.azure.account.key.{stac_name}.blob.core.windows.net", access_key)

oly_df = spark.read.csv(
    f"wasbs://{con_name}@{stac_name}.blob.core.windows.net/raw-data/Athletes.csv",
    header=True,
    inferSchema=True
)
oly_df.printSchema()
print("-------------------")

oly_df2 = spark.read.csv(
    f"wasbs://{con_name}@{stac_name}.blob.core.windows.net/raw-data/EntriesGender.csv",
    header=True,
    inferSchema=True
)
oly_df2.printSchema()
print("-------------------")
oly_df3 = spark.read.csv(
    f"wasbs://{con_name}@{stac_name}.blob.core.windows.net/raw-data/Coaches.csv",
    header=True,
    inferSchema=True
)
oly_df3.printSchema()
print("-------------------")
oly_df4 = spark.read.csv(
    f"wasbs://{con_name}@{stac_name}.blob.core.windows.net/raw-data/Medals.csv",
    header=True,
    inferSchema=True
)
oly_df4.printSchema()
print("-------------------")
oly_df5 = spark.read.csv(
    f"wasbs://{con_name}@{stac_name}.blob.core.windows.net/raw-data/Teams.csv",
    header=True,
    inferSchema=True
)
oly_df5.printSchema()
print("-------------------")
#oly_df.show()


# COMMAND ----------

from pyspark.sql.functions import split,col,when,upper 
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import fuzzy_match

oly_df2 = spark.read.csv(
    f"wasbs://{con_name}@{stac_name}.blob.core.windows.net/raw-data/EntriesGender.csv",
    header=True,
    inferSchema=True
)
oly_df2.printSchema()

#convert dataframe to rdd
oly_rdd2 = oly_df2.rdd

#apply map transformation to the rdd
#create one coulumn rdd
#mapped_rdd2 = oly_rdd2.map(lambda x: (x[0], x[1], x[2], x[3]))
mapped_rdd2 = oly_rdd2.map(lambda x: x[0])
mapped_result = mapped_rdd2.collect()
print(mapped_result)

# If you want to convert the RDD back to a DataFrame
# You need to define a schema for the new DataFrame
from pyspark.sql import Row

new_df = mapped_rdd2.map(lambda x: Row(column1=x)).toDF()
#new_df.printSchema()
trans_df = new_df.withColumnRenamed("column1", "Discipline")
display(trans_df)
#filtered_df = trans_df.filter(trans_df.Discipline.startswith("A"))

#split the column data

split_df = trans_df.withColumn("Dis_part1", split(col("Discipline"), " ").getItem(0)) \
                   .withColumn("Dis_part2", when(split(col("Discipline"), " ").getItem(1).isNull(), "N/A")
                               .otherwise(split(col("Discipline"), " ").getItem(1)))

display(split_df)
#uppercase
upper_split_first_p = split_df.withColumn("Dis_part1",upper(col("Dis_part1")))
display(upper_split_first_p)
                   
#write operation
upper_split_first_p.write.csv( f"wasbs://{con_name}@{stac_name}.blob.core.windows.net/transformed-data/TransformedDiscipline.csv",
    header=True,
    mode="overwrite")





# COMMAND ----------

from pyspark.sql.functions import avg

oly_df2 = spark.read.csv(
    f"wasbs://{con_name}@{stac_name}.blob.core.windows.net/raw-data/EntriesGender.csv",
    header=True,
    inferSchema=True
)
oly_df2.printSchema()
oly_df4 = spark.read.csv(
    f"wasbs://{con_name}@{stac_name}.blob.core.windows.net/raw-data/Medals.csv",
    header=True,
    inferSchema=True
)
oly_df4.printSchema()

# inner join
inner_join_df = oly_df2.join(oly_df4, oly_df2.Discipline == oly_df4.Discipline, "inner")
#display(inner_join_df)
# Remove duplicate common join column
result_df = inner_join_df.select(oly_df2["*"], oly_df4.drop(oly_df4.Discipline)["*"])
result_df.printSchema()
display(result_df)

#groupby and aggregiate
groupby_df = result_df.groupBy("Discipline").agg(avg("Total").alias("Avg_by_dis"))
display(groupby_df)

# left join 

left_join_df = oly_df2.join(oly_df4,oly_df2.Discipline == oly_df4.Discipline,"left")

result_left_join_df = left_join_df.select(oly_df2["*"], oly_df4.drop(oly_df4.Discipline)["*"])
display(result_left_join_df)

#Right join

right_join_df = oly_df2.join(oly_df4,oly_df2.Discipline == oly_df4.Discipline,"right")
result_right_join_df = right_join_df.select(oly_df2["*"], oly_df4.drop(oly_df4.Discipline)["*"])
display(result_right_join_df)

#Full outer join
full_outer_join_df = oly_df2.join(oly_df4,oly_df2.Discipline == oly_df4.Discipline,"outer")
result_full_outer_join_df = full_outer_join_df.select(oly_df2["*"],oly_df4.drop(oly_df4.Discipline)["*"])

display(result_full_outer_join_df)






# COMMAND ----------

