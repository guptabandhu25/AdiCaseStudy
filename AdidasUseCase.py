# Databricks notebook source
from pyspark.sql import functions as f
df1 = spark.read.json("/FileStore/tables/adidas.json")
df1.printSchema()
spark.conf.set('spark.sql.legacy.timeParserPolicy','LEGACY')
customDF = df1.withColumn("derived_publish_date",f.when(f.col('first_publish_date').isNotNull(), f.col('first_publish_date'))
.otherwise(f.col('publish_date')))
.withColumn("harmonized_publish_date", f.when(f.date_format(f.col("derived_publish_date"), "yyyy-MM-dd").isNotNull(),
f.date_format(f.col("derived_publish_date"), "yyyy-MM-dd"))
.otherwise(f.when(f.from_unixtime(f.unix_timestamp("derived_publish_date",'MMM dd, yyyy'),'yyyy-MM-dd').isNotNull(),f.from_unixtime(f.unix_timestamp("derived_publish_date",'MMM dd, yyyy'),'yyyy-MM-dd'))
.otherwise(f.from_unixtime(f.unix_timestamp("derived_publish_date",'MMM yyyy'),'yyyy-MM-dd'))))

filterredDF = customDF
.withColumn('title',f.when(f.col('title')=="",None).otherwise(f.col('title')))
.filter(f.col('title').isNotNull()).filter(f.col('number_of_pages') > 20)
.filter(f.col('harmonized_publish_date') > '1950-01-01')
.filter(f.col('harmonized_publish_date') < '2016-01-01')

print(filterredDF.count())

explodedContributorDF = filterredDF.withColumn("Exploded",f.explode_outer(f.col("contributors")))
.withColumn('Name',f.col("Exploded.name"))
.withColumn('Role',f.col("Exploded.role"))

#explodedContributorDF.filter(f.col('Exploded').isNotNull()).select('Exploded','Name','Role').show(100)

#First book/ author which was published and the last one
#First book
explodedContributorDF
.orderBy(f.col('harmonized_publish_date')
.select('title','harmonized_publish_date','publish_date','first_publish_date','publish_country','publishers','publish_places').show(200,False)

#Last book
explodedContributorDF
.orderBy(f.col('harmonized_publish_date').desc())
.select('title','harmonized_publish_date','publish_date','first_publish_date','publish_country','publishers','publish_places').show(200,False)


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

top5GenresDF = explodedContributorDF.withColumn('genres_exploded',f.explode_outer(f.col('genres')))
top5GenresDF1 = top5GenresDF.groupBy(f.col('genres_exploded')).agg(f.count(f.col('genres_exploded')).alias('cnt'))
window = Window.partitionBy().orderBy(f.col('cnt').desc())
top5GenresDFFinal = top5GenresDF1.withColumn("Genres_Rnk",rank().over(window)).filter(f.col('Genres_Rnk') <= 5)
top5GenresDFFinal.show()

# COMMAND ----------

#explodedContributorDF.select('authors').show(20,False)
authorlengthDf = explodedContributorDF.withColumn("author_len",f.size(f.col("authors"))).select('author_len','authors').filter(f.col('author_len') >= 2)
explodedAuthorDF = authorlengthDf.withColumn('ExplodedAuthor',f.explode('authors')).withColumn('exploded_author',f.col('ExplodedAuthor.author')).withColumn('exploded_key',f.col('ExplodedAuthor.key')).withColumn('exploded_Type',f.col('ExplodedAuthor.type'))

explodedAuthorBookCountDF = explodedAuthorDF.select('ExplodedAuthor','exploded_author','exploded_key','exploded_Type').groupBy('exploded_key').agg(f.count('exploded_key').alias('author_books_count'))
window = Window.orderBy(f.col('author_books_count').desc())
explodedAuthorBookCountDF.withColumn('rnk_coAuthor',rank().over(window)).filter(f.col('rnk_coAuthor') <=5).show()

# COMMAND ----------

publishYearDF = explodedContributorDF.withColumn("pubish_year",f.date_format(f.col("harmonized_publish_date"), "Y"))
authorPerYearDF = publishYearDF.groupBy(f.col('pubish_year')).agg(f.count(f.col('role')).alias("Author_count")).filter(f.col('Author_count') > 0)

authorPerYearDF.printSchema()

# COMMAND ----------

publishYearDF.filter((f.col('pubish_year') > '1950') & (f.col('pubish_year') < '1970')).withColumn("pubish_month",f.date_format(f.col("harmonized_publish_date"), "MM-yyyy")).groupBy(f.col('pubish_month')).agg(f.count('exploded_key').alias('Author_Count_per_month'),f.count('key').alias('Book_Count_Per_Month')).show()



