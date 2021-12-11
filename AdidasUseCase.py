# Databricks notebook source
# 1. Load the data and print/make yourself familiar with the schema. Count the rows in the (raw) data set.
from pyspark.sql import functions as f
df1 = spark.read.json("/FileStore/tables/adidas.json")
df1.printSchema()
df1.show()
# No of rows in raw data: 148163
# 2. Apply some data profiling on the data set and suggest three data quality measures to improve the data set for the purpose of this case study
from pyspark.sql import functions as f
from pandas_profiling import ProfileReport
df2 = df1.select('bio','birth_date','by_statement','copyright_date','death_date','description','edition_name','first_publish_date','first_sentence','full_title','fuller_name','ia_loaded_id','key','location','name','notes','ocaid','pagination','personal_name','physical_dimensions','physical_format','publish_country','publish_date','subtitle','title','title_prefix','website','weight')
#display(df2)

prof = ProfileReport(df2.toPandas(), title='Pandas Profiling Report', explorative=True)
df1.select('first_publish_date').distinct().show(1000,False)
df1.select('publish_date').distinct().show(1000,False)
dfnull=df1.filter(f.col('title').isNull()).select('title')
display(dfnull)
'''
Data Profiling measures
 1) Issue: Data in date fields such as publish_date , first_publish_date is not harmonized: 
    Resolution: Different type of format need to be harmonized to one common format ex.(yyyy-MM-dd)
 2) Issue: Title column is having null values.
    Resolution: Filter the not null records and process it.
 3) Issue: Few rows have publish_date field null but having value in first_publish_date field or vice-versa.
    Resolution: Create a column with publish_date value and substitute the value as first_publish_date where publish_date is Null.
'''

''' 
3)Make sure your data set is cleaned, so we for example don't include in results with empty/null "titles" and "number of pages" is
greater than 20 and "publishing year" is after 1950. If you decided to add additional filters due to the data profiling exercise
above, pls. state them as well. Count the rows in the filtered dataset.
'''
'''
Additional filters:
 A) Few rows have publish_date field null but having value in first_publish_date field or vice-versa.
Created a column with publish_date value and substituted the value as first_publish_date where publish_date is Null.
 b) Added a check on harmonized date to be less than 2016-01-01 since some of the date values are incorrect after this date & giving absurd result.
 c) Harmonized the data in date field as the datatype of format - 'MMM dd, yyyy' & 'MMM yyyy' is converted to 'yyyy-MM-dd'
'''
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
# No of records in filtered data : 77328

'''
4. Run the following queries on the cleaned dataset:
▪ Get the first book / author which was published - and the last one.
'''
explodedContributorDF = filterredDF.withColumn("Exploded",f.explode_outer(f.col("contributors")))
.withColumn('Name',f.col("Exploded.name"))
.withColumn('Role',f.col("Exploded.role"))

#First book/ author which was published and the last one
'''
The dataset fields didn't gave a single book which was first published but returns a group of books.
'''
#First book
explodedContributorDF
.orderBy(f.col('harmonized_publish_date')
.select('title','harmonized_publish_date','publish_date','first_publish_date','publish_country','publishers','publish_places').show(200,False)

#Last book
'''
The Last book is with a title LOVE CHANGES EVERYTHING
'''
explodedContributorDF
.orderBy(f.col('harmonized_publish_date').desc())
.select('title','harmonized_publish_date','publish_date','first_publish_date','publish_country','publishers','publish_places').show(200,False)

         
'''
4. Run the following queries on the cleaned dataset:
▪ Find the top 5 genres with most published books.
'''

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

top5GenresDF = explodedContributorDF.withColumn('genres_exploded',f.explode_outer(f.col('genres')))
top5GenresDF1 = top5GenresDF.groupBy(f.col('genres_exploded')).agg(f.count(f.col('genres_exploded')).alias('cnt'))
window = Window.partitionBy().orderBy(f.col('cnt').desc())
top5GenresDFFinal = top5GenresDF1.withColumn("Genres_Rnk",rank().over(window)).filter(f.col('Genres_Rnk') <= 5)
top5GenresDFFinal.show()

'''
4. Run the following queries on the cleaned dataset:
▪ Retrieve the top 5 authors who (co-)authored the most books.
'''
authorlengthDf = explodedContributorDF.withColumn("author_len",f.size(f.col("authors"))).select('author_len','authors').filter(f.col('author_len') >= 2)
explodedAuthorDF = authorlengthDf.withColumn('ExplodedAuthor',f.explode('authors')).withColumn('exploded_author',f.col('ExplodedAuthor.author')).withColumn('exploded_key',f.col('ExplodedAuthor.key')).withColumn('exploded_Type',f.col('ExplodedAuthor.type'))

explodedAuthorBookCountDF = explodedAuthorDF.select('ExplodedAuthor','exploded_author','exploded_key','exploded_Type').groupBy('exploded_key').agg(f.count('exploded_key').alias('author_books_count'))
window = Window.orderBy(f.col('author_books_count').desc())
explodedAuthorBookCountDF.withColumn('rnk_coAuthor',rank().over(window)).filter(f.col('rnk_coAuthor') <=5).show()

'''
4. Run the following queries on the cleaned dataset:
▪ Per publish year, get the number of authors that published at least one book
'''

publishYearDF = explodedContributorDF.withColumn("pubish_year",f.date_format(f.col("harmonized_publish_date"), "Y"))
authorPerYearDF = publishYearDF.groupBy(f.col('pubish_year')).agg(f.count(f.col('role')).alias("Author_count")).filter(f.col('Author_count') > 0)

'''
4. Run the following queries on the cleaned dataset:
▪ Find the number of authors and number of books published per month for years between 1950 and 1970!
'''

publishMonth=publishYearDF.filter((f.col('pubish_year') > '1950') & (f.col('pubish_year') < '1970'))

booksPublishedPerMonthDF = publishMonth.withColumn("pubish_month",f.date_format(f.col("harmonized_publish_date"), "MM-yyyy")).groupBy(f.col('pubish_month'))
         .agg(f.count('exploded_key').alias('Author_Count_per_month'),f.count('key').alias('Book_Count_Per_Month')).show()


