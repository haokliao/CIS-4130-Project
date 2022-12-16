pip install matplotlib
pip install pandas
pip install s3fs
# pip install boto3

pyspark

#############################################

from pyspark.sql.functions import *
import io
import pandas as pd
import s3fs 
from matplotlib import pyplot as plt

# Yelp Reviews.  
# Replace reviews_filename with S3 bucket reference such as s3://project-data-hkl/yelp_academic_dataset_review.json
reviews_filename = "s3://project-data-hkl/yelp_academic_dataset_review.json"
reviews_sdf = spark.read.json(reviews_filename)
# Rename the stars with review_stars
reviews_sdf = reviews_sdf.withColumnRenamed('stars', 'review_stars')
# Rename the 'date' column with 'review_date'
reviews_sdf = reviews_sdf.withColumnRenamed('date', 'review_date')
# Convert review_date to an actual date data type
reviews_sdf = reviews_sdf.withColumn("review_date", to_date(to_timestamp(col("review_date"), "yyyy-MM-dd HH:mm:ss" )) )
# Get the age of the review in days
reviews_sdf = reviews_sdf.withColumn("review_age_days", datediff(current_date(),col("review_date")) )
reviews_sdf = reviews_sdf.withColumn("review_age_years", col("review_age_days")/365.0 )
reviews_sdf = reviews_sdf.withColumn("review_year", year("review_date") )

# Likely don't need the review_id
reviews_sdf = reviews_sdf.drop('review_id')

# Create an indicator variable if the star rating is more than 3.0
reviews_sdf = reviews_sdf.withColumn("goodreview", when(col("review_stars") > 3.0, 1.0).otherwise(0.0))

############################################
# Yelp Businesses
business_filename = "s3://project-data-hkl/yelp_academic_dataset_business.json"
business_sdf = spark.read.json(business_filename)
business_sdf.printSchema()
# Rename the business "stars"
business_sdf = business_sdf.withColumnRenamed('stars', 'business_stars')
business_sdf.select("business_id", "name", "categories", "attributes.Alcohol", "attributes.NoiseLevel").show()

# Grab some columns we likely want to explore
business_sdf = business_sdf.withColumn("alcohol", business_sdf.attributes.Alcohol)

# Drop some columns we likely don't need
business_sdf = business_sdf.drop('city','hours','is_open','latitude','longitude','postal_code','state','address','attributes')

# Fill in missing / null values
business_sdf = business_sdf.withColumn('alcohol', when(lower(business_sdf.alcohol) == 'none', None).otherwise(business_sdf.alcohol))
business_sdf = business_sdf.withColumn('alcohol', when(lower(business_sdf.alcohol) == "'none'", None).otherwise(business_sdf.alcohol))
business_sdf = business_sdf.withColumn('alcohol', when(lower(business_sdf.alcohol) == u'none', None).otherwise(business_sdf.alcohol))
business_sdf = business_sdf.withColumn('alcohol', when(lower(business_sdf.alcohol) == "u'none'", None).otherwise(business_sdf.alcohol))
business_sdf = business_sdf.withColumn('alcohol', when(lower(business_sdf.alcohol) == "'beer_and_wine'", "beer_and_wine").otherwise(business_sdf.alcohol))
business_sdf = business_sdf.withColumn('alcohol', when(lower(business_sdf.alcohol) == "u'beer_and_wine'", "beer_and_wine").otherwise(business_sdf.alcohol))
business_sdf = business_sdf.withColumn('alcohol', when(lower(business_sdf.alcohol) == "'full_bar'", "full_bar").otherwise(business_sdf.alcohol))
business_sdf = business_sdf.withColumn('alcohol', when(lower(business_sdf.alcohol) == "u'full_bar'", "full_bar").otherwise(business_sdf.alcohol))

# Join business with reviews
yelp_sdf = reviews_sdf.join(business_sdf, "business_id")
yelp_sdf.printSchema()

# root
#  |-- business_id: string (nullable = true)
#  |-- cool: long (nullable = true)
#  |-- review_date: date (nullable = true)
#  |-- funny: long (nullable = true)
#  |-- review_stars: double (nullable = true)
#  |-- text: string (nullable = true)
#  |-- useful: long (nullable = true)
#  |-- user_id: string (nullable = true)
#  |-- review_age_days: integer (nullable = true)
#  |-- review_age_years: double (nullable = true)
#  |-- goodreview: double (nullable = false)
#  |-- categories: string (nullable = true)
#  |-- name: string (nullable = true)
#  |-- review_count: long (nullable = true)
#  |-- business_stars: double (nullable = true)
#  |-- alcohol: string (nullable = true)


#Reviews in the last 10 years
year_count_df = yelp_sdf.where(col("review_year") > 2012).groupby('review_year').count().sort('review_year').toPandas()
fig = plt.figure()

plt.bar(year_count_df['review_year'], year_count_df['count'])
plt.xlabel("Year")
plt.ylabel("Number of Reviews")
plt.title("Number of Reviews after 2012 by Year")
plt.xticks(rotation=90, ha='right')
fig.tight_layout()

review_count_by_year = io.BytesIO()
plt.savefig(review_count_by_year, format='png', bbox_inches='tight')
review_count_by_year.seek(0)
# Connect to the s3fs file system
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('s3://project-data-hkl/review_count_by_year.png', 'wb') as f:
  f.write(review_count_by_year.getbuffer())


# Look at Total Star ratings
star_count_df = yelp_sdf.groupBy("review_stars").count().sort("review_stars").toPandas()
fig = plt.figure()
plt.ticklabel_format(useOffset=False, style='plain', axis='y')
plt.bar(star_count_df['review_stars'],star_count_df['count'] )
# fig.tight_layout()
plt.title("Review Count by Star Rating")
star_rating_count = io.BytesIO()
plt.savefig(star_rating_count, format='png', bbox_inches='tight')
star_rating_count.seek(0)
# Connect to the s3fs file system
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('s3://project-data-hkl/star_rating_count.png', 'wb') as f:
  f.write(star_rating_count.getbuffer())

# Good restaurants that serve alcohol?
alcohol_df = yelp_sdf.where(col("goodreview") != '0').groupby('alcohol').count().sort('alcohol').toPandas()
alcohol_df['alcohol'].replace({None:'None'},inplace=True)
fig = plt.figure()
plt.ticklabel_format(useOffset=False, style='plain', axis='y')
plt.bar(alcohol_df['alcohol'], alcohol_df['count'])
plt.xlabel("Alcohol Service")
plt.ylabel("Count")
plt.title("Number of Restaurants with Good Reviews (>3.0) That Serve Alcohol?")

good_review = io.BytesIO()
plt.savefig(good_review, format='png', bbox_inches='tight')
good_review.seek(0)
# Connect to the s3fs file system
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('s3://project-data-hkl/good_review_alcohol.png', 'wb') as f:
  f.write(good_review.getbuffer())





