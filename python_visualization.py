import io
import boto3
import pandas as pd
import s3fs 
from matplotlib import pyplot as plt

s3 = boto3.resource('s3')


df = pd.read_csv('cleaned_yelp_academic_dataset.csv', lines=True)

#dropping restaurants in states with just one restaurant
df.loc[df['categories'].str.contains('Restaurant',case=False,na=False),'is_restaurant'] = 'Yes'
df.loc[~df['categories'].str.contains('Restaurant',case=False,na=False),'is_restaurant'] = 'No'
restaurant_df = df[df['is_restaurant']=='Yes']
restaurant_df = restaurant_df[~(restaurant_df.state.isin(['XMS','MT','NC','HI','CO']))]

#average reviews over time
restaurant_df['review_year'] = restaurant_df[review_date].dt.year
restaurant_rating_over_time = restaurant_df.groupby(['stars','review_year']).reset_index().round(2)

is_restaurant = df['is_restaurant'].value_counts().reset_index().rename(columns={'index':'is_restaurant','is_restaurant':'count'})
is_restaurant
fig1 = plt.figure()
plt.barh(is_restaurant['is_restaurant'],is_restaurant['count'])
plt.title("Number of Restaurants in Dataset")

# Create a buffer to hold the figure
img_data = io.BytesIO()
# Write the figure to the buffer
plt.savefig(img_data, format='png', bbox_inches='tight')
img_data.seek(0)
# Connect to the s3fs file system
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('s3://project-data-hkl/restaurant_count.png', 'wb') as f:
  f.write(img_data.getbuffer())

######
avg_stars = restaurant_df.groupby(['state',])['stars'].mean().round(decimals=2).reset_index(name='average')
avg_stars = avg_stars.sort_values(by='average',ascending=False)
avg_stars

# Create a buffer to hold the figure
img_data = io.BytesIO()
# Write the figure to the buffer
plt.savefig(img_data, format='png', bbox_inches='tight')
img_data.seek(0)
# Connect to the s3fs file system
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('s3://project-data-hkl/average_restaurant_review_state.png', 'wb') as f:
  f.write(img_data.getbuffer())

ca_rest_df = restaurant_df[restaurant_df['state']=='CA']
restaurant_review_ct = ca_rest_df.groupby(['state','stars'])['business_id'].count().reset_index(name='count')
restaurant_review_ct

fig2 = plt.figure(figsize=(9,6))
plt.bar(restaurant_review_ct['stars'],restaurant_review_ct['count'],width=.4)
plt.title("Most Frequent Star Ratings in California")

# Create a buffer to hold the figure
img_data = io.BytesIO()
# Write the figure to the buffer
plt.savefig(img_data, format='png', bbox_inches='tight')
img_data.seek(0)
# Connect to the s3fs file system
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('s3://project-data-hkl/frequent_stars_rating_ca.png', 'wb') as f:
  f.write(img_data.getbuffer())

#Let's say a good indicator of a restaurant is above 4.0 stars! What city has the most of these restaurants?
four_star_ca_restaurants = ca_rest_df[ca_rest_df['stars']>= 4.0]

four_star_ca_restaurants_cnt = four_star_ca_restaurants.groupby(['city'])['stars'].count().reset_index(name='count')
four_star_ca_restaurants_cnt = four_star_ca_restaurants_cnt[four_star_ca_restaurants_cnt['count']!=1]
four_star_ca_restaurants_cnt

fig2 = plt.figure(figsize=(9,6))
plt.bar(four_star_ca_restaurants_cnt['city'],four_star_ca_restaurants_cnt['count'],width=.4)
plt.title("California Cities with Best Reviewed Restaurants")
plt.show()

# Create a buffer to hold the figure
img_data = io.BytesIO()
# Write the figure to the buffer
plt.savefig(img_data, format='png', bbox_inches='tight')
img_data.seek(0)
# Connect to the s3fs file system
s3 = s3fs.S3FileSystem(anon=False)
with s3.open('s3://project-data-hkl/ca_best_reviewed.png', 'wb') as f:
  f.write(img_data.getbuffer())
