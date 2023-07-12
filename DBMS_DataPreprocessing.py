import pandas as pd
import numpy as np
import sys

# Accept command line inputs for data file path
file1 = sys.argv[1]
file2 = sys.argv[2]

# Load CSV file into a DataFrame
df1 = pd.read_csv(file1)

# Create a new column with the sum of col1 and col2
df1 = df1.assign(total_nights=df1['stays_in_weekend_nights'] + df1['stays_in_week_nights'])
df1 = df1.assign(total_cost=df1['total_nights'] * df1['avg_price_per_room'])

# Define a dictionary mapping each full month name to its corresponding season
season_mapping = {
    'January': 'Winter',
    'February': 'Winter',
    'March': 'Spring',
    'April': 'Spring',
    'May': 'Spring',
    'June': 'Summer',
    'July': 'Summer',
    'August': 'Summer',
    'September': 'Fall',
    'October': 'Fall',
    'November': 'Fall',
    'December': 'Winter'
}

# Extract the month from the existing "month" column and map it to the corresponding season using the dictionary
df1['season'] = df1['arrival_month'].map(season_mapping)

# Load CSV file into a DataFrame
df2 = pd.read_csv(file2)

# Create a new column with the sum of col1 and col2
df2 = df2.assign(total_nights=df2['stays_in_weekend_nights'] + df2['stays_in_week_nights'])
df2 = df2.assign(total_cost=df2['total_nights'] * df2['avg_price_per_room'])

df2['booking_status'] = df2['booking_status'].replace({'Not_Canceled': 0, 'Canceled': 1})

month_season_mapping = {
    1: ('January', 'Winter'),
    2: ('February', 'Winter'),
    3: ('March', 'Spring'),
    4: ('April', 'Spring'),
    5: ('May', 'Spring'),
    6: ('June', 'Summer'),
    7: ('July', 'Summer'),
    8: ('August', 'Summer'),
    9: ('September', 'Fall'),
    10: ('October', 'Fall'),
    11: ('November', 'Fall'),
    12: ('December', 'Winter')
}
df2['arrival_month'], df2['season'] = zip(*df2['arrival_month'].map(month_season_mapping))

# Drop multiple columns
columns_to_drop = ['total_nights', 'hotel', 'lead_time', 'arrival_date_week_number', 'arrival_date_day_of_month', 'market_segment_type', 'stays_in_weekend_nights','stays_in_week_nights', 'avg_price_per_room', 'email']
df1 = df1.drop(columns=columns_to_drop)

df1.to_csv('Intermediate_input/input_country.csv', index=False, header=False)

columns_to_drop_df1 = ['country']
df1 = df1.drop(columns=columns_to_drop_df1)

columns_to_drop_df2 = ['total_nights', 'Booking_ID', 'arrival_date','lead_time', 'stays_in_weekend_nights','stays_in_week_nights', 'market_segment_type', 'avg_price_per_room']
df2 = df2.drop(columns=columns_to_drop_df2)

merged_df = pd.concat([df1, df2])

merged_df['month-year'] = merged_df['arrival_month'] + '-' + merged_df['arrival_year'].astype(str)

# Remove leading and trailing spaces
merged_df.columns = merged_df.columns.str.strip()

merged_df = merged_df.drop(columns=['arrival_year', 'arrival_month'])

merged_df.to_csv('Intermediate_input/input_season.csv', index=False, header=False)

merged_df = merged_df.drop(columns=['season'])

merged_df.to_csv('Intermediate_input/input.csv', index=False, header=False)