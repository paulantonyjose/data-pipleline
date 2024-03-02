import functions_framework
import pandas as pd
from google.cloud import storage

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def aggregated_stats(cloud_event):
    bucket_name = 'booking_information'
    # Load the CSV files into pandas dataframes
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob('bookings.csv')
    blob.download_to_filename('bookings.csv')
    bookings_df = pd.read_csv('bookings.csv')

    blob = bucket.blob('customers.csv')
    blob.download_to_filename('customers.csv')
    bookings_df = pd.read_csv('customers.csv')

    blob = bucket.blob('destinations.csv')
    blob.download_to_filename('destinations.csv')
    bookings_df = pd.read_csv('destinations.csv')


    # Join the dataframes based on IDs
    merged_df = bookings_df.merge(customers_df, left_on='customer_id', right_on='id', suffixes=('_booking', '_customer'))
    merged_df = merged_df.merge(destinations_df, left_on='destination_id', right_on='id', suffixes=('', '_destination'))

    # Aggregate and create statistics like total bookings per destination
    stats_df = merged_df.groupby('destination')['total_booking_value'].sum().reset_index()
    stats_df.rename(columns={'total_booking_value': 'total_bookings_per_destination'}, inplace=True)

    # Display the aggregated statistics
    print(stats_df)