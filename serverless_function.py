import functions_framework
import pandas as pd
from google.cloud import storage
import os
# Uncomment for testing
# os.environ["GCLOUD_PROJECT"] = "INSERT NAME OF YOUR PROJECT"

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def aggregated_stats(cloud_event=None):
    bucket_name = 'booking_information'

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob('bookings.csv')
    blob.download_to_filename('bookings.csv')
    bookings_df = pd.read_csv('bookings.csv')

    blob = bucket.blob('customers.csv')
    blob.download_to_filename('customers.csv')
    customers_df = pd.read_csv('customers.csv')

    blob = bucket.blob('destinations.csv')
    blob.download_to_filename('destinations.csv')
    destinations_df = pd.read_csv('destinations.csv')


    # Join the dataframes based on IDs
    merged_df = bookings_df.merge(customers_df, left_on='customer_id', right_on='id', suffixes=('_booking', '_customer'))
    merged_df = merged_df.merge(destinations_df, left_on='destination_id', right_on='id', suffixes=('', '_destination'))
    # Aggregate and create statistics like total bookings per destination

    destination_stats = merged_df.groupby('destination').agg(
    passengers_per_destination=('number_of_passengers', 'sum'),
    total_bookings_per_destination=('total_booking_value', 'sum'))
    
    date_stats = merged_df.groupby('booking_date').agg(
    passengers_per_date=('number_of_passengers', 'sum'),
    total_bookings_per_date=('total_booking_value', 'sum'))

    customer_stats =  merged_df.groupby('customer_id').agg(
    passengers_per_customer=('number_of_passengers', 'sum'),
    total_bookings_per_customer=('total_booking_value', 'sum'))

    total_destinations = merged_df.groupby('destination_id').size().sum()

    # Calculate total sum of all booking costs
    total_booking_costs = merged_df.groupby('destination_id')['total_booking_value'].sum().sum()

    # Calculate total number of passengers
    total_passengers = merged_df.groupby('customer_id')['number_of_passengers'].sum().sum()

    # Calculate total number of customers
    total_customers = merged_df.groupby('customer_id').size().sum()

    # Display the aggregated statistics
    print(destination_stats)
    print(date_stats)
    print(customer_stats)
    print('Total destinations : ' ,total_destinations)
    print("Total booking costs : ",total_booking_costs)
    print("Total passengers : ",total_passengers)
    print("Total customers : ",total_customers)

# if __name__=='__main__':
#     aggregated_stats()