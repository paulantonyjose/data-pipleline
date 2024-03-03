import psycopg2
import csv
from google.cloud.storage import Client, transfer_manager
import os
os.environ["GCLOUD_PROJECT"] = "My First Project"
def transfer_to_cloud_storage():
    conn = psycopg2.connect(
        host="localhost",
        database="booking_database",
        user="username",
        password="password"
    )

    # Open a cursor to perform database operations
    cur = conn.cursor()

    cur.execute("SELECT * FROM customers")
    customers_data = cur.fetchall()
    with open('customers.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        
        writer.writerow([i[0] for i in cur.description])

        writer.writerows(customers_data)

    cur.execute("SELECT * FROM bookings")
    bookings_data = cur.fetchall()
    with open('bookings.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        
        writer.writerow([i[0] for i in cur.description])

        writer.writerows(bookings_data)

    cur.execute("SELECT * FROM destinations")
    destinations_data = cur.fetchall()
    with open('destinations.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        
        writer.writerow([i[0] for i in cur.description])

        writer.writerows(destinations_data)
        
    cur.close()
    conn.close()


    storage_client = Client()
    bucket_name ='booking_information'
    bucket = storage_client.bucket(bucket_name)
    filenames = ['customers.csv','bookings.csv','destinations.csv']
    source_directory = '.'
    results = transfer_manager.upload_many_from_filenames(
        bucket, filenames, source_directory=source_directory, max_workers=8
    )

    for name, result in zip(filenames, results):
        # The results list is either `None` or an exception for each filename in
        # the input list, in order.

        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))

if __name__=='__main__':
    transfer_to_cloud_storage()