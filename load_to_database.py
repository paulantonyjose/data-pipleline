import psycopg2
def load_to_database():
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host="localhost",
        database="booking_database",
        user="username",
        password="password"
    )

        # Create a cursor object
    cur = conn.cursor()
    with open('/home/paul/data-pipeline/processed_data/processed_destination_data.csv') as file:
        cur.copy_expert("COPY destinations(id,destination,country,popular_season) FROM STDIN WITH CSV HEADER DELIMITER AS ','",file)
    with open('/home/paul/data-pipeline/processed_data/processed_customer_data.csv') as file:
        cur.copy_expert("COPY customers(id,first_name,last_name,email,phone) FROM STDIN WITH CSV HEADER DELIMITER AS ','",file)
    with open('/home/paul/data-pipeline/processed_data/processed_booking_data.csv') as file:
        cur.copy_expert("COPY bookings(id,customer_id,booking_date,number_of_passengers,cost_per_passenger,total_booking_value,destination_id) FROM STDIN WITH (FORMAT 'csv', HEADER false)",file)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    load_to_database()