import pandas as pd 
import csv
def clean_and_transform():
    customer_data = pd.read_csv('https://raw.githubusercontent.com/paulantonyjose/data-pipleline/main/customer_data.csv')
    customer_data.dropna(subset=['customer_id','first_name','email','phone'])
    booking_data = pd.read_csv('https://raw.githubusercontent.com/paulantonyjose/data-pipleline/main/booking_data.csv' )
    booking_data['booking_date'] = pd.to_datetime(booking_data['booking_date'])
    booking_data.dropna()
    booking_data['total_booking_value'] = booking_data['number_of_passengers']*booking_data['cost_per_passenger'] 

    destination_data = pd.read_csv('https://raw.githubusercontent.com/paulantonyjose/data-pipleline/main/destination_data.csv')
    customer_data.dropna(subset=['destination_id','destination'])

    output_directory = 'processed_data/'
    customer_data.to_csv(output_directory + 'cleaned_customer_data.csv', index=False)
    booking_data.to_csv(output_directory + 'processed_booking_data.csv', index=False)
    destination_data.to_csv(output_directory + 'cleaned_destination_data.csv', index=False)

if __name__ == "__main__":
    clean_and_transform()