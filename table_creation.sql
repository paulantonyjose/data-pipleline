CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  phone VARCHAR(20)
);

CREATE TABLE bookings (

    id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id),
    booking_date DATE NOT NULL,
    destination_id INT NOT NULL REFERENCES destinations(id),
    number_of_passengers INT,
    total_booking_value DECIMAL(10,2),
    cost_per_passenger DECIMAL(10,2)
);

CREATE TABLE destinations (
  id SERIAL PRIMARY KEY,
  destination VARCHAR(255) NOT NULL,
  country VARCHAR(255),
  popular_season VARCHAR(255)
);

