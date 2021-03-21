
CREATE TABLE IF NOT EXISTS crime_data_tbl (
    id SERIAL PRIMARY KEY,
    crimetype TEXT NOT NULL, 
    datetime TIMESTAMP NOT NULL,
    casenumber TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
);