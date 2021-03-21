
-- CREATE SCHEMA IF NOT EXISTS crime;

CREATE TABLE IF NOT EXISTS crime_data (
    id SERIAL PRIMARY KEY,
    crimetype TEXT, 
    datetime TIMESTAMP NOT NULL,
    casenumber TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    policebeat TEXT,
    address TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL
);