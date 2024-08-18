
CREATE TABLE ufo_sightings (
    id SERIAL PRIMARY KEY,
    datetime TIMESTAMP,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    shape VARCHAR(50),
    duration_seconds FLOAT,
    duration_hours_min VARCHAR(50),
    comments TEXT,
    date_posted DATE,
    latitude FLOAT,
    longitude FLOAT
);