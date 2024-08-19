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


DROP TABLE IF EXISTS ufo_sightings;
DROP TABLE IF EXISTS ufo_sightings_australia;

SELECT * FROM ufo_sightings;

SELECT * FROM ufo_sightings_australia;