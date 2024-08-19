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

CREATE TABLE ufo_sightings_australia (
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
    longitude FLOAT,
    original_id INT,
    FOREIGN KEY (original_id) REFERENCES ufo_sightings(id)
);

INSERT INTO ufo_sightings_australia (datetime, city, state, country, shape, duration_seconds, duration_hours_min, comments, date_posted, latitude, longitude, original_id)
SELECT datetime, city, state, country, shape, duration_seconds, duration_hours_min, comments, date_posted, latitude, longitude, id
FROM ufo_sightings
WHERE country = 'AU';

SELECT * FROM ufo_sightings;
SELECT * FROM ufo_sightings_australia;

DROP TABLE IF EXISTS ufo_sightings;
DROP TABLE IF EXISTS ufo_sightings_australia;