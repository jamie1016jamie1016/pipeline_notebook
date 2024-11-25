-- Create the custom 'films' type
drop type if exists films;
CREATE TYPE films AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);


-- Create the 'quality_class' enum type
drop type if exists quality_class;
CREATE TYPE quality_class AS ENUM (
    'star',
    'good',
    'average',
    'bad'
);

-- Create the 'actors' table
CREATE TABLE actors (
    actorid TEXT PRIMARY KEY,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER
);
