-- Create the 'actors_history_scd' table
CREATE TABLE actors_history_scd (
    actorid TEXT,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date DATE NOT NULL,
    end_date DATE,
    PRIMARY KEY (actorid, start_date)
);
