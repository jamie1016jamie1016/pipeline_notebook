drop table if exists user_devices_cumulated;


CREATE TABLE user_devices_cumulated (
    user_id numeric,
    browser_type TEXT,
    device_activity_datelist DATE[],
    datelist_int BIGINT,
    PRIMARY KEY (user_id, browser_type)
);
