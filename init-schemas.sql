-- This SQL script creates the two schemas that are used for it's own tables
-- and the tables of users respectively
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE SCHEMA IF NOT EXISTS userdata;

-- And a separate schema for celery beat
CREATE SCHEMA IF NOT EXISTS celery;
