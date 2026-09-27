CREATE TYPE site_status AS ENUM ('indexing', 'indexed', 'failed');
CREATE TABLE sites
(
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url varchar(2048) UNIQUE,
    status site_status,
    status_ts timestamptz,
    last_error text
);