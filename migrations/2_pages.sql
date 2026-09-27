CREATE TABLE pages
(
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url varchar(2048),
    site_id integer REFERENCES sites(id) ON DELETE CASCADE
);