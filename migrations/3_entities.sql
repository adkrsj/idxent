CREATE TABLE entities
(
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    page_id integer REFERENCES pages(id) ON DELETE CASCADE,
    kind varchar(64),
    value text,
    sentence text
);