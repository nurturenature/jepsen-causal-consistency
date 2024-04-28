-- grow only set
CREATE TABLE gset (
  id integer PRIMARY KEY,
   k integer,
   v integer
);

-- electrify the gset table
ALTER TABLE gset ENABLE ELECTRIC;
