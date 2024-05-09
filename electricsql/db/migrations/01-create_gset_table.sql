-- grow only set
CREATE TABLE IF NOT EXISTS gset (
  id INTEGER PRIMARY KEY,
   k INTEGER,
   v INTEGER
);

-- insure empty
DELETE FROM gset;

-- electrify table
ALTER TABLE gset ENABLE ELECTRIC;
