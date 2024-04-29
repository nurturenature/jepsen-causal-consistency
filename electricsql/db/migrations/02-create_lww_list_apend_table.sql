-- lww append only list register
CREATE TABLE IF NOT EXISTS lww (
  k INTEGER PRIMARY KEY,
  v TEXT
);

-- insure empty
DELETE FROM lww;

-- electrify table
ALTER TABLE lww ENABLE ELECTRIC;
