-- dummy table exists only to be a FOREIGN KEY
CREATE TABLE IF NOT EXISTS dummy (
    dummy INTEGER NOT NULL,
    v     TEXT,
    CONSTRAINT dummy_pkey PRIMARY KEY (dummy)
);


-- lww append only list register
CREATE TABLE IF NOT EXISTS lww (
  k     INTEGER NOT NULL,
  v     TEXT,
  dummy INTEGER NOT NULL,
  CONSTRAINT lww_pkey PRIMARY KEY (k),
  FOREIGN KEY (dummy) REFERENCES dummy(dummy) DEFERRABLE
);

-- electrify tables
ALTER TABLE dummy ENABLE ELECTRIC;
ALTER TABLE lww   ENABLE ELECTRIC;

-- pre-populate keys to enable use of updateMany
INSERT INTO dummy (dummy) VALUES (0);

INSERT INTO lww (dummy,k,v) VALUES (0,0,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,1,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,2,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,3,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,4,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,5,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,6,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,7,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,8,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,9,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,10,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,11,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,12,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,13,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,14,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,15,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,16,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,17,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,18,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,19,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,20,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,21,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,22,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,23,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,24,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,25,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,26,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,27,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,28,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,29,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,30,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,31,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,32,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,33,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,34,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,35,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,36,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,37,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,38,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,39,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,40,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,41,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,42,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,43,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,44,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,45,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,46,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,47,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,48,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,49,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,50,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,51,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,52,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,53,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,54,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,55,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,56,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,57,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,58,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,59,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,60,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,61,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,62,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,63,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,64,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,65,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,66,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,67,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,68,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,69,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,70,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,71,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,72,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,73,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,74,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,75,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,76,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,77,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,78,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,79,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,80,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,81,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,82,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,83,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,84,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,85,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,86,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,87,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,88,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,89,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,90,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,91,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,92,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,93,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,94,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,95,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,96,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,97,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,98,NULL);
INSERT INTO lww (dummy,k,v) VALUES (0,99,NULL);
