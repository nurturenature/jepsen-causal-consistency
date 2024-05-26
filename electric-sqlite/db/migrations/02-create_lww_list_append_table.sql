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

INSERT INTO lww (dummy,k) VALUES (0,0);
INSERT INTO lww (dummy,k) VALUES (0,1);
INSERT INTO lww (dummy,k) VALUES (0,2);
INSERT INTO lww (dummy,k) VALUES (0,3);
INSERT INTO lww (dummy,k) VALUES (0,4);
INSERT INTO lww (dummy,k) VALUES (0,5);
INSERT INTO lww (dummy,k) VALUES (0,6);
INSERT INTO lww (dummy,k) VALUES (0,7);
INSERT INTO lww (dummy,k) VALUES (0,8);
INSERT INTO lww (dummy,k) VALUES (0,9);
INSERT INTO lww (dummy,k) VALUES (0,10);
INSERT INTO lww (dummy,k) VALUES (0,11);
INSERT INTO lww (dummy,k) VALUES (0,12);
INSERT INTO lww (dummy,k) VALUES (0,13);
INSERT INTO lww (dummy,k) VALUES (0,14);
INSERT INTO lww (dummy,k) VALUES (0,15);
INSERT INTO lww (dummy,k) VALUES (0,16);
INSERT INTO lww (dummy,k) VALUES (0,17);
INSERT INTO lww (dummy,k) VALUES (0,18);
INSERT INTO lww (dummy,k) VALUES (0,19);
INSERT INTO lww (dummy,k) VALUES (0,20);
INSERT INTO lww (dummy,k) VALUES (0,21);
INSERT INTO lww (dummy,k) VALUES (0,22);
INSERT INTO lww (dummy,k) VALUES (0,23);
INSERT INTO lww (dummy,k) VALUES (0,24);
INSERT INTO lww (dummy,k) VALUES (0,25);
INSERT INTO lww (dummy,k) VALUES (0,26);
INSERT INTO lww (dummy,k) VALUES (0,27);
INSERT INTO lww (dummy,k) VALUES (0,28);
INSERT INTO lww (dummy,k) VALUES (0,29);
INSERT INTO lww (dummy,k) VALUES (0,30);
INSERT INTO lww (dummy,k) VALUES (0,31);
INSERT INTO lww (dummy,k) VALUES (0,32);
INSERT INTO lww (dummy,k) VALUES (0,33);
INSERT INTO lww (dummy,k) VALUES (0,34);
INSERT INTO lww (dummy,k) VALUES (0,35);
INSERT INTO lww (dummy,k) VALUES (0,36);
INSERT INTO lww (dummy,k) VALUES (0,37);
INSERT INTO lww (dummy,k) VALUES (0,38);
INSERT INTO lww (dummy,k) VALUES (0,39);
INSERT INTO lww (dummy,k) VALUES (0,40);
INSERT INTO lww (dummy,k) VALUES (0,41);
INSERT INTO lww (dummy,k) VALUES (0,42);
INSERT INTO lww (dummy,k) VALUES (0,43);
INSERT INTO lww (dummy,k) VALUES (0,44);
INSERT INTO lww (dummy,k) VALUES (0,45);
INSERT INTO lww (dummy,k) VALUES (0,46);
INSERT INTO lww (dummy,k) VALUES (0,47);
INSERT INTO lww (dummy,k) VALUES (0,48);
INSERT INTO lww (dummy,k) VALUES (0,49);
INSERT INTO lww (dummy,k) VALUES (0,50);
INSERT INTO lww (dummy,k) VALUES (0,51);
INSERT INTO lww (dummy,k) VALUES (0,52);
INSERT INTO lww (dummy,k) VALUES (0,53);
INSERT INTO lww (dummy,k) VALUES (0,54);
INSERT INTO lww (dummy,k) VALUES (0,55);
INSERT INTO lww (dummy,k) VALUES (0,56);
INSERT INTO lww (dummy,k) VALUES (0,57);
INSERT INTO lww (dummy,k) VALUES (0,58);
INSERT INTO lww (dummy,k) VALUES (0,59);
INSERT INTO lww (dummy,k) VALUES (0,60);
INSERT INTO lww (dummy,k) VALUES (0,61);
INSERT INTO lww (dummy,k) VALUES (0,62);
INSERT INTO lww (dummy,k) VALUES (0,63);
INSERT INTO lww (dummy,k) VALUES (0,64);
INSERT INTO lww (dummy,k) VALUES (0,65);
INSERT INTO lww (dummy,k) VALUES (0,66);
INSERT INTO lww (dummy,k) VALUES (0,67);
INSERT INTO lww (dummy,k) VALUES (0,68);
INSERT INTO lww (dummy,k) VALUES (0,69);
INSERT INTO lww (dummy,k) VALUES (0,70);
INSERT INTO lww (dummy,k) VALUES (0,71);
INSERT INTO lww (dummy,k) VALUES (0,72);
INSERT INTO lww (dummy,k) VALUES (0,73);
INSERT INTO lww (dummy,k) VALUES (0,74);
INSERT INTO lww (dummy,k) VALUES (0,75);
INSERT INTO lww (dummy,k) VALUES (0,76);
INSERT INTO lww (dummy,k) VALUES (0,77);
INSERT INTO lww (dummy,k) VALUES (0,78);
INSERT INTO lww (dummy,k) VALUES (0,79);
INSERT INTO lww (dummy,k) VALUES (0,80);
INSERT INTO lww (dummy,k) VALUES (0,81);
INSERT INTO lww (dummy,k) VALUES (0,82);
INSERT INTO lww (dummy,k) VALUES (0,83);
INSERT INTO lww (dummy,k) VALUES (0,84);
INSERT INTO lww (dummy,k) VALUES (0,85);
INSERT INTO lww (dummy,k) VALUES (0,86);
INSERT INTO lww (dummy,k) VALUES (0,87);
INSERT INTO lww (dummy,k) VALUES (0,88);
INSERT INTO lww (dummy,k) VALUES (0,89);
INSERT INTO lww (dummy,k) VALUES (0,90);
INSERT INTO lww (dummy,k) VALUES (0,91);
INSERT INTO lww (dummy,k) VALUES (0,92);
INSERT INTO lww (dummy,k) VALUES (0,93);
INSERT INTO lww (dummy,k) VALUES (0,94);
INSERT INTO lww (dummy,k) VALUES (0,95);
INSERT INTO lww (dummy,k) VALUES (0,96);
INSERT INTO lww (dummy,k) VALUES (0,97);
INSERT INTO lww (dummy,k) VALUES (0,98);
INSERT INTO lww (dummy,k) VALUES (0,99);
