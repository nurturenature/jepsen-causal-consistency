-- buckets table exists to be a FOREIGN KEY
CREATE TABLE IF NOT EXISTS buckets (
    bucket INTEGER NOT NULL,
    v      TEXT,
    CONSTRAINT bucket_pkey PRIMARY KEY (bucket)
);


-- lww append only list register
CREATE TABLE IF NOT EXISTS lww (
  k      INTEGER NOT NULL,
  v      TEXT,
  bucket INTEGER NOT NULL,
  CONSTRAINT lww_pkey PRIMARY KEY (k),
  FOREIGN KEY (bucket) REFERENCES buckets(bucket) DEFERRABLE
);

-- electrify tables
ALTER TABLE buckets ENABLE ELECTRIC;
ALTER TABLE lww     ENABLE ELECTRIC;

-- pre-populate keys to enable use of updateMany
INSERT INTO buckets (bucket) VALUES (0);

INSERT INTO lww (k,bucket) VALUES (0,0);
INSERT INTO lww (k,bucket) VALUES (1,0);
INSERT INTO lww (k,bucket) VALUES (2,0);
INSERT INTO lww (k,bucket) VALUES (3,0);
INSERT INTO lww (k,bucket) VALUES (4,0);
INSERT INTO lww (k,bucket) VALUES (5,0);
INSERT INTO lww (k,bucket) VALUES (6,0);
INSERT INTO lww (k,bucket) VALUES (7,0);
INSERT INTO lww (k,bucket) VALUES (8,0);
INSERT INTO lww (k,bucket) VALUES (9,0);
INSERT INTO lww (k,bucket) VALUES (10,0);
INSERT INTO lww (k,bucket) VALUES (11,0);
INSERT INTO lww (k,bucket) VALUES (12,0);
INSERT INTO lww (k,bucket) VALUES (13,0);
INSERT INTO lww (k,bucket) VALUES (14,0);
INSERT INTO lww (k,bucket) VALUES (15,0);
INSERT INTO lww (k,bucket) VALUES (16,0);
INSERT INTO lww (k,bucket) VALUES (17,0);
INSERT INTO lww (k,bucket) VALUES (18,0);
INSERT INTO lww (k,bucket) VALUES (19,0);
INSERT INTO lww (k,bucket) VALUES (20,0);
INSERT INTO lww (k,bucket) VALUES (21,0);
INSERT INTO lww (k,bucket) VALUES (22,0);
INSERT INTO lww (k,bucket) VALUES (23,0);
INSERT INTO lww (k,bucket) VALUES (24,0);
INSERT INTO lww (k,bucket) VALUES (25,0);
INSERT INTO lww (k,bucket) VALUES (26,0);
INSERT INTO lww (k,bucket) VALUES (27,0);
INSERT INTO lww (k,bucket) VALUES (28,0);
INSERT INTO lww (k,bucket) VALUES (29,0);
INSERT INTO lww (k,bucket) VALUES (30,0);
INSERT INTO lww (k,bucket) VALUES (31,0);
INSERT INTO lww (k,bucket) VALUES (32,0);
INSERT INTO lww (k,bucket) VALUES (33,0);
INSERT INTO lww (k,bucket) VALUES (34,0);
INSERT INTO lww (k,bucket) VALUES (35,0);
INSERT INTO lww (k,bucket) VALUES (36,0);
INSERT INTO lww (k,bucket) VALUES (37,0);
INSERT INTO lww (k,bucket) VALUES (38,0);
INSERT INTO lww (k,bucket) VALUES (39,0);
INSERT INTO lww (k,bucket) VALUES (40,0);
INSERT INTO lww (k,bucket) VALUES (41,0);
INSERT INTO lww (k,bucket) VALUES (42,0);
INSERT INTO lww (k,bucket) VALUES (43,0);
INSERT INTO lww (k,bucket) VALUES (44,0);
INSERT INTO lww (k,bucket) VALUES (45,0);
INSERT INTO lww (k,bucket) VALUES (46,0);
INSERT INTO lww (k,bucket) VALUES (47,0);
INSERT INTO lww (k,bucket) VALUES (48,0);
INSERT INTO lww (k,bucket) VALUES (49,0);
INSERT INTO lww (k,bucket) VALUES (50,0);
INSERT INTO lww (k,bucket) VALUES (51,0);
INSERT INTO lww (k,bucket) VALUES (52,0);
INSERT INTO lww (k,bucket) VALUES (53,0);
INSERT INTO lww (k,bucket) VALUES (54,0);
INSERT INTO lww (k,bucket) VALUES (55,0);
INSERT INTO lww (k,bucket) VALUES (56,0);
INSERT INTO lww (k,bucket) VALUES (57,0);
INSERT INTO lww (k,bucket) VALUES (58,0);
INSERT INTO lww (k,bucket) VALUES (59,0);
INSERT INTO lww (k,bucket) VALUES (60,0);
INSERT INTO lww (k,bucket) VALUES (61,0);
INSERT INTO lww (k,bucket) VALUES (62,0);
INSERT INTO lww (k,bucket) VALUES (63,0);
INSERT INTO lww (k,bucket) VALUES (64,0);
INSERT INTO lww (k,bucket) VALUES (65,0);
INSERT INTO lww (k,bucket) VALUES (66,0);
INSERT INTO lww (k,bucket) VALUES (67,0);
INSERT INTO lww (k,bucket) VALUES (68,0);
INSERT INTO lww (k,bucket) VALUES (69,0);
INSERT INTO lww (k,bucket) VALUES (70,0);
INSERT INTO lww (k,bucket) VALUES (71,0);
INSERT INTO lww (k,bucket) VALUES (72,0);
INSERT INTO lww (k,bucket) VALUES (73,0);
INSERT INTO lww (k,bucket) VALUES (74,0);
INSERT INTO lww (k,bucket) VALUES (75,0);
INSERT INTO lww (k,bucket) VALUES (76,0);
INSERT INTO lww (k,bucket) VALUES (77,0);
INSERT INTO lww (k,bucket) VALUES (78,0);
INSERT INTO lww (k,bucket) VALUES (79,0);
INSERT INTO lww (k,bucket) VALUES (80,0);
INSERT INTO lww (k,bucket) VALUES (81,0);
INSERT INTO lww (k,bucket) VALUES (82,0);
INSERT INTO lww (k,bucket) VALUES (83,0);
INSERT INTO lww (k,bucket) VALUES (84,0);
INSERT INTO lww (k,bucket) VALUES (85,0);
INSERT INTO lww (k,bucket) VALUES (86,0);
INSERT INTO lww (k,bucket) VALUES (87,0);
INSERT INTO lww (k,bucket) VALUES (88,0);
INSERT INTO lww (k,bucket) VALUES (89,0);
INSERT INTO lww (k,bucket) VALUES (90,0);
INSERT INTO lww (k,bucket) VALUES (91,0);
INSERT INTO lww (k,bucket) VALUES (92,0);
INSERT INTO lww (k,bucket) VALUES (93,0);
INSERT INTO lww (k,bucket) VALUES (94,0);
INSERT INTO lww (k,bucket) VALUES (95,0);
INSERT INTO lww (k,bucket) VALUES (96,0);
INSERT INTO lww (k,bucket) VALUES (97,0);
INSERT INTO lww (k,bucket) VALUES (98,0);
INSERT INTO lww (k,bucket) VALUES (99,0);
