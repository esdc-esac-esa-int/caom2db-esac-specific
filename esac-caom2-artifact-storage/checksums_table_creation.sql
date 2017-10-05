-- Table: caom2.checksums

-- DROP TABLE caom2.checksums;

CREATE TABLE caom2.checksums
(
  artifact character varying(1024) NOT NULL,
  checksum character varying(64) NOT NULL,
  CONSTRAINT checksums_pkidx PRIMARY KEY (artifact)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE caom2.checksums
  OWNER TO postgres;

-- Index: caom2.checksums_artifact_idx

-- DROP INDEX caom2.checksums_artifact_idx;

CREATE INDEX checksums_artifact_idx
  ON caom2.checksums
  USING btree
  (artifact);

-- Index: caom2.checksums_checksum_idx

-- DROP INDEX caom2.checksums_checksum_idx;

CREATE INDEX checksums_checksum_idx
  ON caom2.checksums
  USING btree
  (checksum COLLATE pg_catalog."default");

