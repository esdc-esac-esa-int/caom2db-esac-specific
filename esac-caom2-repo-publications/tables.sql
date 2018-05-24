-- Table: caom2.proposal

-- DROP TABLE caom2.proposal;

CREATE TABLE caom2_hla.proposal
(
  proposal_oid serial NOT NULL,
  proposal_id integer NOT NULL,
  pi_name citext NOT NULL,
  title citext NOT NULL,
  no_observations integer NOT NULL DEFAULT 0,
  no_publications integer NOT NULL DEFAULT 0,
  abstract citext NOT NULL DEFAULT 'TEST VALUE'::citext,
  science_category citext NOT NULL DEFAULT 'TEST VALUE'::citext,
  proposal_type citext NOT NULL DEFAULT 'TEST VALUE'::citext,
  cycle integer NOT NULL DEFAULT 666,
  CONSTRAINT pk_proposal PRIMARY KEY (proposal_oid),
  CONSTRAINT uk_proposal UNIQUE (proposal_id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE caom2_hla.proposal
  OWNER TO postgres;
GRANT ALL ON TABLE caom2_hla.proposal TO postgres;
GRANT SELECT, INSERT ON TABLE caom2_hla.proposal TO public;

-- Table: caom2.publication

-- DROP TABLE caom2.publication;

CREATE TABLE caom2_hla.publication
(
  publication_oid serial NOT NULL,
  bib_code citext NOT NULL,
  title citext,
  authors citext,
  abstract citext,
  journal citext,
  year smallint,
  page_number integer,
  volume_number smallint,
  no_observations integer NOT NULL DEFAULT 0,
  no_proposals integer NOT NULL DEFAULT 0,
  CONSTRAINT pk_publication PRIMARY KEY (publication_oid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE caom2_hla.publication
  OWNER TO postgres;
GRANT ALL ON TABLE caom2_hla.publication TO postgres;
GRANT SELECT, INSERT ON TABLE caom2_hla.publication TO public;

-- Table: caom2.publication_proposal

-- DROP TABLE caom2.publication_proposal;

CREATE TABLE caom2_hla.publication_proposal
(
  publication_proposal_oid serial NOT NULL,
  proposal_oid integer NOT NULL,
  publication_oid integer NOT NULL,
  CONSTRAINT pk_publication_proposal PRIMARY KEY (publication_proposal_oid),
  CONSTRAINT fk_pub_pro_proposal_oid FOREIGN KEY (proposal_oid)
      REFERENCES caom2_hla.proposal (proposal_oid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT fk_pub_pro_publication_oid FOREIGN KEY (publication_oid)
      REFERENCES caom2_hla.publication (publication_oid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT uk_publication_proposal UNIQUE (proposal_oid, publication_oid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE caom2_hla.publication_proposal
  OWNER TO postgres;
GRANT ALL ON TABLE caom2_hla.publication_proposal TO postgres;
GRANT SELECT, INSERT ON TABLE caom2_hla.publication_proposal TO public;

-- Index: caom2.i_pub_prop_proposal

-- DROP INDEX caom2.i_pub_prop_proposal;

CREATE INDEX i_pub_prop_proposal
  ON caom2_hla.publication_proposal
  USING btree
  (proposal_oid);

-- Index: caom2.i_pub_prop_publication

-- DROP INDEX caom2.i_pub_prop_publication;

CREATE INDEX i_pub_prop_publication
  ON caom2_hla.publication_proposal
  USING btree
  (publication_oid);
