-- Table: caom2.proposal

DROP TABLE caom2.proposal;

CREATE TABLE caom2.proposal
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
ALTER TABLE caom2.proposal
  OWNER TO postgres;
GRANT ALL ON TABLE caom2.proposal TO postgres;
GRANT SELECT, INSERT ON TABLE caom2.proposal TO public;

-- Index: caom2.i_proposal_id

DROP INDEX caom2.i_proposal_id;

CREATE INDEX i_proposal_id
  ON caom2.proposal
  USING btree
  (proposal_id);

-- Index: caom2.i_proposal_pi_name

DROP INDEX caom2.i_proposal_pi_name;

CREATE INDEX i_proposal_pi_name
  ON caom2.proposal
  USING btree
  (pi_name COLLATE pg_catalog."default");

-- Index: caom2.i_proposal_title

DROP INDEX caom2.i_proposal_title;

CREATE INDEX i_proposal_title
  ON caom2.proposal
  USING btree
  (title COLLATE pg_catalog."default");

-- Table: caom2.publication

DROP TABLE caom2.publication;

CREATE TABLE caom2.publication
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
  CONSTRAINT pk_publication PRIMARY KEY (publication_oid),
  CONSTRAINT uk_publication UNIQUE (bib_code)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE caom2.publication
  OWNER TO postgres;
GRANT ALL ON TABLE caom2.publication TO postgres;
GRANT SELECT, INSERT ON TABLE caom2.publication TO public;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE caom2.publication TO ehst_dao;

-- Index: caom2.i_publication_abstract

DROP INDEX caom2.i_publication_abstract;

CREATE INDEX i_publication_abstract
  ON caom2.publication
  USING btree
  (abstract COLLATE pg_catalog."default");

-- Index: caom2.i_publication_authors

DROP INDEX caom2.i_publication_authors;

CREATE INDEX i_publication_authors
  ON caom2.publication
  USING btree
  (authors COLLATE pg_catalog."default");

-- Index: caom2.i_publication_bib_code

DROP INDEX caom2.i_publication_bib_code;

CREATE INDEX i_publication_bib_code
  ON caom2.publication
  USING btree
  (bib_code COLLATE pg_catalog."default");

-- Index: caom2.i_publication_title

DROP INDEX caom2.i_publication_title;

CREATE INDEX i_publication_title
  ON caom2.publication
  USING btree
  (title COLLATE pg_catalog."default");

-- Table: caom2.publication_proposal

DROP TABLE caom2.publication_proposal;

CREATE TABLE caom2.publication_proposal
(
  publication_proposal_oid serial NOT NULL,
  proposal_oid integer NOT NULL,
  publication_oid integer NOT NULL,
  CONSTRAINT pk_publication_proposal PRIMARY KEY (publication_proposal_oid),
  CONSTRAINT fk_pub_pro_proposal_oid FOREIGN KEY (proposal_oid)
      REFERENCES caom2.proposal (proposal_oid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT fk_pub_pro_publication_oid FOREIGN KEY (publication_oid)
      REFERENCES caom2.publication (publication_oid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE,
  CONSTRAINT uk_publication_proposal UNIQUE (proposal_oid, publication_oid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE caom2.publication_proposal
  OWNER TO postgres;
GRANT ALL ON TABLE caom2.publication_proposal TO postgres;
GRANT SELECT, INSERT ON TABLE caom2.publication_proposal TO public;
GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE caom2.publication_proposal TO ehst_dao;

-- Index: caom2.i_pub_prop_proposal

DROP INDEX caom2.i_pub_prop_proposal;

CREATE INDEX i_pub_prop_proposal
  ON caom2.publication_proposal
  USING btree
  (proposal_oid);

-- Index: caom2.i_pub_prop_publication

DROP INDEX caom2.i_pub_prop_publication;

CREATE INDEX i_pub_prop_publication
  ON caom2.publication_proposal
  USING btree
  (publication_oid);

