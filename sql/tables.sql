
CREATE TABLE meta (
	meta_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
	meta_key VARCHAR(64) NOT NULL, 
	meta_value VARCHAR(128), 
	species_id INTEGER UNSIGNED DEFAULT NULL, 
	PRIMARY KEY (meta_id)
)ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci


CREATE UNIQUE INDEX key_value_idx ON meta (meta_key, meta_value)

CREATE TABLE ontology (
	ontology_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
	name VARCHAR(64) NOT NULL, 
	namespace VARCHAR(64) NOT NULL, 
	data_version VARCHAR(64) DEFAULT NULL, 
	title VARCHAR(255) DEFAULT NULL, 
	PRIMARY KEY (ontology_id)
)ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci


CREATE UNIQUE INDEX ontology_name_namespace_idx ON ontology (name, namespace)

CREATE TABLE subset (
	subset_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
	name VARCHAR(64) NOT NULL, 
	definition VARCHAR(511) NOT NULL DEFAULT '', 
	PRIMARY KEY (subset_id), 
	UNIQUE (name)
)ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci



CREATE TABLE relation_type (
	relation_type_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
	name VARCHAR(64) NOT NULL, 
	PRIMARY KEY (relation_type_id), 
	UNIQUE (name)
)ENGINE=MyISAM



CREATE TABLE term (
	term_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
	ontology_id INTEGER UNSIGNED NOT NULL, 
	subsets TEXT, 
	accession VARCHAR(64) NOT NULL, 
	name VARCHAR(255) COLLATE utf8_general_ci NOT NULL, 
	definition TEXT COLLATE utf8_general_ci, 
	is_root INTEGER NOT NULL DEFAULT 0, 
	is_obsolete INTEGER NOT NULL DEFAULT 0, 
	iri TEXT, 
	PRIMARY KEY (term_id), 
	FOREIGN KEY(ontology_id) REFERENCES ontology (ontology_id), 
	UNIQUE (accession)
)ENGINE=MyISAM


CREATE UNIQUE INDEX term_ontology_acc_idx ON term (ontology_id, accession)
CREATE INDEX term_name_idx ON term (name(100))

CREATE TABLE synonym (
	synonym_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
	term_id INTEGER UNSIGNED NOT NULL, 
	name TEXT COLLATE utf8_general_ci NOT NULL, 
	type ENUM('EXACT','BROAD','NARROW','RELATED'), 
	dbxref VARCHAR(500) DEFAULT NULL, 
	PRIMARY KEY (synonym_id), 
	FOREIGN KEY(term_id) REFERENCES term (term_id)
)ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci


CREATE INDEX synonym_name_idx ON synonym (name(100))
CREATE UNIQUE INDEX synonym_term_idx ON synonym (term_id, synonym_id)

CREATE TABLE alt_id (
	alt_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
	term_id INTEGER UNSIGNED NOT NULL, 
	accession VARCHAR(64) NOT NULL, 
	PRIMARY KEY (alt_id), 
	FOREIGN KEY(term_id) REFERENCES term (term_id)
)ENGINE=MyISAM


CREATE INDEX ix_alt_id_accession ON alt_id (accession)
CREATE UNIQUE INDEX term_alt_idx ON alt_id (term_id, alt_id)

CREATE TABLE relation (
	relation_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
	child_term_id INTEGER UNSIGNED NOT NULL, 
	parent_term_id INTEGER UNSIGNED NOT NULL, 
	relation_type_id INTEGER UNSIGNED NOT NULL, 
	intersection_of TINYINT UNSIGNED NOT NULL DEFAULT 0, 
	ontology_id INTEGER UNSIGNED NOT NULL, 
	PRIMARY KEY (relation_id), 
	FOREIGN KEY(child_term_id) REFERENCES term (term_id), 
	FOREIGN KEY(parent_term_id) REFERENCES term (term_id), 
	FOREIGN KEY(relation_type_id) REFERENCES relation_type (relation_type_id), 
	FOREIGN KEY(ontology_id) REFERENCES ontology (ontology_id)
)ENGINE=MyISAM


CREATE UNIQUE INDEX child_parent_idx ON relation (child_term_id, parent_term_id, relation_type_id, intersection_of, ontology_id)
CREATE INDEX ix_relation_parent_term_id ON relation (parent_term_id)
CREATE INDEX ix_relation_relation_type_id ON relation (relation_type_id)
CREATE INDEX ix_relation_ontology_id ON relation (ontology_id)

CREATE TABLE closure (
	closure_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
	child_term_id INTEGER UNSIGNED NOT NULL, 
	parent_term_id INTEGER UNSIGNED NOT NULL, 
	subparent_term_id INTEGER UNSIGNED, 
	distance TINYINT UNSIGNED NOT NULL, 
	ontology_id INTEGER UNSIGNED NOT NULL, 
	confident_relationship BOOL NOT NULL DEFAULT 0, 
	PRIMARY KEY (closure_id), 
	FOREIGN KEY(child_term_id) REFERENCES term (term_id), 
	FOREIGN KEY(parent_term_id) REFERENCES term (term_id), 
	FOREIGN KEY(subparent_term_id) REFERENCES term (term_id), 
	FOREIGN KEY(ontology_id) REFERENCES ontology (ontology_id), 
	CHECK (confident_relationship IN (0, 1))
)ENGINE=MyISAM


CREATE INDEX ix_closure_subparent_term_id ON closure (subparent_term_id)
CREATE UNIQUE INDEX closure_child_parent_idx ON closure (child_term_id, parent_term_id, subparent_term_id, ontology_id)
CREATE INDEX parent_subparent_idx ON closure (parent_term_id, subparent_term_id)
CREATE INDEX ix_closure_ontology_id ON closure (ontology_id)
