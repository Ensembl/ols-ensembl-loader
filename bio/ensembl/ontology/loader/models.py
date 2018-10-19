# -*- coding: utf-8 -*-
"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import enum
import logging

from sqlalchemy import *
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship, synonym
from sqlalchemy.orm.exc import NoResultFound

import ebi.ols.api.helpers as helpers

logger = logging.getLogger(__name__)

"""
SQLAlchemy database models for OLS ontologies loading
 
"""
__all__ = ['Ontology', 'Meta', 'Term', 'Subset', 'RelationType', 'Closure', 'Relation', 'AltId', 'Synonym',
           'SynonymTypeEnum', 'get_one_or_create']

long_string = String(5000)
long_string = long_string.with_variant(String(5000, collation='utf8_general_ci'), 'mysql')


def get_one_or_create(model,
                      session=None,
                      create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    create_kwargs = create_method_kwargs or {}
    q = 'undefined'
    try:
        q = session.query(model).filter_by(**kwargs)
        obj = q.one()
        logger.debug('Exists %s', obj)
        if 'helper' in create_kwargs:
            obj.update_from_helper(helper=create_kwargs.get('helper'))
        else:
            [setattr(obj, attribute, create_kwargs.get(attribute)) for attribute in create_kwargs if
             attribute is not None]
        logger.debug('Updated %s', obj)
        return obj, False
    except NoResultFound:
        try:
            create_kwargs.update(kwargs)
            new_obj = getattr(model, create_method, model)(**create_kwargs)
            session.add(new_obj)
            session.commit()
            logger.debug('Create %s', new_obj)
            return new_obj, True
        except IntegrityError as e:
            logger.error('Integrity error upon flush: %s', str(e))
            logger.error('Initial query: %s ', q)
            logger.error('Initial filters: %s', kwargs or {})
            session.rollback()
            return session.query(model).filter_by(**kwargs).one(), False


class SynonymTypeEnum(enum.Enum):
    EXACT = 'EXACT'
    BROAD = 'BROAD'
    NARROW = 'NARROW'
    RELATED = 'RELATED'


Base = declarative_base()


class LoadAble(object):
    _load_map = dict()

    def __init__(self, helper=None, **kwargs):
        if helper and isinstance(helper, helpers.OLSHelper):
            constructor_args = {key: getattr(helper, self._load_map.get(key, key), None) for key in dir(self)}
            # logger.debug('helper %s args: %s', helper.__class__, constructor_args)
            constructor_args.update(**kwargs)
            logger.debug('Helpers params %s ', helper)
        else:
            constructor_args = kwargs
        logger.debug('%s args: %s', self.__class__, constructor_args)
        super().__init__(**constructor_args)

    def __repr__(self):
        class_name = self.__class__.__name__
        attributes = {name: getattr(self, name) for name in dir(self) if
                      isinstance(getattr(self, name), (type(None), str, int, float, bool))}
        return '<{}({})>'.format(class_name, attributes)

    def update_from_helper(self, helper):
        [self.__setattr__(key, getattr(helper, self._load_map.get(key, key), None)) for key in dir(self) if
         getattr(helper, self._load_map.get(key, key), None) is not None]


class Meta(Base):
    __tablename__ = 'meta'
    __table_args__ = (
        Index('key_value_idx', 'meta_key', 'meta_value', unique=True),
        {'mysql_engine': 'MyISAM'}
    )

    meta_id = Column(Integer, primary_key=True)
    meta_key = Column(String(64), nullable=False, )
    meta_value = Column(String(128))
    species_id = Column(Integer)

    def __repr__(self):
        return '<Meta(meta_id={}, meta_key={}, meta_value={})>'.format(self.meta_id, self.meta_key, self.meta_value)


class Ontology(LoadAble, Base):
    __tablename__ = 'ontology'
    __table_args__ = (
        Index('name_namespace_idx', 'name', 'namespace', unique=True),
        {'mysql_engine': 'MyISAM'}
    )

    _load_map = dict(
        name='ontology_id'
    )

    def __dir__(self):
        return ['id', 'name', 'namespace', 'version', 'title', 'number_of_terms']

    id = Column('ontology_id', Integer, primary_key=True)
    name = Column('name', String(64), nullable=False)
    _namespace = Column('namespace', String(64), nullable=False)
    _version = Column('data_version', String(64), nullable=True)
    title = Column(String(255), nullable=True)

    terms = relationship('Term', cascade="all, delete", backref="ontology")

    number_of_terms = 0

    @hybrid_property
    def namespace(self):
        return self._namespace

    @hybrid_property
    def version(self):
        return self._version

    @namespace.setter
    def namespace(self, namespace):
        if type(namespace) is helpers.Ontology:
            # set from a ontology var
            self._namespace = namespace.namespace
        else:
            # set from db
            self._namespace = namespace

    @version.setter
    def version(self, version):
        if type(version) is helpers.Ontology:
            # set from a ontology var
            self._version = version.version
        else:
            self._version = version

    namespace = synonym('_namespace', descriptor=namespace)
    version = synonym('_version', descriptor=version)


class RelationType(LoadAble, Base):
    __tablename__ = 'relation_type'

    __table_args__ = (
        {'mysql_engine': 'MyISAM'}
    )

    def __dir__(self):
        return ['relation_type_id', 'name']

    relation_type_id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False, unique=True)


class Subset(LoadAble, Base):
    __tablename__ = 'subset'
    __table_args__ = (
        {'mysql_engine': 'MyISAM'}
    )

    _load_map = dict(
        definition='description'
    )

    def __dir__(self):
        return ['subset_id', 'name', 'definition']

    subset_id = Column(Integer, primary_key=True)
    name = Column(String(64, convert_unicode=True), nullable=False, unique=True)
    definition = Column(Unicode(1000), nullable=False, server_default=text("''"))


class Term(LoadAble, Base):
    __tablename__ = 'term'
    __table_args__ = (
        Index('ontology_acc_idx', 'ontology_id', 'accession', unique=True),
        Index('term_name_idx', 'name', mysql_length=100),
        {'mysql_engine': 'MyISAM'}
    )

    def __dir__(self):
        return ['term_id', 'name', 'ontology_id', 'subsets', 'accession', 'description', 'is_root', 'is_obsolete',
                'iri', 'ontology']

    term_id = Column(Integer, primary_key=True)
    ontology_id = Column(ForeignKey(Ontology.id), nullable=False)
    subsets = Column(Unicode(1000))
    accession = Column(String(64), nullable=False, unique=True)
    name = Column(long_string, nullable=False)

    description = Column('definition', long_string)

    is_root = Column(Boolean, nullable=False, default=False)
    is_obsolete = Column(Boolean, nullable=False, default=False)
    iri = Column(Unicode(1000))

    alt_ids = relationship("AltId", back_populates="term", cascade='all')
    synonyms = relationship("Synonym", cascade="delete")
    child_terms = relationship('Relation', cascade='delete', foreign_keys='Relation.parent_term_id')
    parent_terms = relationship('Relation', cascade='delete', foreign_keys='Relation.child_term_id')

    child_closures = relationship('Closure', foreign_keys='Closure.child_term_id', cascade='delete')
    parent_closures = relationship('Closure', foreign_keys='Closure.parent_term_id', cascade='delete')
    subparent_closures = relationship('Closure', foreign_keys='Closure.subparent_term_id', cascade='delete')

    def add_child_relation(self, child_term, rel_type, session):
        relation, created = get_one_or_create(Relation, session,
                                              parent_term=self,
                                              child_term=child_term,
                                              relation_type=rel_type,
                                              ontology=self.ontology)
        if created:
            self.child_terms.append(relation)
        return relation

    def add_parent_relation(self, parent_term, rel_type, session):
        relation, created = get_one_or_create(Relation, session,
                                              parent_term=parent_term,
                                              child_term=self,
                                              ontology=self.ontology,
                                              relation_type=rel_type)
        if created:
            self.parent_terms.append(relation)
        return relation

    def closures(self):
        childs = self.child_closures.all()
        parents = self.parent_closures.all()
        subparents = self.subparent_closures.all()
        return childs + parents + subparents


class AltId(LoadAble, Base):
    __tablename__ = 'alt_id'
    __table_args__ = (
        Index('term_alt_idx', 'term_id', 'alt_id', unique=True),
        {'mysql_engine': 'MyISAM'}
    )

    def __dir__(self):
        return ['alt_id', 'term_id', 'accession']

    alt_id = Column(Integer, primary_key=True)
    term_id = Column(ForeignKey('term.term_id'), nullable=False)
    accession = Column(String(64), nullable=False, index=True)

    term = relationship('Term', back_populates='alt_ids')


class Closure(LoadAble, Base):
    # NOT USE for now, closure is computed by perl standard script
    __tablename__ = 'closure'
    __table_args__ = (
        Index('child_parent_idx', 'child_term_id', 'parent_term_id', 'subparent_term_id', 'ontology_id', unique=True),
        Index('parent_subparent_idx', 'parent_term_id', 'subparent_term_id'),
        {'mysql_engine': 'MyISAM'}
    )

    def __dir__(self):
        return ['closure_id', 'ontology', 'child_term_id', 'parent_term_id']

    closure_id = Column(Integer, primary_key=True)
    child_term_id = Column(ForeignKey('term.term_id'), nullable=False)
    parent_term_id = Column(ForeignKey('term.term_id'), nullable=False)
    subparent_term_id = Column(ForeignKey('term.term_id'), index=True)
    distance = Column(Integer, nullable=False)
    ontology_id = Column(ForeignKey('ontology.ontology_id'), nullable=False, index=True)
    confident_relationship = Column(Integer, nullable=False, server_default=text("'0'"))

    ontology = relationship('Ontology')
    child_term = relationship('Term', primaryjoin='Closure.child_term_id == Term.term_id',
                              back_populates='child_closures')
    parent_term = relationship('Term', primaryjoin='Closure.parent_term_id == Term.term_id',
                               back_populates='parent_closures')
    subparent_term = relationship('Term', primaryjoin='Closure.subparent_term_id == Term.term_id',
                                  back_populates='subparent_closures')


class Relation(LoadAble, Base):
    __tablename__ = 'relation'
    __table_args__ = (
        Index('child_parent__term_idx', 'child_term_id', 'parent_term_id', 'relation_type_id', 'intersection_of',
              'ontology_id', unique=True),
        {'mysql_engine': 'MyISAM'}
    )

    def __dir__(self):
        return ['relation_id', 'ontology', 'relation_type', 'parent_term', 'child_term']

    relation_id = Column(Integer, primary_key=True)
    child_term_id = Column(ForeignKey('term.term_id'), nullable=False)
    parent_term_id = Column(ForeignKey('term.term_id'), nullable=False, index=True)
    relation_type_id = Column(ForeignKey('relation_type.relation_type_id'), nullable=False, index=True)
    intersection_of = Column(Boolean, nullable=False, server_default=text("'0'"))
    ontology_id = Column(ForeignKey('ontology.ontology_id'), nullable=False, index=True)

    child_term = relationship('Term', primaryjoin='Relation.child_term_id == Term.term_id',
                              back_populates='parent_terms')
    parent_term = relationship('Term', primaryjoin='Relation.parent_term_id == Term.term_id',
                               back_populates='child_terms')
    ontology = relationship('Ontology')
    relation_type = relationship('RelationType')

    def __repr__(self):
        return '<Relation(relation_id={}, child_term_id={}, parent_term_id={}, relation_type={})>'.format(
            self.relation_id, self.child_term.accession, self.parent_term.accession, self.relation_type.name)


class Synonym(LoadAble, Base):
    __tablename__ = 'synonym'
    __table_args__ = (
        Index('term_synonym_idx', 'term_id', 'synonym_id', unique=True),
        {'mysql_engine': 'MyISAM'}
        # Index('term_name_idx', 'name', mysql_length=2048),
    )

    synonym_id = Column(Integer, primary_key=True)
    term_id = Column(ForeignKey('term.term_id'), nullable=False)
    name = Column('name', long_string, nullable=False)
    type = Column(Enum(SynonymTypeEnum))
    db_xref = Column('dbxref', Unicode(500), nullable=True)

    term = relationship('Term', back_populates='synonyms')

    def __repr__(self):
        return '<Synonym(synonym_id={}, term_id={}, name={}, type={})>'.format(
            self.synonym_id, self.term_id, self.name, self.type)
