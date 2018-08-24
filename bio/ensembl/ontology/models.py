# -*- coding: utf-8 -*-
import enum
import logging
from typing import Iterable

from sqlalchemy import *
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

import ebi.ols.api.helpers as helpers
from .db import Base

logger = logging.getLogger(__name__)

"""
SQLAlchemy database models for OLS ontologies loading
 
"""
__all__ = ['Ontology', 'Meta', 'Term', 'Subset', 'RelationType', 'Relation', 'AltId', 'Synonym', 'SynonymTypeEnum']


class SynonymTypeEnum(enum.Enum):
    EXACT = 'EXACT'
    BROAD = 'BROAD'
    NARROW = 'NARROW'
    RELATED = 'RELATED'


class LoadAble(object):
    _load_map = dict()

    def __init__(self, helper=None, **kwargs) -> None:
        # print('in it 1', helper, isinstance(helper, helpers.OLSHelper))
        if helper and isinstance(helper, helpers.OLSHelper):
            constructor_args = {key: getattr(helper, self._load_map.get(key, key), None) for key in dir(self)}
            # logger.debug('helper %s args: %s', helper.__class__, constructor_args)
            # constructor_args.update(**kwargs)
            super().__init__(**constructor_args)
        else:
            logger.debug('No Helper %s args: %s', self.__class__, kwargs)
            super().__init__(**kwargs)

    def __repr__(self):
        class_name = self.__class__.__name__
        attributes = {name: getattr(self, name) for name in dir(self) if
                      isinstance(getattr(self, name), (type(None), str, int, float, bool))}
        return '<{}({})>'.format(class_name, attributes)


class Meta(Base):
    __tablename__ = 'meta'
    __table_args__ = (
        Index('key_value_idx', 'meta_key', 'meta_value', unique=True),
    )

    meta_id = Column(Integer, primary_key=True)
    meta_key = Column(String, nullable=False)
    meta_value = Column(String)
    species_id = Column(Integer)

    def __repr__(self):
        return '<Meta(meta_id={}, meta_key={}, meta_value={})>'.format(self.meta_id, self.meta_key, self.meta_value)


class Ontology(LoadAble, Base):
    __tablename__ = 'ontology'
    __table_args__ = (
        Index('name_namespace_idx', 'name', 'namespace', unique=True),
    )

    _load_map = dict(
        name='ontology_id'
    )

    def __dir__(self):
        return ['id', 'name', 'namespace', 'version', 'title']

    id = Column('ontology_id', Integer, primary_key=True)
    name = Column(String, nullable=False)
    _namespace = Column('namespace', String, nullable=False)
    _version = Column('data_version', String, nullable=True)
    title = Column(String, nullable=True)

    terms = relationship('Term')

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


class RelationType(Base):
    __tablename__ = 'relation_type'

    relation_type_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    def __repr__(self):
        return '<RelationType(relation_type_id={}, name={})>'.format(
            self.relation_type_id, self.name)


class Subset(LoadAble, Base):
    __tablename__ = 'subset'

    _load_map = dict(
        definition='description'
    )

    def __dir__(self):
        return ['subset_id', 'name', 'definition']

    subset_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    definition = Column(String, nullable=False, server_default=text("''"))


class Term(LoadAble, Base):
    __tablename__ = 'term'
    __table_args__ = (
        Index('ontology_acc_idx', 'ontology_id', 'accession', unique=True),
    )

    def __dir__(self):
        return ['term_id', 'name', 'ontology_id', 'subsets', 'accession', 'description', 'is_root', 'is_obsolete',
                'iri']

    term_id = Column(Integer, primary_key=True)
    ontology_id = Column(ForeignKey('ontology.ontology_id'), nullable=False)
    subsets = Column(Text)
    accession = Column(String, nullable=False, unique=True)
    name = Column(String, nullable=False, index=True)
    description = Column('definition', Text)
    is_root = Column(Boolean, nullable=False)
    is_obsolete = Column(Boolean, nullable=False)
    iri = Column(Text)

    ontology = relationship('Ontology')
    alt_accession = relationship("AltId", back_populates="term")


class AltId(LoadAble, Base):
    __tablename__ = 'alt_id'
    __table_args__ = (
        Index('term_alt_idx', 'term_id', 'alt_id', unique=True),
    )

    def __dir__(self) -> Iterable[str]:
        return ['alt_id', 'term_id', 'accession']

    alt_id = Column(Integer, primary_key=True)
    term_id = Column(ForeignKey('term.term_id'), nullable=False)
    accession = Column(String, nullable=False, index=True)

    term = relationship('Term', back_populates='alt_accession')


class Closure(Base):
    __tablename__ = 'closure'
    __table_args__ = (
        Index('child_parent_idx', 'child_term_id', 'parent_term_id', 'subparent_term_id', 'ontology_id', unique=True),
        Index('parent_subparent_idx', 'parent_term_id', 'subparent_term_id')
    )

    closure_id = Column(Integer, primary_key=True)
    child_term_id = Column(ForeignKey('term.term_id'), nullable=False)
    parent_term_id = Column(ForeignKey('term.term_id'), nullable=False)
    subparent_term_id = Column(ForeignKey('term.term_id'), index=True)
    distance = Column(Integer, nullable=False)
    ontology_id = Column(ForeignKey('ontology.ontology_id'), nullable=False, index=True)
    confident_relationship = Column(Integer, nullable=False, server_default=text("'0'"))

    child_term = relationship('Term', primaryjoin='Closure.child_term_id == Term.term_id')
    ontology = relationship('Ontology')
    parent_term = relationship('Term', primaryjoin='Closure.parent_term_id == Term.term_id')
    subparent_term = relationship('Term', primaryjoin='Closure.subparent_term_id == Term.term_id')

    def __repr__(self):
        return '<Closure(closure_id={}, child_term_id={}, parent_term_id={}, ontology_id={}, distance={})>'.format(
            self.closure_id, self.child_term_id, self.parent_term_id, self.ontology_id, self.distance)


class Relation(Base):
    __tablename__ = 'relation'
    __table_args__ = (
        Index('child_parent__term_idx', 'child_term_id', 'parent_term_id', 'relation_type_id', 'intersection_of',
              'ontology_id', unique=True),
    )

    relation_id = Column(Integer, primary_key=True)
    child_term_id = Column(ForeignKey('term.term_id'), nullable=False)
    parent_term_id = Column(ForeignKey('term.term_id'), nullable=False, index=True)
    relation_type_id = Column(ForeignKey('relation_type.relation_type_id'), nullable=False, index=True)
    intersection_of = Column(Boolean, nullable=False, server_default=text("'0'"))
    ontology_id = Column(ForeignKey('ontology.ontology_id'), nullable=False, index=True)
    # ontology_id = Column(Integer)

    child_term = relationship('Term', primaryjoin='Relation.child_term_id == Term.term_id')
    parent_term = relationship('Term', primaryjoin='Relation.parent_term_id == Term.term_id')
    ontology = relationship('Ontology')
    relation_type = relationship('RelationType')

    def __repr__(self):
        return '<Relation(relation_id={}, child_term_id={}, parent_term_id={}, relation_type_id={})>'.format(
            self.relation_type_id, self.child_term_id, self.parent_term_id, self.relation_type_id)


class Synonym(LoadAble, Base):
    __tablename__ = 'synonym'
    __table_args__ = (
        Index('term_synonym_idx', 'term_id', 'synonym_id', unique=True),
    )

    synonym_id = Column(Integer, primary_key=True)
    term_id = Column(ForeignKey('term.term_id'), nullable=False)
    name = Column(TEXT, nullable=False, index=True)
    type = Column(Enum(SynonymTypeEnum))
    db_xref = Column('dbxref', VARCHAR(255), nullable=True)

    term = relationship('Term')

    def __repr__(self):
        return '<Synonym(synonym_id={}, term_id={}, name={}, type={})>'.format(
            self.synonym_id, self.term_id, self.name, self.type)
