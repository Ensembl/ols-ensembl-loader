#  -*- coding: utf-8 -*-
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
import datetime
import logging
from os import getenv

import dateutil.parser
import time
from coreapi.exceptions import NetworkError, ErrorMessage
from requests.exceptions import ConnectionError
from sqlalchemy.orm.exc import NoResultFound

import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.loader.db import *
from bio.ensembl.ontology.loader.models import *
from ebi.ols.api.client import OlsClient

logger = logging.getLogger(__name__)


class OlsLoader(object):
    """ class loader for mapping retrieved DTO from OLS client into expected database fields """
    __relation_map = {
        'parents': 'is_a',
        'children': 'is_a',
        'derives_from/develops_from': 'develops_from'
    }
    __ignored_relations = [
        'graph', 'jstree', 'descendants', 'ancestors', 'hierarchicalParents',  # 'parents',
        'hierarchicalAncestors', 'hierarchicalChildren', 'hierarchicalDescendants'
    ]

    __synonym_map = {
        'hasExactSynonym': 'EXACT',
        'hasBroadSynonym': 'BROAD',
        'hasNarrowSynonym': 'NARROW',
        'hasRelatedSynonym': 'RELATED'
    }
    _default_options = dict(
        echo=False,
        wipe=False,
        db_version=getenv('ENS_VERSION'),
        max_retry=5,
        timeout=720
    )

    allowed_ontologies = ['go', 'so', 'pato', 'hp', 'vt', 'efo', 'po', 'eo', 'to', 'chebi', 'pr', 'fypo', 'peco', 'bfo',
                          'bto', 'cl', 'cmo', 'eco', 'mod', 'mp', 'ogms', 'uo']

    def __init__(self, url, **options):
        self.db_url = url
        self.options = self._default_options
        self.options.update(options)
        self.client = OlsClient()
        self.retry = 0
        self.db_init = False
        dal.db_init(self.db_url, **self.options)
        logger.info('Loaded with options %s ', self.options)
        self.current_ontology = None

    def init_meta(self):
        with dal.session_scope() as session:
            metas = {
                'schema_version': self.options.get('db_version'),
                'schema_type': 'ontology'
            }
            for meta_key, meta_value in metas.items():
                get_one_or_create(Meta,
                                  session,
                                  meta_key=meta_key,
                                  create_method_kwargs=dict(meta_value=meta_value))
        self.db_init = True

    def __call_client(self, method, *args, **kwargs):
        """
        Try 'max_retry' time to contact OLS api via its client, reraise error when limit reached.
        :param method: client method to call
        :param args:  client methods args
        :return: client response
        """
        retry = 0
        max_retry = self.options.get('max_retry')
        while retry < max_retry:
            try:
                logger.debug('Calling client.%s(%s)(%s)', method, args, kwargs)
                return self.client.__getattribute__(method)(*args, **kwargs)
            except (ConnectionError, NetworkError, ErrorMessage) as e:
                logger.error('Network error (%s) for %s(%s)(%s): %s ', self.current_ontology, method, args, kwargs, e)
                # wait 5 seconds until next OLS api client try
                time.sleep(5)
                retry += 1
                if retry >= max_retry:
                    logger.fatal('Max API retry for %s(%s)(%s)', method, args, kwargs)
                    raise e

    def load_ontology(self, ontology_name, namespace=None):
        self.current_ontology = ontology_name
        with dal.session_scope() as session:
            start = datetime.datetime.now()
            logger.debug('Updating meta for ontology %s', ontology_name)
            get_one_or_create(Meta,
                              session,
                              meta_key=ontology_name + '_load_date',
                              create_method_kwargs=dict(
                                  meta_value=ontology_name.upper() + '/' + start.strftime('%c')))
            o_ontology = self.__call_client('ontology', ontology_name)
            get_one_or_create(Meta,
                              session,
                              meta_key=o_ontology.ontology_id + '_file_date',
                              create_method_kwargs=dict(
                                  meta_value=o_ontology.ontology_id.upper() + '/' + dateutil.parser.parse(
                                      o_ontology.updated).strftime('%c')))
            m_ontology, created = get_one_or_create(Ontology,
                                                    session,
                                                    name=o_ontology.ontology_id,
                                                    namespace=namespace or o_ontology.namespace,
                                                    create_method_kwargs={'helper': o_ontology})
            logger.info('Loaded ontology %s', m_ontology)
            return m_ontology

    @staticmethod
    def wipe_ontology(ontology_name):
        with dal.session_scope() as session:
            logger.info('Wipe ontology %s', ontology_name)
            try:
                metas = session.query(Meta).filter(Meta.meta_key.like("%" + ontology_name + "%")).all()
                for meta in metas:
                    logger.debug('Deleted meta %s', meta)
                    session.delete(meta)
                ontologies = session.query(Ontology).filter_by(name=ontology_name).all()
                for ontology in ontologies:
                    logger.debug('Deleting ontology %s', ontology)
                    session.delete(ontology)
                return True
            except NoResultFound:
                logger.error('Ontology %s not found !', ontology_name)
            logger.info('... done')

    def load_ontology_terms(self, ontology, start=None, end=None):
        nb_terms = 0
        with dal.session_scope() as session:
            if type(ontology) is str:
                m_ontology = self.load_ontology(ontology)
                session.add(m_ontology)
                o_ontology = self.__call_client('ontology', ontology)
            elif isinstance(ontology, Ontology):
                m_ontology = ontology
                # session.add(m_ontology)
                o_ontology = self.__call_client('ontology', ontology.name)
            elif isinstance(ontology, helpers.Ontology):
                m_ontology = Ontology(helper=ontology)
                o_ontology = ontology
            else:
                raise RuntimeError('Wrong parameter')
            terms = o_ontology.terms()
            logger.info('Loading %s terms for %s', len(terms), m_ontology.name)
            if start and end:
                logger.warning('Getting slice ! %s %s', start, end)
                terms = terms[start:end]

            for o_term in terms:
                if o_term.is_defining_ontology and o_term.obo_id:
                    logger.debug('Loaded term (from OLS) %s', o_term)
                    logger.debug('Adding/Retrieving namespaced ontology %s', o_term.obo_name_space)
                    ontology, created = get_one_or_create(Ontology,
                                                          session,
                                                          name=m_ontology.name,
                                                          namespace=o_term.obo_name_space or m_ontology.name,
                                                          create_method_kwargs=dict(
                                                              version=m_ontology.version,
                                                              title=m_ontology.title))
                    term = self.load_term(o_term, ontology, session)
                    session.add(term)
                    nb_terms += 1
            return nb_terms
        return False

    def load_term(self, o_term, ontology, session):
        if type(ontology) is str:
            m_ontology = self.load_ontology(ontology)
        elif isinstance(ontology, Ontology):
            m_ontology = ontology
        elif isinstance(ontology, helpers.Ontology):
            m_ontology = Ontology(helper=ontology)
        else:
            raise RuntimeError('Wrong parameter')
        session.add(m_ontology)
        m_term, created = get_one_or_create(Term,
                                            session,
                                            accession=o_term.obo_id,
                                            create_method_kwargs=dict(helper=o_term,
                                                                      ontology=m_ontology))

        logger.info('Loaded Term %s ...', m_term)
        relation_types = [rel for rel in o_term.relations_types if rel not in self.__ignored_relations]
        self.load_term_relations(m_term, o_term, relation_types, session)
        self.load_term_synonyms(m_term, o_term, session)
        self.load_alt_ids(o_term, m_term, session)
        self.load_term_subsets(m_term, session)
        logger.info('... Done')
        return m_term

    def load_alt_ids(self, o_term, m_term, session):
        if self.options.get('wipe') is True:
            session.query(AltId).filter(AltId.term == m_term).delete()
        for alt_id in o_term.annotation.has_alternative_id:
            logger.info('Loaded AltId %s', alt_id)
            get_one_or_create(AltId,
                              session,
                              accession=alt_id,
                              create_method_kwargs=dict(accession=alt_id,
                                                        term=m_term))
            logger.info('...done')
        return m_term

    def load_term_subsets(self, term, session):
        subsets = []
        if term.subsets:
            logger.info('Loading term subsets: %s', term.subsets)
            subset_names = term.subsets.split(',')
            for subset_name in subset_names:
                subset = self.load_subset(subset_name, term.ontology.name, session)
                subsets.append(subset) if subset else None
            logger.info('... Done')
        return subsets

    def load_subset(self, subset_name, ontology_name, session):
        logger.debug(' Processing subset %s', subset_name)
        search = self.__call_client('search', query=subset_name, filters={'ontology': ontology_name,
                                                                          'type': 'property'})
        if search and len(search) == 1:
            details = self.__call_client('detail', search[0])
            if details:
                subset_def = details.definition or details.annotation.get('comment', [''])[0] or subset_name
                if not subset_def:
                    logger.warning('Subset %s has no definition (%s)', subset_name, ontology_name)
                subset, created = get_one_or_create(Subset, session,
                                                    name=subset_name,
                                                    definition=subset_def)
                return subset
            else:
                logger.warning('Unable to retrieve subset details %s (%s)', subset_name, ontology_name)
        else:
            logger.warning('Unable to retrieve subset %s (%s)', subset_name, ontology_name)
        return None

    def load_term_relations(self, m_term, o_term, relation_types, session):
        # remove previous relationships
        if self.options.get('wipe') is True:
            session.query(Relation).filter(Relation.child_term == m_term).delete()
            session.query(Relation).filter(Relation.parent_term == m_term).delete()
        logger.debug('Terms relations to load %s', relation_types)
        n_relations = 0
        for rel_name in relation_types:
            # updates relation types
            o_term = helpers.Term(ontology_name=o_term.ontology_name, iri=m_term.iri)
            o_relatives = o_term.load_relation(rel_name)
            relation_type, created = get_one_or_create(RelationType,
                                                       session,
                                                       name=self.__relation_map.get(rel_name, rel_name))

            logger.info('Loading %s relation %s (%s)...', m_term.accession, rel_name, relation_type.name)
            logger.info('%s related terms ', len(o_relatives))
            for o_related in o_relatives:
                r_accession = o_related.obo_id or o_related.short_form.replace('_', ':') or o_related.annotation.id[0]
                if r_accession is not None and o_related.ontology_name in self.allowed_ontologies:
                    if not o_related.is_defining_ontology:
                        logger.info('Related term is defined in another ontology: %s', o_related.ontology_name)
                        o_term_details = self.__call_client('term', o_related.iri, silent=True, unique=True)
                    else:
                        o_term_details = o_related
                    if o_term_details and o_term_details.ontology_name in self.allowed_ontologies:
                        o_onto_details = self.__call_client('ontology', o_term_details.ontology_name)

                        r_ontology, created = get_one_or_create(Ontology,
                                                                session,
                                                                name=o_onto_details.ontology_id,
                                                                namespace=o_related.obo_name_space,
                                                                create_method_kwargs=dict(
                                                                    version=o_onto_details.version,
                                                                    title=o_onto_details.title))
                        m_related, created = get_one_or_create(Term,
                                                               session,
                                                               accession=r_accession,
                                                               create_method_kwargs=dict(
                                                                   helper=o_term_details,
                                                                   ontology=r_ontology,
                                                               ))
                        # hack to reverse OBO loading behavior
                        if rel_name == 'children':
                            parent_term = m_term
                            child_term = m_related
                        else:
                            parent_term = m_related
                            child_term = m_term
                        relation, r_created = get_one_or_create(Relation,
                                                                session,
                                                                parent_term=parent_term,
                                                                child_term=child_term,
                                                                relation_type=relation_type,
                                                                ontology=m_term.ontology)
                        n_relations += 1 if r_created else 0
                        logger.info('Loaded relation %s %s %s', m_term.accession, relation_type.name,
                                    m_related.accession)
                    else:
                        logger.warning('Term %s (%s) relation %s with %s not found in %s ',
                                       m_term.accession, m_term.ontology.name,
                                       self.__relation_map.get(rel_name, rel_name),
                                       o_related.iri, o_related.ontology_name)
                else:
                    logger.info('Ignored related %s', o_related)
            logger.info('... Done (%s)', n_relations)
        return n_relations

    def load_term_synonyms(self, m_term, o_term, session):
        logger.info('Loading term synonyms...')
        if self.options.get('wipe') is True:
            session.query(Synonym).filter(Synonym.term == m_term).delete()
        n_synonyms = 0

        obo_synonyms = o_term.obo_synonym or []
        for synonym in obo_synonyms:
            if isinstance(synonym, dict):
                logger.info('Term obo synonym %s - %s', synonym['name'], self.__synonym_map[synonym['scope']])
                db_xref = synonym['xrefs'][0]['database'] + ':' + synonym['xrefs'][0]['id'] \
                    if 'xrefs' in synonym and len(synonym['xrefs']) > 0 else ''
                m_syno, created = get_one_or_create(Synonym,
                                                    session,
                                                    term=m_term,
                                                    name=synonym['name'],
                                                    create_method_kwargs=dict(
                                                        db_xref=db_xref,
                                                        type=self.__synonym_map[synonym['scope']]))
                n_synonyms += 1 if created else 0
        # OBO Xref are winning against standard synonymz
        synonyms = o_term.synonyms or []
        for synonym in synonyms:
            logger.info('Term synonym %s - EXACT - No dbXref', synonym)
            m_syno, created = get_one_or_create(Synonym,
                                                session,
                                                term=m_term, name=synonym, type='EXACT')
            n_synonyms += 1 if created else 0

        logger.info('... Done')
        return n_synonyms
