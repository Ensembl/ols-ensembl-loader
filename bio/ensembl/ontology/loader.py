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

import dateutil.parser
import time
from coreapi.exceptions import NetworkError
from requests.exceptions import ConnectionError

import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.db import *
from bio.ensembl.ontology.models import *
from ebi.ols.api.client import OlsClient

logger = logging.getLogger(__name__)


class OlsLoader(object):
    """ class loader for mapping retrieved DTO from OLS client into expected database fields """
    __class_map = {
        helpers.Ontology: Ontology,
        helpers.Term: Term,
        helpers.Subset: Subset,
    }

    __relation_map = {
        'children': 'is_a',
    }

    _session = None
    __list_onto = None

    _default_options = dict(
        echo=False,
        wipe=False,
        max_retry=5,
    )

    def __init__(self, url, **options):
        self.db_url = url
        self.options = self._default_options
        self.options.update(options)
        self.client = OlsClient()
        self.retry = 0
        dal.db_init(self.db_url, **self.options)

    @property
    def allowed_ontologies(self):
        if self.__list_onto is None:
            from os.path import dirname, join
            with open(join(dirname(__file__), 'ontologies.ini')) as f:
                content = f.readlines()
            self.__list_onto = [x.strip() for x in content]
        return self.__list_onto

    def init_meta(self):
        metas = {
            'schema_version': self.options.get('db_version'),
            'schema_type': 'ontology'
        }
        for meta_key, meta_value in metas.items():
            get_one_or_create(Meta, meta_key=meta_key,
                              create_method_kwargs=dict(meta_value=meta_value))

    def load(self, ontology_name):
        with dal.session_scope() as session:
            if self.options.get('wipe', False):
                logger.info('Removing ontology %s', ontology_name)
                self.wipe_ontology(ontology_name)
                metas = session.query(Meta).filter(Meta.meta_key.like("%" + ontology_name + "%")).all()
                for meta in metas:
                    meta.delete()
            else:
                logger.info('Updating ontology %s', ontology_name)
            start = datetime.datetime.now()
            meta, created = get_one_or_create(Meta, meta_key=ontology_name + '_load_date',
                                              create_method_kwargs=dict(meta_value=start.strftime('%c')))
            if not created:
                meta.meta_value = start.strftime('%c')
            m_ontology = self.load_ontology(ontology_name)
            self.load_ontology_terms(m_ontology)
            ended = datetime.datetime.now()
            meta_time, created = get_one_or_create(Meta, meta_key=ontology_name + '_load_time',
                                                   create_method_kwargs=dict(
                                                       meta_value=(ended - start).total_seconds()))
            if not created:
                logger.debug('Updating meta_load_time to %s', (ended - start).total_seconds())
                meta_time.meta_value = (ended - start).total_seconds()
            return m_ontology

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
            except (ConnectionError, NetworkError) as e:
                logger.error('Network error %s for %s(%s)(%s)', e, method, args, kwargs)
                # wait 5 seconds until next OLS api client try
                time.sleep(5)
                retry += 1
                if retry == max_retry:
                    logger.fatal('Max API retry for %s(%s)(%s)', method, args, kwargs)
                    raise e

    def load_ontology(self, ontology_name, namespace=None):
        o_ontology = self.__call_client('ontology', ontology_name)
        meta, created = get_one_or_create(Meta, meta_key=ontology_name + '_file_date',
                                          create_method_kwargs=dict(
                                              meta_value=ontology_name.upper() + '/' + dateutil.parser.parse(
                                                  o_ontology.updated).strftime('%c')))

        if not created:
            meta.meta_value = dateutil.parser.parse(o_ontology.updated).strftime('%c')
        m_ontology, created = get_one_or_create(Ontology, name=o_ontology.ontology_id,
                                                namespace=namespace or o_ontology.namespace,
                                                create_method_kwargs={'helper': o_ontology})
        logger.info('Loaded ontology %s', m_ontology)
        return m_ontology

    @staticmethod
    def wipe_ontology(ontology_name):
        with dal.session_scope() as session:
            try:
                logger.debug('Delete ontology %s', ontology_name)
                ontologies = session.query(Ontology).filter_by(name=ontology_name).all()
                for ontology in ontologies:
                    logger.debug('Deleting ontology %s', ontology)
                    session.delete(ontology)
                return True
            except NoResultFound:
                logger.debug('Ontology not found')
        return False

    def load_term_relations(self, m_term, relation_type, rel_name):
        logger.info('   Loading %s relation %s (%s)...', m_term.accession, rel_name, relation_type.name)
        o_term = helpers.Term(ontology_name=m_term.ontology.name, iri=m_term.iri)
        o_relatives = o_term.load_relation(rel_name)
        logger.info('   %s related terms ', len(o_relatives))
        n_relations = 0

        for o_related in o_relatives:
            if o_related.accession is not None:
                if o_related.is_defining_ontology:
                    if o_related.ontology_name == m_term.ontology.name:
                        r_ontology = m_term.ontology
                    else:
                        logger.info('   The term %s does not belong to current ontology %s', o_related.accession,
                                    m_term.ontology.name)
                        # ro_term = self.client.term(identifier=o_related.iri, unique=True)
                        ro_term = self.__call_client('term', identifier=o_related.iri, unique=True)
                        if ro_term is not None and ro_term.ontology_name in self.allowed_ontologies:
                            logger.debug('  Term is defined in another expected ontology: %s', ro_term.ontology_name)
                            # load ontology
                            o_onto_details = self.__call_client('ontology', ro_term.ontology_name)
                            r_ontology, created = get_one_or_create(Ontology, name=o_onto_details.ontology_id,
                                                                    namespace=ro_term.obo_name_space,
                                                                    create_method_kwargs=dict(
                                                                        version=o_onto_details.version,
                                                                        title=o_onto_details.title))
                        else:
                            r_ontology = None
                    if r_ontology is not None:
                        m_related, created = get_one_or_create(Term,
                                                               accession=o_related.obo_id,
                                                               create_method_kwargs=dict(
                                                                   helper=o_related,
                                                                   ontology=r_ontology,
                                                               ))
                        relation, r_created = get_one_or_create(Relation,
                                                                child_term=m_related,
                                                                parent_term=m_term,
                                                                relation_type=relation_type,
                                                                ontology=m_term.ontology)
                        n_relations += 1 if r_created else 0
                        logger.info('Loaded relation %s %s %s', m_term.accession, relation_type.name,
                                    m_related.accession)
            else:
                logger.warning('This term is not in current ontology, neither defining one %s', o_related)
        logger.info('   ... Done (%s)', n_relations)
        return n_relations

    def load_term_synonyms(self, m_term: Term, o_term: helpers.Term):
        logger.info('   Loading term synonyms...')
        with dal.session_scope() as session:
            session.query(Synonym).filter(Synonym.term == m_term).delete()
        n_synonyms = 0
        synonym_map = {
            'hasExactSynonym': 'EXACT',
            'hasBroadSynonym': 'BROAD',
            'hasNarrowSynonym': 'NARROW',
            'hasRelatedSynonym': 'RELATED'
        }
        obo_synonyms = o_term.obo_synonym or []
        for synonym in obo_synonyms:
            if isinstance(synonym, dict):
                logger.info('   Term obo synonym %s - %s', synonym['name'], synonym_map[synonym['scope']])
                db_xref = synonym['xrefs'][0]['database'] + ':' + synonym['xrefs'][0]['id'] \
                    if 'xrefs' in synonym and len(synonym['xrefs']) > 0 else ''
                m_syno, created = get_one_or_create(Synonym, term=m_term, name=synonym['name'],
                                                    create_method_kwargs=dict(
                                                        db_xref=db_xref,
                                                        type=synonym_map[synonym['scope']]))
                n_synonyms += 1 if created else 0
        # OBO Xref are winning against standard synonymz
        synonyms = o_term.synonyms or []
        for synonym in synonyms:
            logger.info('   Term synonym %s - EXACT - No dbXref', synonym)
            m_syno, created = get_one_or_create(Synonym, term=m_term, name=synonym, type='EXACT')
            n_synonyms += 1 if created else 0

        logger.info('   ... Done')
        return n_synonyms

    def load_ontology_terms(self, ontology):
        nb_terms = 0
        if type(ontology) is str:
            m_ontology = self.load_ontology(ontology)
            terms = self.__call_client('ontology', ontology).terms()
        elif isinstance(ontology, Ontology):
            m_ontology = ontology
            terms = self.__call_client('ontology', ontology.name).terms()
        elif isinstance(ontology, helpers.Ontology):
            m_ontology = Ontology(helper=ontology)
            terms = ontology.terms()
        else:
            raise RuntimeError('Wrong parameter')
        logger.info('Loading %s terms for %s', len(terms), m_ontology.name)
        for o_term in terms:
            if o_term.is_defining_ontology and o_term.obo_id:
                logger.debug('Loaded term (from OLS) %s', o_term)
                ontology, created = get_one_or_create(Ontology, name=m_ontology.name,
                                                      namespace=o_term.obo_name_space or m_ontology.name,
                                                      create_method_kwargs=dict(
                                                          version=m_ontology.version,
                                                          title=m_ontology.title))
                self.load_term(o_term, ontology)
                nb_terms += 1
        return nb_terms

    def load_term(self, o_term, m_ontology):
        logger.debug('Adding/Retrieving namespaced ontology %s', o_term.obo_name_space)
        m_term, created = get_one_or_create(Term, accession=o_term.obo_id,
                                            create_method_kwargs=dict(helper=o_term,
                                                                      ontology=m_ontology))

        logger.info('Create term %s ...', m_term)
        self.load_term_subsets(m_term)
        types = o_term.relations_types + ['parents']
        relation_types = [rel for rel in types]
        with dal.session_scope() as session:
            # session = dal.get_session()
            session.query(Relation).filter(Relation.child_term == m_term).delete()
            session.query(Relation).filter(Relation.parent_term == m_term).delete()
        for relation in relation_types:
            # updates relation types
            relation_type, created = get_one_or_create(RelationType,
                                                       name=self.__relation_map.get(relation, relation))
            self.load_term_relations(m_term, relation_type, relation)
        self.load_term_synonyms(m_term, o_term)
        self.load_alt_ids(o_term, m_term)
        logger.info('... Done')
        return m_term

    def load_alt_ids(self, o_term, m_term):
        with dal.session_scope() as session:
            session.query(AltId).filter(AltId.term == m_term).delete()
        for alt_id in o_term.annotation.has_alternative_id:
            logger.info('Loaded AltId %s', alt_id)
            m_term.alt_ids.append(AltId(accession=alt_id))
        return m_term

    def load_term_subsets(self, term: Term):
        subsets = []
        if term.subsets:
            logger.info('   Loading term subsets: %s', term.subsets)
            subset_names = term.subsets.split(',')
            for subset_name in subset_names:
                logger.debug('      Processing subset %s', subset_name)
                # search = self.client.search(query=subset_name, filters={'ontology': term.ontology.name, 'type': 'property'})
                search = self.__call_client('search', query=subset_name, filters={'ontology': term.ontology.name,
                                                                              'type': 'property'})
                if len(search) == 1:
                    details = self.__call_client('detail', search[0])
                    subset, created = get_one_or_create(Subset, name=subset_name,
                                                        definition=details.definition or '')
                    if created:
                        logger.info('      Created subset [%s: %s]', subset.subset_id, subset_name)
                        subsets += subset
            logger.info('   ... Done')
        return subsets
