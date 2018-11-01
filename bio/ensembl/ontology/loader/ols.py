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
import inflection
import itypes
import time
from coreapi.exceptions import CoreAPIException
from requests.exceptions import ConnectionError
from sqlalchemy.orm.exc import NoResultFound

import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.loader.db import *
from bio.ensembl.ontology.loader.models import *
from ebi.ols.api.client import OlsClient

logger = logging.getLogger(__name__)


def get_accession(o_term):
    if not o_term.obo_id and o_term.short_form:
        log = logging.getLogger('ols_errors')
        log.error('[NO_OBO_ID][%s][%s]', o_term.short_form, o_term.iri)
        # guess
        sp = o_term.short_form.split('_')
        if len(sp) == 2:
            o_term.obo_id = ':'.join(sp)
            return o_term.obo_id
        else:
            logger.warning('Unable to parse %s', o_term.short_form)
            return False
    return o_term.obo_id


class OlsLoader(object):
    """ class loader for mapping retrieved DTO from OLS client into expected database fields """
    __relation_map = {
        'parents': 'is_a',
        'children': 'is_a',
        'derives_from/develops_from': 'develops_from'
    }
    __ignored_relations = [
        'graph', 'jstree', 'descendants', 'ancestors', 'hierarchicalParents', 'children',
        'parents', 'hierarchicalAncestors', 'hierarchicalChildren', 'hierarchicalDescendants'
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
        timeout=720,
        process_relations=True,
        process_parents=True
    )

    allowed_ontologies = ['go', 'so', 'pato', 'hp', 'vt', 'efo', 'po', 'eo', 'to', 'chebi', 'pr', 'fypo', 'peco', 'bfo',
                          'bto', 'cl', 'cmo', 'eco', 'mod', 'mp', 'ogms', 'uo', 'mondo']

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
            except (ConnectionError, CoreAPIException) as e:
                # wait 5 seconds until next OLS api client try
                time.sleep(5)
                logger.warning('%s call retry: %s(%s)(%s): %s ',
                               self.current_ontology, method, args, kwargs, e)
                retry += 1
                if retry >= max_retry:
                    logger.fatal('Max API retry for %s(%s)(%s)', method, args, kwargs)
                    raise e

    def load_ontology(self, ontology_name, namespace=''):
        self.current_ontology = ontology_name
        with dal.session_scope() as session:
            start = datetime.datetime.now()
            logger.debug('Updating meta for ontology %s', ontology_name)
            get_one_or_create(Meta,
                              session,
                              meta_key=ontology_name + '_load_date',
                              create_method_kwargs=dict(
                                  meta_value=ontology_name.upper() + '/' + start.strftime('%c')))
            o_ontology = self.__call_client('ontology', identifier=ontology_name)
            get_one_or_create(Meta,
                              session,
                              meta_key=o_ontology.ontology_id + '_file_date',
                              create_method_kwargs=dict(
                                  meta_value=o_ontology.ontology_id.upper() + '/' + dateutil.parser.parse(
                                      o_ontology.updated).strftime('%c')))
            m_ontology, created = get_one_or_create(Ontology,
                                                    session,
                                                    name=o_ontology.ontology_id,
                                                    namespace=namespace or ontology_name,
                                                    create_method_kwargs={'helper': o_ontology})
            logger.info('Loaded Ontology [%s/%s] %s', m_ontology.name, m_ontology.namespace, m_ontology.title)
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
                    logger.info('Deleting namespaced ontology %s - %s', ontology.name, ontology.namespace)
                    res = session.query(Synonym).filter(Synonym.term_id == Term.term_id,
                                                        Term.ontology_id == ontology.id).delete(
                        synchronize_session=False)
                    logger.info('Wiped %s synonyms', res)
                    rel = session.query(Relation).filter(Relation.child_term_id == Term.term_id,
                                                         Term.ontology_id == ontology.id).delete(
                        synchronize_session=False)
                    rel2 = session.query(Relation).filter(Relation.parent_term_id == Term.term_id,
                                                          Term.ontology_id == ontology.id).delete(
                        synchronize_session=False)
                    logger.info('Wiped %s Relations', rel + rel2)

                    clo = session.query(Closure).filter(Closure.child_term_id == Term.term_id,
                                                        Term.ontology_id == ontology.id).delete(
                        synchronize_session=False)
                    clo1 = session.query(Closure).filter(Closure.parent_term_id == Term.term_id,
                                                         Term.ontology_id == ontology.id).delete(
                        synchronize_session=False)
                    clo2 = session.query(Closure).filter(Closure.subparent_term_id == Term.term_id,
                                                         Term.ontology_id == ontology.id).delete(
                        synchronize_session=False)
                    logger.info('Wiped %s Closure', clo + clo1 + clo2)

                    res = session.query(AltId).filter(AltId.term_id == Term.term_id,
                                                      Term.ontology_id == ontology.id).delete(synchronize_session=False)
                    logger.info('Wiped %s AltIds', res)
                    res = session.query(Term).filter(Term.ontology_id == ontology.id).delete(synchronize_session=False)
                    logger.info('Wiped %s Terms', res)
                    session.delete(ontology)
                return True
            except NoResultFound:
                logger.error('Ontology %s not found !', ontology_name)
            logger.debug('...Done')

    def load_ontology_terms(self, ontology, start=None, end=None):
        self.current_ontology = ontology
        nb_terms = 0
        o_ontology = self.__call_client('ontology', identifier=ontology)
        self.current_ontology = o_ontology.ontology_id
        terms = o_ontology.terms()
        logger.info('Loading %s terms for %s', len(terms), o_ontology.ontology_id)
        if start is not None and end is not None:
            logger.info('Loading terms slice [%s, %s]', start, end)
            logger.info('-----------------------------------------')
            terms = terms[start:end]
            logger.info('Slice len %s', len(terms))
        for o_term in terms:
            with dal.session_scope() as session:
                if o_term.is_defining_ontology and get_accession(o_term):
                    logger.debug('Loaded term (from OLS) %s', o_term)
                    logger.debug('Adding/Retrieving namespaced ontology %s', o_term.obo_name_space)
                    ontology, created = get_one_or_create(Ontology,
                                                          session,
                                                          name=o_ontology.ontology_id,
                                                          namespace=o_term.obo_name_space or '',
                                                          create_method_kwargs=dict(
                                                              version=o_ontology.version,
                                                              title=o_ontology.title))
                    term = self.load_term(o_term, ontology, session)
                    if term:
                        session.add(term)
                    nb_terms += 1
        return nb_terms

    def load_term(self, o_term, ontology, session, process_relation=True):
        if type(ontology) is str:
            m_ontology = self.load_ontology(ontology)
        elif isinstance(ontology, Ontology):
            m_ontology = ontology
        elif isinstance(ontology, helpers.Ontology):
            m_ontology = Ontology(helper=ontology)
        else:
            raise RuntimeError('Wrong parameter')
        session.add(m_ontology)
        if get_accession(o_term):
            m_term, created = get_one_or_create(Term,
                                                session,
                                                accession=o_term.obo_id,
                                                create_method_kwargs=dict(helper=o_term,
                                                                          ontology=m_ontology))

            logger.info('Loaded Term [%s] ...', m_term.accession)
            logger.debug('Detailed Term %s', m_term)
            if created:
                self.load_term_subsets(m_term, session)
                self.load_alt_ids(m_term, o_term, session)
                self.load_term_synonyms(m_term, o_term, session)
                if m_term.ontology.name in self.allowed_ontologies and self.options.get('process_relations', True) \
                        and process_relation:
                    self.load_term_relations(m_term, o_term, session)
                if not m_term.is_root and self.options.get('process_parents', True) and process_relation:
                    self.load_term_ancestors(m_term, o_term, session)
            return m_term
        else:
            return None

    def load_alt_ids(self, m_term, o_term, session):
        session.query(AltId).filter(AltId.term == m_term).delete()
        if o_term.annotation.has_alternative_id:
            logger.info('Loaded AltId %s', o_term.annotation.has_alternative_id)
            for alt_id in o_term.annotation.has_alternative_id:
                get_one_or_create(AltId,
                                  session,
                                  accession=alt_id,
                                  create_method_kwargs=dict(accession=alt_id,
                                                            term=m_term))
            logger.debug('...Done')
        else:
            logger.info('...No AltIds')
        return m_term

    def load_term_subsets(self, term, session):
        subsets = []
        if term.subsets:
            subset_names = term.subsets.split(',')
            for subset_name in subset_names:
                subset = self.load_subset(subset_name, term.ontology.name, session)
                subsets.append(subset) if subset else None
            logger.info('Loaded subsets: %s ', subset_names)
        else:
            logger.info('...No Subset')
        return subsets

    def load_subset(self, subset_name, ontology_name, session):
        logger.debug(' Processing subset %s', subset_name)
        search = self.__call_client('search', query=subset_name, filters={'ontology': ontology_name,
                                                                          'type': 'property'})
        if search and len(search) >= 1:
            prop = helpers.Property(ontology_name=ontology_name, iri=search[0].iri)
            details = self.__call_client('detail', item=prop, unique=True, silent=True)
            if details:
                subset_def = details.definition or details.annotation.get('comment', [''])[0] or subset_name
                if not subset_def:
                    logger.warning('Subset %s has no definition (%s)', subset_name, ontology_name)
                subset, created = get_one_or_create(Subset, session,
                                                    name=subset_name,
                                                    definition=subset_def)
                return subset
            else:
                logger.warning('Unable to retrieve subset details %s for ontology %s', subset_name, ontology_name)

        else:
            logger.warning('Unable to retrieve subset %s (%s) fall back on default', subset_name, ontology_name)
        # default behavior
        subset_def = inflection.humanize(subset_name)
        subset, created = get_one_or_create(Subset, session,
                                            name=subset_name,
                                            definition=subset_def)
        return subset

    def load_term_relations(self, m_term, o_term, session):
        # remove previous relationships
        '''
        old_related = session.query(Relation).join(Relation.relation_type).filter(
            Relation.parent_term == m_term).filter(RelationType.name is not 'is_a')
        for related in old_related.all():
            logger.info('Removing related %s', related.child_term.accession)
            session.delete(related)
        '''
        # TODO check if parent should be removed as well
        # session.query(Relation).filter(Relation.parent_term == m_term).delete()
        relation_types = [rel for rel in o_term.relations_types if rel not in self.__ignored_relations]
        logger.info('Terms relations %s', relation_types)
        n_relations = 0
        for rel_name in relation_types:
            # updates relation types
            o_relatives = o_term.load_relation(rel_name)

            logger.info('Loading %s relation %s (%s)...', m_term.accession, rel_name, rel_name)
            logger.info('%s related terms ', len(o_relatives))
            for o_related in o_relatives:
                if get_accession(o_related) is not None:
                    # o_related.ontology_name in self.allowed_ontologies
                    relation_type, created = get_one_or_create(RelationType,
                                                               session,
                                                               name=self.__relation_map.get(rel_name, rel_name))

                    m_related, relation = self.load_term_relation(m_term, o_related, relation_type, session)
                    n_relations += 1
                    logger.debug('Loading related %s', m_related)
            logger.info('... Done (%s)', n_relations)
        return n_relations

    def rel_dest_ontology(self, m_term, o_term, session):
        accession = get_accession(o_term)
        if o_term.is_defining_ontology:
            logger.debug('Related term is defined in SAME ontology')
            o_term_details = o_term
            r_ontology = m_term.ontology
        else:
            guessed_ontology = accession.split(':')[0].lower()
            logger.debug('Term ontology: %s', guessed_ontology)
            if guessed_ontology not in self.allowed_ontologies:
                logger.debug('Related term is defined in EXTERNAL ontology')
                r_ontology = m_term.ontology
                o_term_details = o_term
            else:
                logger.debug('Related term is defined in EXPECTED ontology')
                o_term_details = self.__call_client('term', identifier=o_term.iri, silent=True, unique=True)
                if o_term_details:
                    o_onto_details = self.__call_client('ontology', identifier=o_term_details.ontology_name)
                    r_ontology, created = get_one_or_create(Ontology,
                                                            session,
                                                            name=o_onto_details.ontology_id,
                                                            namespace=o_term_details.obo_name_space or '',
                                                            create_method_kwargs=dict(
                                                                version=o_onto_details.version,
                                                                title=o_onto_details.title))
        return o_term_details, r_ontology

    def load_term_relation(self, m_term, o_term, relation_type, session):
        accession = get_accession(o_term)
        if accession:
            o_term_details, r_ontology = self.rel_dest_ontology(m_term, o_term, session)
            if o_term_details:
                if get_accession(o_term_details):
                    m_related = self.load_term(o_term=o_term_details, ontology=r_ontology, session=session,
                                               process_relation=False)
                    logger.info('Adding relation %s %s %s', m_term.accession, relation_type.name,
                                m_related.accession)
                    m_relation = m_term.add_child_relation(m_related, relation_type, session)
                    logger.debug('Loaded relation %s %s %s', m_term.accession, relation_type.name, m_related.accession)
                    return m_related, m_relation
            else:
                logger.warning('Term %s (%s) relation %s with %s not found in %s ',
                               m_term.accession, m_term.ontology.name,
                               relation_type.name,
                               o_term.iri, o_term.ontology_name)
        return None, None

    def load_term_ancestors(self, m_term, o_term, session):
        # delete old ancestors
        try:
            ancestors = o_term.load_relation('parents')
            r_ancestors = 0
            relation_type, created = get_one_or_create(RelationType,
                                                       session,
                                                       name='is_a')
            for ancestor in ancestors:
                logger.debug('Parent %s ', ancestor.obo_id)
                if get_accession(ancestor):
                    parent, relation = self.load_term_relation(m_term, ancestor, relation_type, session)
                    if parent is not None:
                        self.load_term_ancestors(parent, ancestor, session)
                        r_ancestors = r_ancestors + 1
            return r_ancestors
        except CoreAPIException as e:
            logger.info('...No parent %s ')
            return 0

    def load_term_synonyms(self, m_term, o_term, session):
        logger.debug('Loading term synonyms...')

        session.query(Synonym).filter(Synonym.term == m_term).delete()
        n_synonyms = 0

        obo_synonyms = o_term.obo_synonym or []
        for synonym in obo_synonyms:
            if isinstance(synonym, itypes.Dict):
                try:
                    db_xref = synonym['xrefs'][0]['database'] or '' + ':' + synonym['xrefs'][0][
                        'id'] if 'xrefs' in synonym and len(synonym['xrefs']) > 0 else ''
                    logger.info('Term synonym [%s - %s (%s)]', synonym['name'], self.__synonym_map[synonym['scope']],
                                db_xref)
                    m_syno, created = get_one_or_create(Synonym,
                                                        session,
                                                        term=m_term,
                                                        name=synonym['name'],
                                                        create_method_kwargs=dict(
                                                            db_xref=db_xref,
                                                            type=self.__synonym_map[synonym['scope']]))
                    n_synonyms += 1 if created else 0
                except KeyError as e:
                    logging.error('Parse Synonym error %s: %s', synonym, str(e))
            else:
                logging.warning('Unable to parse obo_synonym %s', synonym)
        # OBO Xref are winning against standard synonymz
        synonyms = o_term.synonyms or []
        for synonym in synonyms:
            logger.info('Term synonym [%s - EXACT (No dbXref)]', synonym)
            m_syno, created = get_one_or_create(Synonym,
                                                session,
                                                term=m_term,
                                                name=synonym,
                                                create_method_kwargs=dict(
                                                    type='EXACT')
                                                )
            if not created:
                n_synonyms += 1 if created else 0
        if n_synonyms == 0:
            logger.info('...No Synonym')
        logger.debug('...Done')
        return n_synonyms
