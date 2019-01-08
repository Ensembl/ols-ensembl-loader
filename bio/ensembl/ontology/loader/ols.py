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
from os.path import join

import inflection
import itypes
from coreapi.exceptions import CoreAPIException
from sqlalchemy.orm.exc import NoResultFound

import ebi.ols.api.exceptions
import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.loader.db import dal
from bio.ensembl.ontology.loader.models import *
from ebi.ols.api.client import OlsClient

logger = logging.getLogger(__name__)


def has_accession(o_term):
    return o_term.accession is not None


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
        process_parents=True,
        page_size=1000,
        output_dir=getenv("HOME")
    )

    allowed_ontologies = ['go', 'so', 'pato', 'hp', 'vt', 'efo', 'po', 'eo', 'to', 'chebi', 'pr', 'fypo', 'peco', 'bfo',
                          'bto', 'cl', 'cmo', 'eco', 'mod', 'mp', 'ogms', 'uo', 'mondo', 'phi']

    def __init__(self, url, **options):
        self.db_url = url
        self.options = self._default_options
        self.options.update(options)
        self.client = OlsClient(page_size=self.options.get('page_size'))
        self.retry = 0
        self.db_init = False
        dal.db_init(self.db_url, **self.options)
        logger.info('Loaded with options %s ', self.options)
        self.current_ontology = None

    def get_report_logger(self):
        report_logger = logging.getLogger(self.current_ontology + '_report')
        report_logger.setLevel(logging.INFO)
        if not len(report_logger.handlers):
            log_file = '{}_report.log'
            ols_report_handler = logging.FileHandler(
                join(self.options.get('output_dir'), log_file.format(self.current_ontology)))
            formatter = logging.Formatter('%(asctime)s:  - \t%(message)s')
            ols_report_handler.setFormatter(formatter)
            report_logger.addHandler(ols_report_handler)
        return report_logger

    def report(self, *messages):
        report = self.get_report_logger()
        if len(messages) > 1:
            report.info(messages[0] % messages[1:])
        else:
            report.info(messages[0])

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

    def load_ontology(self, ontology, namespace=''):
        """
        Load single ontology data from OLS API.
        Update
        :param ontology:
        :param namespace:
        :return: an Ontology model object.
        """
        if type(ontology) is str:
            ontology = self.client.ontology(identifier=ontology)
        elif not isinstance(ontology, helpers.Ontology):
            raise RuntimeError('Wrong parameter')
        ontology_name = ontology.ontology_id
        self.current_ontology = ontology_name
        with dal.session_scope() as session:
            self.report('----------------------------------')
            self.report('Ontology [%s] - %s:' % (ontology_name, ontology.config.title))
            self.report('- Number of terms: %s' % ontology.number_of_terms)
            self.report('- Number of individuals: %s' % ontology.number_of_individuals)
            self.report('- Number of properties: %s' % ontology.number_of_properties)
            m_ontology, created = get_one_or_create(Ontology,
                                                    session,
                                                    name=ontology.ontology_id,
                                                    namespace=namespace or ontology_name,
                                                    create_method_kwargs={'helper': ontology})

            start = datetime.datetime.now()
            logger.debug('Updating meta for ontology %s', ontology_name)
            get_one_or_create(Meta,
                              session,
                              meta_key=ontology_name + '_load_date',
                              create_method_kwargs=dict(
                                  meta_value=ontology_name.upper() + '/' + start.strftime('%c')))
            try:
                updated_at = datetime.datetime.strptime(ontology.updated, '%Y-%m-%dT%H:%M:%S.%f%z')
            except ValueError:
                # Default update to current date time
                updated_at = datetime.datetime.now()
            meta, created = get_one_or_create(Meta,
                                              session,
                                              meta_key=ontology.ontology_id + '_file_date',
                                              create_method_kwargs=dict(
                                                  meta_value=ontology.ontology_id.upper() + '/' + updated_at.strftime(
                                                      '%c')))
            self.report('- {}/{}'.format(meta.meta_key, meta.meta_value))
            self.report('----------------------------------')
            logger.info('Loaded [%s/%s] %s', m_ontology.name, m_ontology.namespace, m_ontology.title)
            return m_ontology
        return None

    @staticmethod
    def wipe_ontology(ontology_name):
        """
        Completely remove all ontology related data from DBs
        :param ontology_name: specified ontology short name
        :return: boolean whether or not Ontology has been successfully deleted
        """
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
                    logger.debug('...Done')
                return True
            except NoResultFound:
                logger.error('Ontology %s not found !', ontology_name)
        return False

    def load_ontology_terms(self, ontology, start=None, end=None):
        self.current_ontology = ontology
        nb_terms = 0
        nb_terms_ignored = 0
        o_ontology = self.client.ontology(identifier=ontology)
        if o_ontology:
            self.current_ontology = o_ontology.ontology_id
            terms = o_ontology.terms()
            logger.info('Loading %s terms for %s', len(terms), o_ontology.ontology_id)
            if start is not None and end is not None:
                logger.info('Loading terms slice [%s, %s]', start, end)
                logger.info('-----------------------------------------')
                terms = terms[start:end]
                logger.info('Slice len %s', len(terms))
                report_msg = ('- Loading %s terms slice [%s:%s]', ontology, start, end)
            else:
                report_msg = ('- Loading all terms (%s)', len(terms))
            with dal.session_scope() as session:
                for o_term in terms:
                    if o_term.is_defining_ontology and has_accession(o_term):
                        m_ontology, created = get_one_or_create(Ontology,
                                                                session,
                                                                name=o_ontology.ontology_id,
                                                                namespace=o_term.obo_name_space,
                                                                create_method_kwargs=dict(
                                                                    version=o_ontology.version,
                                                                    title=o_ontology.title))
                        logger.debug('Loaded term (from OLS) %s', o_term)
                        logger.debug('Adding/Retrieving namespaced ontology %s', o_term.obo_name_space)
                        term = self.load_term(o_term, m_ontology, session)
                        if term:
                            session.add(term)
                            nb_terms += 1
                    else:
                        logger.info('Ignored term [%s:%s]', o_term.is_defining_ontology, o_term.short_form)
                        nb_terms_ignored += 1
                self.report(*report_msg)
                self.report('- Expected %s terms (defined in ontology)', nb_terms)
                self.report('- Ignored %s terms (not defined in ontology)', nb_terms_ignored)
                return nb_terms, nb_terms_ignored
        return None

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

        if has_accession(o_term):
            if not o_term.description:
                o_term.description = [inflection.humanize(o_term.label)]
            m_term, created = get_one_or_create(Term,
                                                session,
                                                accession=o_term.accession,
                                                create_method_kwargs=dict(helper=o_term,
                                                                          ontology=m_ontology))

            logger.info('Loaded Term [%s][%s] ...', m_term.accession, o_term.iri)
            logger.debug('%s => %s', m_term.accession, m_term)
            if created:
                self.load_term_subsets(m_term, session)
                self.load_alt_ids(m_term, o_term, session)
                self.load_term_synonyms(m_term, o_term, session)
                if m_term.ontology.name in self.allowed_ontologies and self.options.get('process_relations', True) \
                        and process_relation:
                    self.load_term_relations(m_term, o_term, session)
                if not m_term.is_root and self.options.get('process_parents', True):
                    self.load_term_ancestors(m_term, o_term, session)
            return m_term
        else:
            return None

    def load_alt_ids(self, m_term, o_term, session):
        session.query(AltId).filter(AltId.term == m_term).delete()
        if o_term.annotation.has_alternative_id:
            logger.info('Loaded AltId %s', o_term.annotation.has_alternative_id)
            [m_term.alt_ids.append(AltId(accession=alt_id, term=m_term)) for alt_id in
             o_term.annotation.has_alternative_id]
            logger.debug('...Done')
        else:
            logger.info('...No AltIds')
        return m_term

    def load_term_subsets(self, term, session):
        subsets = []
        if term.subsets:
            s_subsets = self.client.search(query=term.subsets, type='property')
            seen = set()
            unique_subsets = [x for x in s_subsets if
                              x.short_form.lower() not in seen and not seen.add(x.short_form.lower())]
            for subset in unique_subsets:
                subset_def = inflection.humanize(subset.label)
                m_subset, created = get_one_or_create(Subset, session,
                                                      name=inflection.underscore(subset.label),
                                                      create_method_kwargs=dict(
                                                          definition=subset_def))
                if created:
                    # avoid call to API if already exists
                    try:
                        details = self.client.property(identifier=subset.iri)
                        if not details:
                            logger.warning('Unable to retrieve subset details %s for ontology %s', subset.label,
                                           term.ontology.name)
                        else:
                            m_subset.definition = details.definition
                            session.merge(m_subset)
                            session.commit()
                    except ebi.ols.api.exceptions.ObjectNotRetrievedError:
                        logger.error('Too Many errors from API %s %s', subset.label, term.ontology.name)
            logger.info('Loaded subsets: %s ', subsets)
        else:
            logger.info('...No Subset')
        return subsets

    def load_term_relations(self, m_term, o_term, session):
        relation_types = [rel for rel in o_term.relations_types if rel not in self.__ignored_relations]
        logger.info('Terms relations %s', relation_types)
        n_relations = 0
        for rel_name in relation_types:
            # updates relation types
            o_relatives = o_term.load_relation(rel_name)

            logger.info('Loading %s relation %s (%s)...', m_term.accession, rel_name, rel_name)
            logger.info('%s related terms ', len(o_relatives))
            for o_related in o_relatives:
                if has_accession(o_related):
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
        if o_term.is_defining_ontology:
            logger.debug('Related term is defined in SAME ontology')
            o_term_details = o_term
            r_ontology = m_term.ontology
            return o_term_details, r_ontology
        else:
            if has_accession(o_term):
                guessed_ontology = o_term.accession.split(':')[0].lower()
                logger.debug('Term ontology: %s', guessed_ontology)
                if guessed_ontology not in self.allowed_ontologies:
                    logger.debug('Related term is defined in EXTERNAL ontology')
                    r_ontology = m_term.ontology
                    o_term_details = o_term
                    return o_term_details, r_ontology
                else:
                    logger.debug('Related term is defined in EXPECTED ontology')
                    o_term_details = self.client.term(identifier=o_term.iri, silent=True, unique=True)
                    if o_term_details:
                        o_onto_details = self.client.ontology(identifier=o_term_details.ontology_name)
                        if o_onto_details:
                            r_ontology, created = get_one_or_create(Ontology,
                                                                    session,
                                                                    name=o_onto_details.ontology_id,
                                                                    namespace=o_term_details.obo_name_space or '',
                                                                    create_method_kwargs=dict(
                                                                        version=o_onto_details.version,
                                                                        title=o_onto_details.title))
                            return o_term_details, r_ontology
        return None, None

    def load_term_relation(self, m_term, o_term, relation_type, session):

        if has_accession(o_term):
            o_term_details, r_ontology = self.rel_dest_ontology(m_term, o_term, session)
            if o_term_details and has_accession(o_term_details):
                m_related = self.load_term(o_term=o_term_details, ontology=r_ontology, session=session,
                                           process_relation=False)
                logger.info('Adding relation %s %s %s', m_term.accession, relation_type.name,
                            m_related.accession)
                m_relation = m_term.add_parent_relation(m_related, relation_type, session)
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
                logger.debug('Parent %s ', ancestor.accession)
                if has_accession(ancestor):
                    parent, relation = self.load_term_relation(m_term, ancestor, relation_type, session)
                    if parent:
                        r_ancestors = r_ancestors + 1
            return r_ancestors
        except CoreAPIException as e:
            logger.info('...No parent %s ')
            return 0

    def load_term_synonyms(self, m_term, o_term, session):
        logger.debug('Loading term synonyms...')

        session.query(Synonym).filter(Synonym.term == m_term).delete()
        n_synonyms = []

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
                    if created:
                        n_synonyms.append(synonym)
                except KeyError as e:
                    logging.error('Parse Synonym error %s: %s', synonym, str(e))
            else:
                logging.error('obo_synonym type error: %s', synonym)
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
            if created:
                n_synonyms.append(synonym)
        if len(n_synonyms) == 0:
            logger.info('...No Synonym')
        logger.debug('...Done')
        return n_synonyms

    def final_report(self, ontology_name):
        """ Create a report from actual inserted data for ontology """
        session = dal.get_session()
        ontologies = session.query(Ontology).filter_by(name=ontology_name).all()
        self.current_ontology = ontology_name
        for ontology in ontologies:
            synonyms = session.query(Synonym).filter(Synonym.term_id == Term.term_id,
                                                     Term.ontology_id == ontology.id).count()
            relations = session.query(Relation).filter(Relation.ontology == ontology).count()
            closures = session.query(Closure).filter(Closure.ontology == ontology).count()
            alt_ids = session.query(AltId).filter(AltId.term_id == Term.term_id,
                                                  Term.ontology_id == ontology.id).count()
            terms = session.query(Term).filter(Term.ontology == ontology).count()
            repeat = len('Ontology %s / Namespace %s' % (ontology.name, ontology.namespace))
            self.report('-' * repeat)
            self.report('Ontology %s / Namespace %s', ontology.name, ontology.namespace)
            self.report('-' * repeat)
            self.report('- Imported Terms %s', terms)
            self.report('- Imported Relations %s', relations)
            self.report('- Imported Alt Ids %s', alt_ids)
            self.report('- Imported Synonyms %s', synonyms)
            self.report('- Generated Closure %s', closures)
