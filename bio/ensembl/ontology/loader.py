# -*- coding: utf-8 -*-

import logging

import sqlalchemy

import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.models import *
from ebi.ols.api.client import OlsClient
from .db import *

logger = logging.getLogger(__name__)


class OlsLoader(object):
    """ class loader for mapping retrieved DTO from OLS client into expected database fields """
    __class_map = {
        helpers.Ontology: Ontology,
        helpers.Term: Term,
        helpers.Subset: Subset,
    }

    __relation_map = {
        'parents': 'is_a',
    }

    __syno_map = {
        'hasExactSynonym': 'EXACT',
        'hasBroadSynonym': 'BROAD',
        'hasNarrowSynonym': 'NARROW',
        'hasRelatedSynonym': 'RELATED'
    }

    # TODO check PBQ, FYPO-EXTENSION, FYPO_GO
    ONTOLOGIES_LIST = ['go', 'so', 'pato', 'hpo', 'vt', 'efo', 'po', 'eo', 'to', 'chebi', 'pro', 'fypo',
                       'peco', 'bfo', 'bto', 'cl', 'cmo', 'eco', 'mp', 'ogms', 'uo']  # PBQ? FROM old script order

    def __init__(self, url, **options):
        self._base_url = url
        self.options = options
        self.client = OlsClient()
        engine = sqlalchemy.create_engine(self._base_url,
                                          pool_recycle=self.options.get('timeout', 36000),
                                          echo=self.options.get('echo', False))
        Session.configure(bind=engine)
        self.session = Session()

    @contextlib.contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        # self.session = Session()
        if not self.session.is_active:
            self.session = Session()
        logger.debug('Open session')
        try:
            yield self.session
            logger.debug('Commit session')
            self.session.commit()
        except Exception as e:
            logger.exception('Error in session %s', e)
            self.session.rollback()
            raise
        finally:
            logger.debug('Closing session')
            self.session.close()

    def get_or_create(self, model, create_method='', create_method_kwargs=None, **kwargs):
        try:
            logger.debug('querying model %s: %s', model, kwargs)
            obj = self.session.query(model).filter_by(**kwargs).one()
            return obj, False
        except NoResultFound:
            kwargs.update(create_method_kwargs or {})
            logger.debug('created %s', kwargs)
            created = getattr(model, create_method, model)(**kwargs)
            try:
                self.session.add(created)
                self.session.flush()
                # print('object is session', created in self.session)
                logger.debug('object created and flushed %s', created)
                return created, True
            except IntegrityError:
                self.session.rollback()
                if create_method_kwargs:
                    [kwargs.pop(key) for key in create_method_kwargs.keys()]
                logger.debug('search %s', kwargs)
                return self.session.query(model).filter_by(**kwargs).one(), False

    def exists(self, model, **kwargs):
        try:
            return self.session.query(model).filter_by(**kwargs).one()
        except NoResultFound:
            return False

    def create_schema(self):
        with self.session_scope():
            if self.options.get('reset', False) is False:
                self.drop_schema()
            Base.metadata.create_all(self.session.bind)

    def load_all(self, ontology_name=None):
        if ontology_name is not None:
            s = Session()
            m_ontology = self.load_ontology(ontology_name)
            s.add(m_ontology)
            # load terms
            terms = self.load_ontology_terms(m_ontology)
            for term in terms:
                s.add(term)
                # load subsets
                s.bulk_save_objects(self.load_term_subsets(term))
                # load synonyms ?
                # load relations
                # load alt_id
            s.commit()
        else:
            # run process for all defaults ontologies setup
            for ontology in self.ONTOLOGIES_LIST:
                self.run_all(ontology)

    def _ontology_api(self, ontology_name):
        return self.client.ontology(ontology_name)

    def _ontology_model(self, o_ontology: helpers.Ontology):
        m_ontology, created = self.get_or_create(Ontology, name=o_ontology.ontology_id,
                                                 create_method_kwargs={'helper': o_ontology})
        return m_ontology

    def load_ontology(self, ontology_name):
        return self._ontology_model(self._ontology_api(ontology_name))

    def load_ontology_terms(self, ontology):
        with self.session_scope():
            # todo delete current terms ?
            nb_terms = 0
            if type(ontology) is str:
                m_ontology = self.load_ontology(ontology)
                terms = self.client.ontology(ontology).terms()
            elif isinstance(ontology, Ontology):
                m_ontology = Ontology
                terms = self.client.ontology(m_ontology.name).terms()
            elif isinstance(ontology, helpers.Ontology):
                m_ontology = Ontology(ontology)
                terms = ontology.terms()
                # session.merge(m_ontology)
            else:
                raise RuntimeError('Wrong parameter')
            # print(m_ontology.namespace)
            terms_onto = {m_ontology.namespace: m_ontology}
            logger.debug('Loaded ontology terms: %s', len(terms))
            for o_term in terms:
                if o_term.is_defining_ontology and o_term.obo_id:
                    if o_term.obo_name_space not in terms_onto:
                        terms_onto[o_term.obo_name_space] = self._get_ontology_namespaced(m_ontology,
                                                                                          o_term.obo_name_space)

                    m_term = Term(o_term)
                    m_term.ontology = terms_onto[o_term.obo_name_space]

                    self.session.add(m_term)
                    nb_terms += 1
            return nb_terms

    def _get_ontology_namespaced(self, m_ontology, namespace):
        t_ontology, created = self.get_or_create(Ontology, name=m_ontology.name, namespace=namespace,
                                                 create_method_kwargs=dict(version=m_ontology.version,
                                                                           title=m_ontology.title))
        return t_ontology

    def load_term_subsets(self, term: Term):
        with self.session_scope():
            subsets = 0
            for subset_name in term.subsets.split(','):
                search = self.client.search(query=subset_name, filters={'ontology': term.ontology.name,
                                                                        'type': 'property'})
                if len(search) == 1:
                    details = self.client.detail(search[0])
                    subset, created = self.get_or_create(Subset, name=subset_name,
                                                         definition=details.definition)
                    if created:
                        self.session.add(subset)
                        subsets += 1
        return subsets

    def _term_api(self, iri, ontology_name=None, unique=True):
        if ontology_name is not None:
            return self.client.detail(ontology_name=ontology_name, iri=iri, item=helpers.Term, unique=unique)
        else:
            return self.client.term(identifier=iri, unique=True, silent=True)

    def _term_object(self, o_term: helpers.Term):
        m_ontology = self.load_ontology(o_term.ontology_name)
        m_term, created = self.get_or_create(Term, accession=o_term.accession,
                                             create_method_kwargs=dict(ontology=m_ontology,
                                                                       helper=o_term
                                                                       ))
        return m_term

    def load_term(self, iri, ontology_name=None):
        o_term = self._term_api(iri, ontology_name)
        with self.session_scope():
            m_term = self._term_object(o_term)
            self.session.query(AltId).filter_by(term=m_term).delete()
            for alt_id in o_term.annotation.has_alternative_id:
                m_term.alt_accession.append(AltId(accession=alt_id))
            print(m_term.alt_accession)
        return m_term

    def load_term_relation(self, term_iri, ontology_name, relation_name):
        with self.session_scope():
            m_term = self.load_term(term_iri, ontology_name)
            rel_name = self.__relation_map.get(relation_name, relation_name)
            logger.debug('loading relation %s mapped to ols link %s', rel_name, relation_name)
            relation_type, created = self.get_or_create(RelationType, name=rel_name)
            o_term = helpers.Term(ontology_name=m_term.ontology.name, iri=m_term.iri)
            o_relatives = o_term.load_relation(relation_name)
            logger.debug('%s term parents count %s', m_term.name, len(o_relatives))
            n_relations = 0
            for o_related in o_relatives:
                if o_related.accession is not None:
                    if o_related.is_defining_ontology:
                        m_related, created = self.get_or_create(Term,
                                                                create_method_kwargs=dict(helper=o_related),
                                                                accession=o_related.obo_id,
                                                                ontology=m_term.ontology)
                    else:
                        # Search in all ontologies to find where it's defined
                        m_related = self.load_term(o_related.iri)
                        # print('loaded from another ', m_related.ontology.name)
                    relation, created = self.get_or_create(Relation,
                                                           child_term=m_related,
                                                           parent_term=m_term,
                                                           relation_type=relation_type,
                                                           ontology=m_term.ontology)
                    n_relations += 1
                    assert (m_related in self.session)
                    assert (relation in self.session)
            self.session.flush(m_term)
            return n_relations

    def load_term_synonyms(self, term_iri, ontology_name):

        with self.session_scope():
            o_term = self._term_api(term_iri, ontology_name)
            m_term = self._term_object(o_term)
            dels = self.session.query(Synonym).filter_by(term=m_term).delete()
            for synonym in o_term.obo_synonym:
                m_syno = Synonym(term=m_term, name=synonym['name'],
                                 db_xref=synonym['xrefs'][0]['database'] + ':' + synonym['xrefs'][0]['id'])
                m_syno.type = self.__syno_map[synonym['scope']]
                self.session.add(m_syno)
                print(m_syno)
                print(o_term.obo_synonym)
            pass
