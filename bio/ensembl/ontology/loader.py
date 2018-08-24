#  -*- coding: utf-8 -*-

import logging

import sqlalchemy

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
        self._session = Session()
        logger.debug('Loaded Options %s', self.options)

    @contextlib.contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        # self.session = Session()
        if not self._session.is_active:
            self._session = Session()
        logger.debug('Open session')
        try:
            yield self._session
            logger.debug('Commit session')
            self._session.commit()
        except Exception as e:
            logger.exception('Error in session %s', e)
            self._session.rollback()
            raise
        finally:
            logger.debug('Closing session')
            self._session.close()

    def get_or_create(self, model, create_method='', create_method_kwargs=None, **kwargs):
        try:
            obj = self._session.query(model).filter_by(**kwargs).one()
            logger.debug('Existing entity %s: %s', model, kwargs)

            return obj, False
        except NoResultFound:
            try:
                kwargs.update(create_method_kwargs or {})
                logger.debug('Not existing %s entity for params %s', model, kwargs)
                created = getattr(model, create_method, model)(**kwargs)
                self._session.add(created)
                self._session.flush()
                # print('object is session', created in self.session)
                logger.debug('Created: %s', created)
                return created, True
            except IntegrityError:
                logger.info('Integrity error upon flush')
                self._session.rollback()
                if create_method_kwargs is not None:
                    [kwargs.pop(key) for key in create_method_kwargs.keys()]
                logger.debug('%s', kwargs)
                return self._session.query(model).filter_by(**kwargs).one(), False

    def exists(self, model, **kwargs):
        try:
            return self._session.query(model).filter_by(**kwargs).one()
        except NoResultFound:
            return False

    def create_schema(self):
        with self.session_scope():
            if self.options.get('drop', False) is True:
                logger.debug('Dropping all tables')
                Base.metadata.drop_all(self._session.bind)
            Base.metadata.create_all(self._session.bind)

    def load(self, ontology_name='all'):
        if ontology_name != 'all':
            self.load_all(ontology_name)
        else:
            # run process for all defaults ontologies setup
            for ontology in self.ONTOLOGIES_LIST:
                self.load_all(ontology)

    def load_all(self, ontology_name):
        with self.session_scope():
            m_ontology = self.load_ontology(ontology_name)
            logger.info('Loaded ontology %s', m_ontology)
            n_terms = self.load_ontology_terms(m_ontology)
            logger.info('Loaded %s terms for ontology %s', n_terms, m_ontology.title)

    def wipe_ontology(self, ontology_name):
        with self.session_scope():
            try:
                logger.debug('Trying to delete ontology %s', ontology_name)
                obj = self._session.query(Ontology).filter_by(name=ontology_name).one()
                return obj, False
            except NoResultFound:
                logger.debug('Ontology didn\'t exists')

    def _ontology_api(self, ontology_name):
        return self.client.ontology(ontology_name)

    def _ontology_model(self, o_ontology: helpers.Ontology):
        m_ontology, created = self.get_or_create(Ontology, name=o_ontology.ontology_id,
                                                 create_method_kwargs={'helper': o_ontology})
        return m_ontology

    def load_ontology(self, ontology_name):
        return self._ontology_model(self._ontology_api(ontology_name))

    def load_ontology_terms(self, ontology):
        # todo delete current terms ?
        nb_terms = 0
        if type(ontology) is str:
            m_ontology = self.load_ontology(ontology)
            terms = self.client.ontology(ontology).terms()
        elif isinstance(ontology, Ontology):
            m_ontology = ontology
            terms = self.client.ontology(ontology.name).terms()
        elif isinstance(ontology, helpers.Ontology):
            m_ontology = Ontology(ontology)
            terms = ontology.terms()
            # session.merge(m_ontology)
        else:
            raise RuntimeError('Wrong parameter')
        # print(m_ontology.namespace)
        terms_onto = {m_ontology.namespace: m_ontology}
        logger.debug('Loaded ontology terms: %s', len(terms))
        print(terms_onto)
        # delete all previous terms
        for o_term in terms:
            if o_term.is_defining_ontology and o_term.obo_id:
                logger.debug('Loaded term %s', o_term)
                if o_term.obo_name_space not in terms_onto.keys():
                    print('search again ')
                    terms_onto[o_term.obo_name_space] = self._get_ontology_namespaced(m_ontology.name,
                                                                                      o_term.obo_name_space,
                                                                                      version=m_ontology.version,
                                                                                      title=m_ontology.title)

                m_term = Term(o_term)
                m_term.ontology = terms_onto[o_term.obo_name_space]
                self._session.add(m_term)
                # self._session.flush()
                self.load_term_subsets(m_term)
                #for relation in o_term.relations_types:
                #    self.load_term_relation(o_term.iri, o_term.ontology_name, relation)
                #self.load_term_synonyms(o_term.iri, o_term.ontology_name)
                self._session.flush()
                nb_terms += 1
        return nb_terms

    def _get_ontology_namespaced(self, name, namespace, **kwargs):
        print('namespaced searh ', name, namespace, kwargs)
        t_ontology, created = self.get_or_create(Ontology, name=name, _namespace=namespace, create_method_kwargs=kwargs)
        return t_ontology

    def load_term_subsets(self, term: Term):
        subsets = 0
        for subset_name in term.subsets.split(','):
            search = self.client.search(query=subset_name, filters={'ontology': term.ontology.name,
                                                                    'type': 'property'})
            if len(search) == 1:
                details = self.client.detail(search[0])
                subset, created = self.get_or_create(Subset, name=subset_name,
                                                     definition=details.definition)
                if created:
                    self._session.add(subset)
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
                                                                       helper=o_term))
        return m_term

    def load_term(self, iri, ontology_name=None):
        o_term = self._term_api(iri, ontology_name)
        m_term = self._term_object(o_term)
        self._session.query(AltId).filter_by(term=m_term).delete()
        logger.debug('Loaded term %s', m_term.alt_accession)
        for alt_id in o_term.annotation.has_alternative_id:
            logger.debug('Loaded AltId %s', alt_id)
            m_term.alt_accession.append(AltId(accession=alt_id))
        return m_term

    def load_term_relation(self, term_iri, ontology_name, relation_name):
        m_term = self.load_term(term_iri, ontology_name)
        rel_name = self.__relation_map.get(relation_name, relation_name)
        logger.debug('loading relation %s mapped to ols link %s', rel_name, relation_name)
        relation_type, created = self.get_or_create(RelationType, name=rel_name)
        o_term = helpers.Term(ontology_name=ontology_name, iri=term_iri)
        o_relatives = o_term.load_relation(relation_name)
        # logger.debug('%s term parents count %s', m_term.name, len(o_relatives))
        n_relations = 0
        # self.session.commit()
        for o_related in o_relatives:
            skip = False
            if o_related.accession is not None:
                if o_related.is_defining_ontology:
                    m_related, created = self.get_or_create(Term,
                                                            create_method_kwargs=dict(helper=o_related),
                                                            accession=o_related.obo_id,
                                                            ontology=m_term.ontology)
                else:
                    # Search in all ontologies to find where it's defined
                    logger.debug('The term %s does not belong to ontology %s', o_related.accession, ontology_name)
                    ro_term = self.client.term(identifier=o_related.iri, unique=True)
                    if ro_term is not None and ro_term.ontology_name in self.ONTOLOGIES_LIST:
                        logger.debug('Term is defined in another expected ontology: %s', ro_term.ontology_name)
                        # load ontology
                        o_onto_details = self.client.ontology(ro_term.ontology_name)
                        mo_ontology = self._get_ontology_namespaced(o_onto_details.ontology_id,
                                                                    ro_term.obo_name_space,
                                                                    version=o_onto_details.version,
                                                                    title=o_onto_details.title)
                        m_related, created = self.get_or_create(Term,
                                                                create_method_kwargs=dict(helper=o_related),
                                                                accession=o_related.obo_id,
                                                                ontology=mo_ontology)
                    else:
                        skip = True
                    # FIXME what to do when term listed in onto is not from this onto ?
                    # m_related = self.load_term(o_related.iri)
                if not skip:
                    relation, created = self.get_or_create(Relation,
                                                           child_term=m_related,
                                                           parent_term=m_term,
                                                           relation_type=relation_type,
                                                           ontology=m_term.ontology)
                    n_relations += 1
                    assert (m_related in self._session)
                    assert (relation in self._session)
                    # print('loaded from another ', m_related.ontology.name)
        self._session.flush(m_term)
        return n_relations

    def load_term_synonyms(self, term_iri, ontology_name):
        o_term = self._term_api(term_iri, ontology_name)
        m_term = self._term_object(o_term)
        dels = self._session.query(Synonym).filter_by(term=m_term).delete()
        if o_term.obo_synonym:
            for synonym in o_term.obo_synonym:
                m_syno = Synonym(term=m_term, name=synonym['name'],
                                 db_xref=synonym['xrefs'][0]['database'] + ':' + synonym['xrefs'][0]['id'])
                m_syno.type = self.__syno_map[synonym['scope']]
                self._session.add(m_syno)
                print(m_syno)
                print(o_term.obo_synonym)
        pass
