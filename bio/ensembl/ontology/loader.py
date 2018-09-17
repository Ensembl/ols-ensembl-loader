#  -*- coding: utf-8 -*-

import datetime

import dateutil.parser

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

    # TODO check PBQ, FYPO-EXTENSION, FYPO_GO
    ONTOLOGIES_LIST = ['go', 'so', 'pato', 'hp', 'vt', 'efo', 'po', 'eo', 'to', 'chebi', 'pr', 'fypo', 'peco', 'bfo',
                       'bto', 'cl', 'cmo', 'eco', 'mp', 'ogms', 'uo']  # PBQ? FROM old script order

    _session = None

    _default_options = dict(
        echo=False,
        wipe=False,
    )

    def __init__(self, url, **options):
        self.db_url = url
        self.options = self._default_options
        self.options.update(options)
        self.client = OlsClient()
        dal.db_init(self.db_url, **self.options)

    def db_init(self):
        dal.db_init(self.db_url, **self.options)

    def init_meta(self):
        metas = {
            'schema_version': self.options.get('db_version'),
            'schema_type': 'ontology'
        }
        for meta_key, meta_value in metas.items():
            get_one_or_create(Meta, meta_key=meta_key,
                              create_method_kwargs=dict(meta_value=meta_value))

    @property
    def session(self):
        return dal.get_session()

    def load(self, ontology_name):
        # run process for all defaults ontologies setup
        for ontology in self.ONTOLOGIES_LIST:
            m_ontology = self.load_ontology(ontology)
            self.load_ontology_terms(m_ontology)

    def load_ontology(self, ontology_name, namespace=None):
        if self.options.get('wipe', False):
            logger.debug('Removing ontology %s', ontology_name)
            self.wipe_ontology(ontology_name)
        o_ontology = self.client.ontology(ontology_name)
        get_one_or_create(Meta, meta_key=ontology_name + '_file_date',
                          create_method_kwargs=dict(
                              meta_value=dateutil.parser.parse(o_ontology.updated).strftime('%c')))
        get_one_or_create(Meta, meta_key=ontology_name + '_load_date',
                          create_method_kwargs=dict(meta_value=datetime.datetime.now().strftime('%c')))

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
                ontologies = session.query(Ontology).filter_by(name=ontology_name)
                for ontology in ontologies:
                    session.delete(ontology)
                return True
            except NoResultFound:
                logger.debug('Ontology not found')
        return False

    def load_term_relations(self, m_term, relation_type):
        rel_name = self.__relation_map.get(relation_type.name, relation_type.name)
        logger.info('   Loading %s relation %s ...', m_term.accession, rel_name)
        o_term = helpers.Term(ontology_name=m_term.ontology.name, iri=m_term.iri)
        o_relatives = o_term.load_relation(relation_type.name)
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
                        ro_term = self.client.term(identifier=o_related.iri, unique=True)
                        if ro_term is not None and ro_term.ontology_name in self.ONTOLOGIES_LIST:
                            logger.debug('  Term is defined in another expected ontology: %s', ro_term.ontology_name)
                            # load ontology
                            o_onto_details = self.client.ontology(ro_term.ontology_name)
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
                        # FIXME what to do when term listed in onto is not from this onto ?
                        # m_related = self.load_term(o_related.iri)
                        relation, r_created = get_one_or_create(Relation,
                                                                parent_term=m_related,
                                                                child_term=m_term,
                                                                relation_type=relation_type,
                                                                ontology=m_term.ontology)
                        n_relations += 1 if r_created else None
                        logger.info('Loaded relation %s %s %s', m_term.accession, rel_name, m_related.accession)
            else:
                logger.warning('This term is not in current ontology, neither defining one %s', o_related)
        logger.info('   ... Done')
        return n_relations

    def _load_term_synonyms(self, m_term: Term, o_term: helpers.Term):
        logger.info('   Loading term synonyms...')
        session = dal.get_session()
        session.query(Synonym).filter(Synonym.term_id == m_term.term_id).delete()
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
                n_synonyms += 1 if created else None
        # OBO Xref are winning against standard synonymz
        synonyms = o_term.synonyms or []
        for synonym in synonyms:
            logger.info('   Term synonym %s - EXACT - No dbXref', synonym)
            m_syno, created = get_one_or_create(Synonym, term=m_term, name=synonym, type='EXACT')
            n_synonyms += 1 if created else None

        logger.info('   ... Done')
        return n_synonyms

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
        if created:
            logger.info('Create term %s ...', m_term)
            self.load_term_subsets(m_term)
            types  = o_term.relations_types + ['parents']
            relation_types = [rel for rel in types if rel not in ('children')]
            for relation in relation_types:
                # updates relation types
                relation_type, created = get_one_or_create(RelationType,
                                                           name=self.__relation_map.get(relation, relation))
                self.load_term_relations(m_term, relation_type)
            self._load_term_synonyms(m_term, o_term)
            for alt_id in o_term.annotation.has_alternative_id:
                logger.info('Loaded AltId %s', alt_id)
                m_term.alt_ids.append(AltId(accession=alt_id))
            logger.info('... Done')
        return m_term

    def load_term_subsets(self, term: Term):
        subsets = 0
        logger.info('   Loading term subsets')
        for subset_name in term.subsets.split(','):
            logger.debug('      Processing subset %s', subset_name)
            search = self.client.search(query=subset_name, filters={'ontology': term.ontology.name,
                                                                    'type': 'property'})
            if len(search) == 1:
                details = self.client.detail(search[0])
                subset, created = get_one_or_create(Subset, name=subset_name,
                                                    definition=details.definition or '')
                if created:
                    logger.info('      Created subset [%s: %s]', subset.subset_id, subset_name)
                    subsets += 1
        logger.info('   ... Done')
        return subsets
