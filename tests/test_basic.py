# -*- coding: utf-8 -*-
import unittest
import warnings

from bio.ensembl.ontology.db import *
from bio.ensembl.ontology.loader import OlsLoader
from bio.ensembl.ontology.models import *
from ebi.ols.api.client import OlsClient
import ebi.ols.api.helpers as helpers
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s : %(name)s.%(funcName)s(%(lineno)d) - %(message)s',
                    datefmt='%m-%d %H:%M - %s')

logger = logging.getLogger(__name__)

logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)


def ignore_warnings(test_func):
    def do_test(self, *args, **kwargs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", ResourceWarning)
            test_func(self, *args, **kwargs)

    return do_test


class TestLoading(unittest.TestCase):
    _multiprocess_shared_ = True
    db_url = 'sqlite://'

    @ignore_warnings
    def testInitDb(self):
        loader = OlsLoader(self.db_url, echo=True)
        for table_name in ['meta', 'ontology', 'relation_type', 'subset', 'term', 'alt_id', 'closure', 'relation',
                           'synonym']:
            self.assertIn(table_name, Base.metadata.tables)

        with dal.session_scope() as session:
            for meta_info in Base.__subclasses__():
                self.assertIsNotNone(session.query(meta_info).all())

    @ignore_warnings
    def testLoadOntology(self):
        # test retrieve
        # test try to create duplicated
        loader = OlsLoader(self.db_url)
        session = dal.get_session()
        ontology_name = 'cvdo'
        m_ontology = loader.load_ontology(ontology_name)
        logger.info('Loaded ontology %s', m_ontology)
        r_ontology = session.query(Ontology).filter_by(name=ontology_name,
                                                       namespace=m_ontology.namespace).one()
        logger.info('(RE) Loaded ontology %s', r_ontology)
        self.assertEqual(m_ontology.name, r_ontology.name)
        self.assertEqual(m_ontology.version, r_ontology.version)
        assert isinstance(r_ontology, Ontology)
        # automatically create another one with another namespace
        new_ontology, created = loader.get_or_create(Ontology,
                                                     name=r_ontology.name,
                                                     namespace='another_namespace')
        for i in range(0, 5):
            session.add(Term(ontology=m_ontology, accession='CCC_00000{}'.format(i), name='Term {}'.format(i),
                        is_root=False, is_obsolete=False))

        session.commit()
        self.assertEqual(session.query(Term).count(), 5)

        self.assertTrue(created)
        ontologies = session.query(Ontology).filter_by(name=ontology_name)
        self.assertEqual(len(ontologies.all()), 2)
        self.assertTrue(new_ontology.name == r_ontology.name)
        loader.wipe_ontology(ontology_name=ontology_name)
        ontologies = session.query(Ontology).filter_by(name=ontology_name)
        self.assertEqual(len(ontologies.all()), 0)
        # test cascade
        self.assertEqual(session.query(Term).count(), 0)

    @ignore_warnings
    def testLoadOntologyTerms(self):
        loader = OlsLoader(self.db_url)
        session = dal.get_session()
        ontology_name = 'cvdo'
        m_ontology = loader.load_ontology(ontology_name)
        expected = loader.load_ontology_terms(m_ontology)
        logger.info('Expected terms %s', expected)
        s_terms = session.query(Term).filter(Ontology.name == 'cvdo')
        inserted = s_terms.all()
        logger.info('Inserted terms %s', len(inserted))
        self.assertEqual(expected, len(inserted))

    @ignore_warnings
    def testLoadSubsets(self):
        loader = OlsLoader(self.db_url)
        # print(detail)
        m_term = loader.load_term('http://purl.obolibrary.org/obo/GO_0006412', 'go')
        logger.info('Loaded Term: %s', m_term)
        subsets = loader.load_term_subsets(m_term)

        with loader.session_scope() as session:
            logger.info('Loaded subsets: %s', subsets)
            m_subsets = session.query(Subset).all()
            self.assertEqual(subsets, len(m_subsets))

    @ignore_warnings
    def testLoadTermRelation(self):
        loader = OlsLoader(self.db_url)
        # with loader.session_scope() as get_session:
        iri = 'http://purl.obolibrary.org/obo/GO_0006412'
        onto = 'eco'
        client = OlsClient()
        term = client.detail(ontology_name=onto, iri=iri, item=helpers.Term)

        for relation in term.relations_types:
            logger.info('Loading relation %s', relation)
            # loader.load_term_relations(term.iri, term.ontology_name, relation)

        with loader.session_scope() as session:
            n_ontologies = session.query(Ontology).count()
            self.assertGreater(n_ontologies, 1)

    @ignore_warnings
    def testLoadSynonyms(self):
        loader = OlsLoader(self.db_url)
        iri = 'http://purl.obolibrary.org/obo/GO_0008810'
        onto = 'go'
        m_term = loader._load_term_synonyms(iri, onto)

    @ignore_warnings
    def testLoadAltId(self):
        loader = OlsLoader(self.db_url)
        iri = 'http://purl.obolibrary.org/obo/GO_0000003'
        onto = 'go'
        m_term = loader.load_term(iri, onto)
        with loader.session_scope() as session:
            m_term_1 = session.query(Term).filter_by(accession='GO:0000003').one()
            self.assertGreaterEqual(len(m_term_1.alt_accession), 2)
