# -*- coding: utf-8 -*-
import unittest
import warnings
from os.path import dirname
from os import remove
# from sqlalchemy_utils.functions import create_database

from bio.ensembl.ontology.models import *
# import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.db import *
from bio.ensembl.ontology.loader import OlsLoader
# from ebi.ols.api.client import OlsClient

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)s.%(funcName)s(%(lineno)d) - %(message)s',
                    datefmt='%m-%d %H:%M')

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
    # db_url = 'sqlite:////' + dirname(__file__) + '/test_ontology.sqlite'
    db_url = 'sqlite:////' + dirname(__file__) + '/test_ontology.sqlite'

    # db_url = 'mysql+pymysql://marc:projet@localhost:3306/ols_ontology'

    @classmethod
    def setUp(cls):
        try:
            remove(dirname(__file__) + '/test_ontology.sqlite')
            logger.info('Remove old DB %s', dirname(__file__) + '/test_ontology.sqlite')
        except OSError as e:
            logger.info('---- Unable to delete old db ----')
            pass
        dal.db_init(cls.db_url, echo=True)

    def testInitDb(self):
        self.assertEqual(1,1)
        # create_database(self.db_url)

    """
    @ignore_warnings
    def testLoadOntology(self):
        loader = OlsLoader(self.db_url)
        # print(m_ontology)

        # test retrieve
        # test try to create duplicated
        with dal.session_scope() as session:
            m_ontology = loader._load_ontology_model('efo')
            logger.info('Loaded ontology %s', m_ontology)

            r_ontology = session.query(Ontology).filter_by(name=m_ontology.name,
                                                                  namespace=m_ontology.namespace).one()
            logger.info('(RE) Loaded ontology %s', r_ontology)
            self.assertEqual(m_ontology.name, r_ontology.name)
            assert isinstance(r_ontology, Ontology)
            # automatically create another one
            new_ontology, created = loader.get_or_create(Ontology,
                                                         name=r_ontology.name,
                                                         namespace='another_namespace')
            self.assertTrue(created)
            self.assertTrue(new_ontology in session)

    @ignore_warnings
    def testLoadOntologyTerms(self):
        loader = OlsLoader(self.db_url, autocommit=True)

        with loader.session_scope() as session:
            expected = loader._load_ontology_terms('bto')
            logger.info('Expected terms %s', expected)

            s_terms = session.query(Term).filter(Ontology.name == 'bto')
            inserted = s_terms.all()
            logger.info('Inserted terms %s', len(inserted))
            if logger.isEnabledFor(logging.DEBUG):
                [logger.debug(m_term) for m_term in inserted]
            self.assertEqual(expected, len(inserted))

    @ignore_warnings
    def testLoadSubsets(self):
        loader = OlsLoader(self.db_url)
        # print(detail)
        m_term = loader._load_term('http://purl.obolibrary.org/obo/GO_0006412', 'go')
        logger.info('Loaded Term: %s', m_term)
        subsets = loader._load_term_subsets(m_term)

        with loader.session_scope() as session:
            logger.info('Loaded subsets: %s', subsets)
            m_subsets = session.query(Subset).all()
            self.assertEqual(subsets, len(m_subsets))

    @ignore_warnings
    def testLoadTermRelation(self):
        loader = OlsLoader(self.db_url)
        # with loader.session_scope() as session:
        iri = 'http://purl.obolibrary.org/obo/GO_0006412'
        onto = 'eco'
        client = OlsClient()
        term = client.detail(ontology_name=onto, iri=iri, item=helpers.Term)

        for relation in term.relations_types:
            logger.info('Loading relation %s', relation)
            loader._load_term_relation(term.iri, term.ontology_name, relation)

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
        m_term = loader._load_term(iri, onto)
        with loader.session_scope() as session:
            m_term_1 = session.query(Term).filter_by(accession='GO:0000003').one()
            self.assertGreaterEqual(len(m_term_1.alt_accession), 2)

    """