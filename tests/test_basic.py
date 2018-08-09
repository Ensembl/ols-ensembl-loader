# -*- coding: utf-8 -*-
import logging
import unittest
import warnings
from os.path import dirname

import bio.ensembl.ontology.models as models
import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.loader import OlsLoader
from ebi.ols.api.client import OlsClient

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)s %(levelname)s %(message)s',
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
    db_url = 'sqlite:////' + dirname(__file__) + '/test_ontology.db'

    @classmethod
    def setUp(cls):
        from os import remove
        try:
            remove(dirname(__file__) + '/test_ontology.db')
            logger.info('Remove old DB %s', dirname(__file__) + '/test_ontology.db')
        except OSError as e:
            logger.info('---- Unable to delete old db ----')
            pass
        loader = OlsLoader(cls.db_url)
        loader.create_schema()

    @ignore_warnings
    def testLoadOntology(self):
        loader = OlsLoader(self.db_url)
        # print(m_ontology)
        m_ontology = loader.load_ontology('efo')
        logger.info('Loaded ontology %s', m_ontology)

        with loader.session_scope() as session:
            # test retrieve
            # test try to create duplicated

            r_ontology = session.query(models.Ontology).filter_by(name=m_ontology.name,
                                                                  namespace=m_ontology.namespace).one()
            logger.info('(RE) Loaded ontology %s', r_ontology)
            self.assertEqual(m_ontology.name, r_ontology.name)
            assert isinstance(r_ontology, models.Ontology)
            # automatically create another one
            new_ontology, created = loader.get_or_create(models.Ontology,
                                                         name=r_ontology.name,
                                                         namespace='another_namespace')
            self.assertTrue(created)
            self.assertTrue(new_ontology in session)

    @ignore_warnings
    def testLoadOntologyTerms(self):
        loader = OlsLoader(self.db_url)

        expected = loader.load_ontology_terms('geno')
        logger.info('Expected terms %s', expected)

        with loader.session_scope() as session:
            s_terms = session.query(models.Term).filter(models.Ontology.name == 'geno')
            inserted = s_terms.all()
            logger.info('Inserted terms %s', len(inserted))
            if logger.isEnabledFor(logging.DEBUG):
                [logger.debug(m_term) for m_term in inserted]
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
            m_subsets = session.query(models.Subset).all()
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
            loader.load_term_relation(term.iri, term.ontology_name, relation)

        with loader.session_scope() as session:
            n_ontologies = session.query(models.Ontology).count()
            self.assertGreater(n_ontologies, 1)

    @ignore_warnings
    def testLoadSynonyms(self):
        loader = OlsLoader(self.db_url)
        iri = 'http://purl.obolibrary.org/obo/GO_0008810'
        onto = 'go'
        m_term = loader.load_term_synonyms(iri, onto)

    @ignore_warnings
    def testLoadAltId(self):
        loader = OlsLoader(self.db_url)
        iri = 'http://purl.obolibrary.org/obo/GO_0000003'
        onto = 'go'
        m_term = loader.load_term(iri, onto)
        with loader.session_scope() as session:
            m_term_1 = session.query(models.Term).filter_by(accession='GO:0000003').one()
            self.assertGreaterEqual(len(m_term_1.alt_accession), 2)
