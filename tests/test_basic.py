# -*- coding: utf-8 -*-
import unittest
import warnings

from bio.ensembl.ontology.db import *
from bio.ensembl.ontology.loader import OlsLoader
from bio.ensembl.ontology.models import *

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

    def setUp(self):
        super().setUp()
        self.loader = OlsLoader(self.db_url)

    @ignore_warnings
    def testInitDb(self):
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
        session = dal.get_session()
        ontology_name = 'cvdo'
        m_ontology = self.loader.load_ontology(ontology_name)
        logger.info('Loaded ontology %s', m_ontology)
        r_ontology = session.query(Ontology).filter_by(name=ontology_name,
                                                       namespace=m_ontology.namespace).one()
        logger.info('(RE) Loaded ontology %s', r_ontology)
        self.assertEqual(m_ontology.name, r_ontology.name)
        self.assertEqual(m_ontology.version, r_ontology.version)
        assert isinstance(r_ontology, Ontology)
        # automatically create another one with another namespace
        new_ontology, created = get_one_or_create(Ontology,
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
        self.loader.wipe_ontology(ontology_name=ontology_name)
        ontologies = session.query(Ontology).filter_by(name=ontology_name)
        self.assertEqual(len(ontologies.all()), 0)
        # test cascade
        self.assertEqual(session.query(Term).count(), 0)

    @ignore_warnings
    def testLoadOntologyTerms(self):
        session = dal.get_session()
        ontology_name = 'bfo'
        m_ontology = self.loader.load_ontology(ontology_name)
        expected = self.loader.load_ontology_terms(m_ontology)
        logger.info('Expected terms %s', expected)
        s_terms = session.query(Term).filter(Ontology.name == 'bfo')
        inserted = s_terms.all()
        logger.info('Inserted terms %s', len(inserted))
        self.assertEqual(expected, len(inserted))

    def testCascadeDelete(self):
        with dal.session_scope() as session:
            m_ontology = Ontology(name='GO', _namespace='namespace', _version='1', title='Ontology test')
            m_ontology_2 = Ontology(name='GO', _namespace='namespace 2', _version='1', title='Ontology test 2')
            m_ontology_3 = Ontology(name='FPO', _namespace='namespace 3', _version='1', title='Ontology test 2')
            session.add(m_ontology)
            session.add(m_ontology_2)
            session.add(m_ontology_3)
            rel_type = RelationType(name='is_a')
            for i in range(1, 5):
                m_term = Term(accession='T:0000%s' % i, name='Term %s' % i, ontology=m_ontology)
                m_term_2 = Term(accession='T2:0000%s' % i, name='Term %s' % i, ontology=m_ontology_2)
                m_term_3 = Term(accession='T3:0000%s' % i, name='Term %s' % i, ontology=m_ontology_3)
                m_term.synonyms.append(Synonym(name='TS:000%s' % i, type=SynonymTypeEnum.EXACT, db_xref='REF:000%s' % i))
                m_term_2.synonyms.append(Synonym(name='TS2:000%s' % i, type=SynonymTypeEnum.EXACT, db_xref='REF:000%s' % i))
                m_term.alt_ids.append(AltId(accession='ATL:000%s' % i))
                m_term.add_child_relation(rel_type=rel_type, child_term=m_term_3, ontology=m_ontology)
                m_term.add_parent_relation(rel_type=rel_type, parent_term=m_term_2, ontology=m_ontology_2)
                closure_1 = Closure(child_term=m_term, parent_term=m_term_2, distance=1, ontology=m_ontology)
                closure_2 = Closure(parent_term=m_term, child_term=m_term_3, distance=3, ontology=m_ontology_2)
                closure_3 = Closure(parent_term=m_term_2, child_term=m_term_3, subparent_term=m_term, distance=2,
                                    ontology=m_ontology_3)
                session.add_all([closure_1, closure_2, closure_3])

        with dal.session_scope() as session:
            self.loader.wipe_ontology('GO')
            session.flush()
            self.assertEqual(session.query(Term).count(), 4)
            self.assertEqual(session.query(Synonym).count(), 0)
            self.assertEqual(session.query(AltId).count(), 0)
            self.assertEqual(session.query(Relation).count(), 0)
            self.assertEqual(session.query(Closure).count(), 0)






