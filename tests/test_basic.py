# -*- coding: utf-8 -*-
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
import unittest
import warnings

import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.db import *
from bio.ensembl.ontology.loader import OlsLoader
from bio.ensembl.ontology.models import *
from ebi.ols.api.client import OlsClient

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
    _multiprocess_shared_ = False
    # db_url = 'sqlite://'
    db_url = 'mysql://marc:projet@localhost:3306/ols_ontology?charset=utf8'

    def setUp(self):
        self.loader = OlsLoader(self.db_url)
        self.loader.init_db()
        self.client = OlsClient()

    def tearDown(self):
        dal.wipe_schema(self.db_url)

    @ignore_warnings
    def testLoadOntology(self):
        # test retrieve
        # test try to create duplicated
        ontology_name = 'cvdo'
        m_ontology = self.loader.load_ontology(ontology_name)
        logger.info('Loaded ontology %s', m_ontology)

        with dal.session_scope() as session:

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

            self.assertTrue(created)
            for i in range(0, 5):
                session.add(Term(accession='CCC_00000{}'.format(i),
                                 name='Term {}'.format(i),
                                 ontology=r_ontology,
                                 is_root=False,
                                 is_obsolete=False))
            self.assertTrue(new_ontology.name == r_ontology.name)

        session = dal.get_session()
        self.assertEqual(5, session.query(Term).count())
        ontologies = session.query(Ontology).filter_by(name=ontology_name)
        self.assertEqual(ontologies.count(), 2)
        session = dal.get_session()
        self.loader.wipe_ontology(ontology_name=ontology_name, session=session)
        ontologies = session.query(Ontology).filter_by(name=ontology_name).count()
        self.assertEqual(ontologies, 0)

    @ignore_warnings
    def testLoadOntologyTerms(self):
        session = dal.get_session()
        ontology_name = 'cio'
        expected = self.loader.load_ontology_terms(ontology_name, session)
        logger.info('Expected terms %s', expected)
        s_terms = session.query(Term).filter(Ontology.name == ontology_name)
        inserted = s_terms.count()
        logger.info('Inserted terms %s', inserted)
        self.assertEqual(expected, inserted)

    @ignore_warnings
    def testLoadTimeMeta(self):
        ontology_name = 'bfo'
        self.loader.options['wipe'] = False
        m_ontology = self.loader.load_all('bfo')
        self.assertTrue(m_ontology)
        session = dal.get_session()
        meta_file_date = session.query(Meta).filter_by(meta_key=ontology_name + '_file_date').one()
        meta_time = session.query(Meta).filter_by(meta_key=ontology_name + '_load_time').one()
        meta_start = session.query(Meta).filter_by(meta_key=ontology_name + '_load_date').one()
        self.assertTrue(float(meta_time.meta_value) > 0)
        self.assertTrue(datetime.datetime.strptime(meta_start.meta_value, "BFO/%c") < datetime.datetime.now())
        logger.debug('meta load_all date: %s', meta_start)
        logger.debug('meta file date: %s', meta_file_date)
        try:
            datetime.datetime.strptime(meta_file_date.meta_value, "BFO/%c")
            datetime.datetime.strptime(meta_start.meta_value, "BFO/%c")
        except ValueError:
            self.fail('Wrong date format')

    @ignore_warnings
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
                m_term.synonyms.append(
                    Synonym(name='TS:000%s' % i, type=SynonymTypeEnum.EXACT, db_xref='REF:000%s' % i))
                m_term_2.synonyms.append(
                    Synonym(name='TS2:000%s' % i, type=SynonymTypeEnum.EXACT, db_xref='REF:000%s' % i))
                m_term.alt_ids.append(AltId(accession='ATL:000%s' % i))
                m_term.add_child_relation(rel_type=rel_type, child_term=m_term_3, ontology=m_ontology)
                m_term.add_parent_relation(rel_type=rel_type, parent_term=m_term_2, ontology=m_ontology_2)
                closure_1 = Closure(child_term=m_term, parent_term=m_term_2, distance=1, ontology=m_ontology)
                closure_2 = Closure(parent_term=m_term, child_term=m_term_3, distance=3, ontology=m_ontology_2)
                closure_3 = Closure(parent_term=m_term_2, child_term=m_term_3, subparent_term=m_term, distance=2,
                                    ontology=m_ontology_3)
                session.add_all([closure_1, closure_2, closure_3])

        with dal.session_scope() as session:
            self.loader.wipe_ontology('GO', session)
            self.assertEqual(session.query(Term).count(), 4)
            self.assertEqual(session.query(Synonym).count(), 0)
            self.assertEqual(session.query(AltId).count(), 0)
            self.assertEqual(session.query(Relation).count(), 0)
            self.assertEqual(session.query(Closure).count(), 0)

    @ignore_warnings
    def testMeta(self):
        session = dal.get_session()
        metas = session.query(Meta).all()
        self.assertGreaterEqual(len(metas), 2)

    @ignore_warnings
    def testEncodingTerm(self):
        session = dal.get_session()
        m_ontology = self.loader.load_ontology('fypo', session)
        term = helpers.Term(ontology_name='fypo', iri='http://purl.obolibrary.org/obo/FYPO_0005645')
        o_term = self.client.detail(term)
        m_term = self.loader.load_term(o_term, m_ontology, session)
        self.assertIn('λ', m_term.description)

    @ignore_warnings
    def testSingleTerm(self):
        session = dal.get_session()
        m_ontology = self.loader.load_ontology('fypo', session)
        term = helpers.Term(ontology_name='fypo', iri='http://purl.obolibrary.org/obo/FYPO_0000001')
        o_term = self.client.detail(term)
        m_term = self.loader.load_term(o_term, m_ontology, session)
        session.commit()
        self.assertGreaterEqual(len(m_term.child_terms), 6)

    @ignore_warnings
    def testOntologiesList(self):
        self.assertIsInstance(self.loader.allowed_ontologies, list)
        self.assertIn('go', self.loader.allowed_ontologies)


    @ignore_warnings
    def testRelationsShips(self):
        session = dal.get_session()
        m_ontology = self.loader.load_ontology('go', session)
        term = helpers.Term(ontology_name='go', iri='http://purl.obolibrary.org/obo/GO_0000228')
        o_term = self.client.detail(term)
        m_term = self.loader.load_term(o_term, m_ontology, session)
        session.commit()
        print(m_term.child_terms.all())



