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
import configparser
import datetime
import logging.config
import os
import unittest
import warnings
from os.path import dirname, join

import eHive
import sqlalchemy
from eHive.Process import Job

import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.hive.OLSHiveLoader import OLSHiveLoader
from bio.ensembl.ontology.hive.OLSOntologyLoader import OLSOntologyLoader
from bio.ensembl.ontology.hive.OLSTermsLoader import OLSTermsLoader
from bio.ensembl.ontology.loader.db import *
from bio.ensembl.ontology.loader.models import *
from bio.ensembl.ontology.loader.ols import OlsLoader, init_schema, log_format
from ebi.ols.api.client import OlsClient
from ebi.ols.api.exceptions import NotFoundException

# TODO add potential multi processing thread safe logger class
#  https://mattgathu.github.io/multiprocessing-logging-in-python/
# config = yaml.safe_load(open(dirname(__file__) + '/logging.yaml'))
# logging.config.dictConfig(config)

logging.basicConfig(level=logging.DEBUG,
                    format=log_format,
                    datefmt='%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)


def read_env():
    """
    Reads a INI file named .env in the same directory manage.py is invoked and
    loads it as environment variables.
    Note: At least one section must be present. If the environment variable
    TEST_ENV is not set then the [DEFAULT] section will be loaded.
    More info: https://docs.python.org/3/library/configparser.html
    """
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(os.path.join(os.path.dirname(__file__), '.env'))
    section = os.environ.get("TEST_ENV", "DEFAULT")

    for var, value in config[section].items():
        os.environ.setdefault(var, value)


read_env()


class TestOLSLoaderBasic(unittest.TestCase):
    _multiprocess_shared_ = False
    db_url = os.getenv('DB_TEST_URL',
                       'mysql+pymysql://root@localhost:3306/ols_test_ontology?charset=utf8&autocommit=true')
    ols_api_url = os.getenv('OLS_API_URL', 'http://localhost:8080/api')
    test_ontologies = ['AERO', 'DUO', 'BFO', 'EO', 'SO', 'ECO', 'PHI', 'OGMS']

    @classmethod
    def setUpClass(cls):
        logger.info('Using %s connexion string', cls.db_url)
        warnings.simplefilter("ignore", ResourceWarning)
        try:
            dal.wipe_schema(cls.db_url)
        except sqlalchemy.exc.InternalError as e:
            logger.info("Unable to wipe schema %s", e)

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        try:
            dal.wipe_schema(self.db_url)
        except sqlalchemy.exc.InternalError as e:
            logger.info("Unable to wipe schema %s", e)
        self.loader = OlsLoader(self.db_url, echo=False, output_dir='.', verbosity=logging.DEBUG,
                                allowed_ontologies=self.test_ontologies,
                                ols_api_url=self.ols_api_url)
        self.client = OlsClient(base_site=self.ols_api_url)

    def testCascadeDelete(self):
        if 'mysql' not in self.db_url:
            self.skipTest('Only with mysql')
        with dal.session_scope() as session:
            m_ontology = Ontology(name='GO', _namespace='namespace', version='1', title='Ontology test')
            m_ontology_2 = Ontology(name='GO', _namespace='namespace 2', version='1', title='Ontology test 2')
            m_ontology_3 = Ontology(name='FPO', _namespace='namespace 3', version='1', title='Ontology test 2')
            session.add(m_ontology)
            session.add(m_ontology_2)
            session.add(m_ontology_3)
            rel_type, created = get_one_or_create(RelationType,
                                                  session,
                                                  name='is_a')
            for i in range(0, 5):
                m_term = Term(accession='GO:0000%s' % i, name='Term %s' % i, ontology=m_ontology)
                m_term_2 = Term(accession='GO:1000%s' % i, name='Term %s' % i, ontology=m_ontology_2)
                m_term_3 = Term(accession='T3:0000%s' % i, name='Term %s' % i, ontology=m_ontology_3)
                syn_1 = Synonym(name='TS:000%s' % i, type=SynonymTypeEnum.EXACT, db_xref='REF:000%s' % i)
                m_term.synonyms.append(syn_1)
                syn_2 = Synonym(name='TS2:000%s' % i, type=SynonymTypeEnum.EXACT, db_xref='REF:000%s' % i)
                m_term_2.synonyms.append(syn_2)
                session.add_all([syn_1, syn_2])
                alt_id = AltId(accession='ATL:000%s' % i)
                m_term.alt_ids.append(alt_id)
                session.add(alt_id)
                m_term.add_child_relation(session=session, rel_type=rel_type, child_term=m_term_3)
                m_term.add_parent_relation(session=session, rel_type=rel_type, parent_term=m_term_2)
                closure_1 = Closure(child_term=m_term, parent_term=m_term_2, distance=1, ontology=m_ontology)
                closure_2 = Closure(parent_term=m_term, child_term=m_term_3, distance=3, ontology=m_ontology_2)
                closure_3 = Closure(parent_term=m_term_2, child_term=m_term_3, subparent_term=m_term, distance=2,
                                    ontology=m_ontology_3)
                session.add_all([closure_1, closure_2, closure_3])

            self.assertEqual(session.query(Synonym).count(), 10)
            self.assertEqual(session.query(AltId).count(), 5)
            self.assertEqual(session.query(Relation).count(), 10)
            self.assertEqual(session.query(Closure).count(), 12)

        with dal.session_scope() as session:
            self.loader.wipe_ontology('GO')
            for term in session.query(Term).all():
                self.assertTrue(term.accession.startswith('T3'))
            self.assertEqual(0, session.query(Term).filter(Term.ontology_id == 1).count())
            self.assertEqual(session.query(Term).count(), 5)
            self.assertEqual(session.query(Synonym).count(), 0)
            self.assertEqual(session.query(AltId).count(), 0)
            self.assertEqual(session.query(Relation).count(), 0)
            self.assertEqual(session.query(Closure).count(), 0)

    def testLoadOntologyTerms(self):
        session = dal.get_session()
        ontology_name = 'PHI'
        self.loader.load_ontology(ontology_name, session)
        expected, ignored = self.loader.load_ontology_terms(ontology_name)
        logger.info('Expected terms %s', expected)
        inserted = session.query(Term).count()
        logger.info('Inserted terms %s', inserted)
        self.assertEqual(expected, inserted)
        logger.info('Testing unknown ontology')
        with self.assertRaises(NotFoundException):
            expected, ignored = self.loader.load_ontology_terms('unknownontology')
            self.assertEqual(0, expected)

    def testLoadOntology(self):
        # test retrieve
        # test try to create duplicated
        ontology_name = 'ogms'

        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology(ontology_name, session)
            logger.info('Loaded ontology %s', m_ontology)
            logger.info('number of Terms %s', m_ontology.number_of_terms)
            r_ontology = session.query(Ontology).filter_by(name=ontology_name,
                                                           namespace='OGMS').one()
            ontology_id = r_ontology.id
            logger.info('(RE) Loaded ontology %s', r_ontology)
            self.assertEqual(m_ontology.name, r_ontology.name)
            self.assertEqual(m_ontology.version, r_ontology.version)
            assert isinstance(r_ontology, Ontology)
            # automatically create another one with another namespace
            new_ontology, created = get_one_or_create(Ontology,
                                                      session,
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
        self.assertEqual(5, session.query(Term).filter_by(ontology_id=ontology_id).count())
        ontologies = session.query(Ontology).filter_by(name=ontology_name)
        self.assertEqual(ontologies.count(), 2)
        self.loader.final_report(ontology_name)
        self.assertTrue(os.path.isfile(ontology_name + '.ontology.log'))

    def testUpperCase(self):
        ontology_name = 'OGMS'
        self.loader.options['process_relations'] = False
        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology(ontology_name, session)
            session.add(m_ontology)
            self.assertEqual(m_ontology.name, 'OGMS')
            onto_id = m_ontology.id
            logger.info("Ontololgy name in DB %s", m_ontology.name)
            self.loader.load_ontology_terms('aero', 0, 50)
            terms = session.query(Term).all()
            for term in terms:
                if term.ontology.name == 'OGMS':
                    self.assertTrue(term.ontology_id == onto_id)

    def testRelatedNonExpected(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True
        with dal.session_scope() as session:
            ontology_name = 'ECO'
            expected, _ignored = self.loader.load_ontology_terms(ontology_name, start=0, end=50)
            logger.info('Expected terms %s', expected)
            s_terms = session.query(Term).filter(Ontology.name == ontology_name)
            inserted = s_terms.count()
            logger.info('Inserted terms %s', inserted)
            self.assertGreaterEqual(inserted, expected)

    def testLoadTimeMeta(self):
        ontology_name = 'BFO'
        self.loader.options['wipe'] = True
        self.loader.options['ens_version'] = 99

        init_schema(self.db_url, **self.loader.options)
        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology(ontology_name, session)
            session.add(m_ontology)
            self.assertIsInstance(m_ontology, Ontology)
        session = dal.get_session()
        meta_file_date = session.query(Meta).filter_by(meta_key=ontology_name + '_file_date').one()
        meta_start = session.query(Meta).filter_by(meta_key=ontology_name + '_load_date').one()
        meta_schema = session.query(Meta).filter_by(meta_key='patch').one()
        self.assertEqual('patch_98_99_a.sql|schema version', meta_schema.meta_value)
        self.assertTrue(
            datetime.datetime.strptime(meta_start.meta_value, ontology_name.upper() + "/%c") < datetime.datetime.now())
        logger.debug('meta load_all date: %s', meta_start)
        logger.debug('meta file date: %s', meta_file_date)
        try:
            datetime.datetime.strptime(meta_file_date.meta_value, ontology_name.upper() + "/%c")
            datetime.datetime.strptime(meta_start.meta_value, ontology_name.upper() + "/%c")
        except ValueError:
            self.fail('Wrong date format')

    def testLogger(self):
        self.loader = OlsLoader(self.db_url, echo=False, output_dir='.', verbosity='DEBUG')

        with dal.session_scope() as session:
            self.loader.load_ontology('bfo', session)
            self.assertTrue(os.path.isfile(os.path.join(dirname(__file__), 'bfo.ontology.log')))
            self.loader.load_ontology_terms('bfo', 0, 15)
            self.assertTrue(os.path.isfile(os.path.join(dirname(__file__), 'bfo.terms.0.15.log')))

    def testHiveLoader(self):
        class RunnableWithParams(OLSHiveLoader):
            def __init__(self, d):
                self._BaseRunnable__params = eHive.Params.ParamContainer(d)
                self.input_job = Job()
                self.input_job.transient_error = True
                self.debug = 1

        hive_loader = RunnableWithParams({
            'ontology_name': 'duo',
            'ens_version': 100,
            'db_url': self.db_url,
            'output_dir': dirname(__file__)
        })
        hive_loader.run()
        with dal.session_scope() as session:
            metas = session.query(Meta).all()
            self.assertGreaterEqual(len(metas), 2)
            schema_type = session.query(Meta).filter_by(meta_key='schema_type').one()
            self.assertEqual(schema_type.meta_value, 'ontology')
            schema_type = session.query(Meta).filter_by(meta_key='schema_version').one()
            self.assertEqual(schema_type.meta_value, '100')
            schema_patch = session.query(Meta).filter_by(meta_key='patch').one()
            self.assertEqual(schema_patch.meta_value, 'patch_99_100_a.sql|schema version')

    def testOntologyLoader(self):
        class OntologyLoader(OLSOntologyLoader):
            def __init__(self, d):
                self._BaseRunnable__params = eHive.Params.ParamContainer(d)
                self._BaseRunnable__read_pipe = open(join(dirname(__file__), 'hive.in'), mode='rb', buffering=0)
                self._BaseRunnable__write_pipe = open(join(dirname(__file__), 'hive.out'), mode='wb', buffering=0)
                self.input_job = Job()
                self.input_job.transient_error = True
                self.debug = 1

        hive_loader = OntologyLoader({
            'ontology_name': 'aero',
            'ens_version': 100,
            'db_url': self.db_url,
            'output_dir': dirname(__file__),
            'verbosity': '4',
            'wipe_one': 0,
            'allowed_ontologies': self.test_ontologies,
            'ols_api_url': self.ols_api_url
        })

        hive_loader.run()
        with dal.session_scope() as session:
            self.assertIsNotNone(session.query(Meta).filter_by(meta_key='AERO_load_date').one())
            self.assertIsNotNone(session.query(Meta).filter_by(meta_key='AERO_file_date').one())

    def testTermHiveLoader(self):
        class TermLoader(OLSTermsLoader):
            def __init__(self, d):
                self._BaseRunnable__params = eHive.Params.ParamContainer(d)
                self.input_job = Job()
                self.input_job.transient_error = True
                self.debug = 1

        params_set = {
            'ontology_name': 'bfo',
            'db_url': self.db_url,
            'output_dir': dirname(__file__),
            'verbosity': '4',
            '_start_term_index': 0,
            '_end_term_index': 19,
            'ols_api_url': self.ols_api_url,
            'allowed_ontologies': self.test_ontologies,
            'page_size': 20
        }

        term_loader = TermLoader(params_set)
        term_loader.run()
        with dal.session_scope() as session:
            self.assertIsNotNone(session.query(Ontology).filter_by(name='BFO').one())
            self.assertGreaterEqual(session.query(Term).count(), 17)
            self.assertGreaterEqual(session.query(Relation).count(), 17)
            self.assertGreaterEqual(session.query(RelationType).count(), 1)

        params_set['_start_term_index'] = 20
        params_set['_end_term_index'] = 100
        term_loader = TermLoader(params_set)
        term_loader.run()
        with dal.session_scope() as session:
            self.assertIsNotNone(session.query(Ontology).filter_by(name='BFO').one())
            self.assertGreaterEqual(session.query(Term).count(), 18)
            self.assertGreaterEqual(session.query(Relation).count(), 18)
            self.assertEqual(session.query(RelationType).count(), 1)
        self.assertTrue(os.path.isfile(os.path.join(dirname(__file__), 'bfo.ontology.log')))
        self.assertTrue(os.path.isfile(os.path.join(dirname(__file__), 'bfo.terms.0.15.log')))

    def testRelationSingleTerm(self):
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://purl.obolibrary.org/obo/ECO_0007571')
            m_term = self.loader.load_term(o_term, 'ECO', session)
            session.add(m_term)
            session.commit()

    def testAltIds(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False

        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://purl.obolibrary.org/obo/SO_0000569')
            # was http://purl.obolibrary.org/obo/GO_0005261
            m_term = self.loader.load_term(o_term, 'SO', session)
            session.add(m_term)
            session.commit()
            term = session.query(Term).filter_by(accession='SO:0000569').one()
            logger.debug("Retrieved alt Ids: %s", term.alt_ids)
            self.assertGreaterEqual(len(term.alt_ids), 1)

    def testSubsetEco(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True
        with dal.session_scope() as session:
            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/ECO_0000305",
                                        ontology_name='ECO', type=helpers.Term)
            m_term = self.loader.load_term(o_term, 'ECO', session)
            session.commit()
            subsets = session.query(Subset).all()
            subsets_name = [sub.name for sub in subsets]
            term_subsets = m_term.subsets.split(',')
            self.assertEqual(set(subsets_name), set(term_subsets))
            for definition in subsets:
                self.assertIsNotNone(definition)
