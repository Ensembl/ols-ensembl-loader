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
from os.path import dirname

import eHive
import sqlalchemy
from eHive.Process import Job

import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.loader.db import *
from bio.ensembl.ontology.loader.models import *
from bio.ensembl.ontology.loader.ols import OlsLoader, init_schema, log_format
from ebi.ols.api.client import OlsClient
from ebi.ols.api.exceptions import NotFoundException
from ensembl.ontology.hive.OLSHiveLoader import OLSHiveLoader
from ensembl.ontology.hive.OLSOntologyLoader import OLSOntologyLoader
# TODO add potential multi processing thread safe logger class
#  https://mattgathu.github.io/multiprocessing-logging-in-python/
# config = yaml.safe_load(open(dirname(__file__) + '/logging.yaml'))
# logging.config.dictConfig(config)
from ensembl.ontology.hive.OLSTermsLoader import OLSTermsLoader

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


class TestOLSLoaderRemote(unittest.TestCase):
    _multiprocess_shared_ = False
    db_url = os.getenv('DB_TEST_URL',
                       'mysql+pymysql://root@localhost:3306/ols_test_ontology?charset=utf8&autocommit=true')
    ols_api_url = 'https://www.ebi.ac.uk/ols/api'

    @classmethod
    def setUpClass(cls):
        logger.info('Using %s connexion string', cls.db_url)
        try:
            dal.wipe_schema(cls.db_url)
        except sqlalchemy.exc.InternalError as e:
            logger.info("Unable to wipe schema %s", e)

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        self.loader = OlsLoader(self.db_url, echo=False, output_dir='.')
        self.loader.allowed_ontologies = ['GO', 'SO', 'PATO', 'HP', 'VT', 'EFO', 'PO', 'EO', 'TO', 'CHEBI', 'PR',
                                          'FYPO', 'PECO', 'BFO', 'BTO', 'CL', 'CMO', 'ECO', 'MOD', 'MP', 'OGMS', 'UO',
                                          'MONDO', 'PHI', 'DUO']
        self.client = OlsClient(base_site=self.ols_api_url)

    def tearDown(self):
        dal.wipe_schema(self.db_url)

    def testGoExpectedLinks(self):
        go_term = [
            'GO_0005575',
            'GO_0003674',
            'GO_0008150',
        ]
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = True
        with dal.session_scope() as session:
            terms = self.loader.load_ontology_terms('GO', 0, 20)
            ontologies = session.query(Ontology).filter_by(name='GO')
            namespaces = [onto.namespace for onto in ontologies]
            self.assertSetEqual(set(['go', 'biological_process', 'cellular_component', 'molecular_function']),
                                set(namespaces))
            GO_0005575 = session.query(Term).filter_by(accession='GO:0005575').one()
            GO_0003674 = session.query(Term).filter_by(accession='GO:0003674').one()
            GO_0008150 = session.query(Term).filter_by(accession='GO:0008150').one()
            self.assertEqual('biological_process', GO_0008150.ontology.namespace)
            self.assertEqual('cellular_component', GO_0005575.ontology.namespace)
            self.assertEqual('molecular_function', GO_0003674.ontology.namespace)

    def testPartOfRelationship(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = False
        with dal.session_scope() as session:
            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/GO_0032042",
                                        ontology_name='GO', type=helpers.Term)
            m_term = self.loader.load_term(o_term, 'GO', session)
            self.assertIn('part_of', o_term.relations_types)
            self.assertIn('part_of', [relation.relation_type.name for relation in m_term.parent_terms])
            self.assertIn('occurs_in', [relation.relation_type.name for relation in m_term.parent_terms])

    def testChebi(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False
        self.loader.load_ontology_terms('CHEBI', start=1200, end=1250)
        session = dal.get_session()
        subsets = session.query(Subset).all()
        for subset in subsets:
            self.assertNotEqual(subset.definition, subset.name)

    def testTermInvalidDefinition(self):
        '''
        Term has invalid characters in the definition (e.g. "\\n")
        '''
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False
        with dal.session_scope() as session:
            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/GO_0090481",
                                        ontology_name='GO', type=helpers.Term)
            if '\n' not in o_term.description:
                self.skipTest("Term Description does not contain invalid characters.")
            else:
                m_term = self.loader.load_term(o_term, 'GO', session)
                self.assertNotIn('\n', m_term.description)

    def testTermNoDefinition(self):
        '''
        Term does not declared a definition neither within annotation, label is therefore inserted
        '''
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False
        with dal.session_scope() as session:
            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/MONDO_0020003",
                                        ontology_name='MONDO', type=helpers.Term)
            m_term = self.loader.load_term(o_term, 'MONDO', session)
            self.assertEqual(m_term.name, m_term.description.lower())

    def testLongTermDefinition(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True
        with dal.session_scope() as session:
            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/UBERON_0000948",
                                        ontology_name='UBERON', type=helpers.Term)
            m_term = self.loader.load_term(o_term, 'UBERON', session)
            for syn in m_term.synonyms:
                self.assertNotEqual(syn.name, '')

            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/MONDO_0004933",
                                        ontology_name='MONDO', type=helpers.Term)
            m_term = self.loader.load_term(o_term, 'MONDO', session)
            for syn in m_term.synonyms:
                self.assertNotEqual(syn.name, '')

    def testGoTerm(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True
        with dal.session_scope() as session:
            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/GO_0030118",
                                        ontology_name='GO', type=helpers.Term)
            m_term = self.loader.load_term(o_term, 'GO', session)
            session.add(m_term)
            self.assertIn('GO:0030117', [rel.parent_term.accession for rel in m_term.parent_terms])
            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/GO_0030131",
                                        ontology_name='GO', type=helpers.Term)
            m_term = self.loader.load_term(o_term, 'GO', session)
            session.add(m_term)
            self.assertIn('GO:0030119', [rel.parent_term.accession for rel in m_term.parent_terms if
                                         rel.relation_type.name == 'is_a'])
            self.assertIn('GO:0030118', [rel.parent_term.accession for rel in m_term.parent_terms if
                                         rel.relation_type.name == 'part_of'])

    def testExternalRelationship(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://www.ebi.ac.uk/efo/EFO_0002911', unique=True, silent=True)
            m_term = self.loader.load_term(o_term, 'EFO', session)
            session.add(m_term)
            found = False
            for relation in m_term.parent_terms:
                found = found or (relation.parent_term.accession == 'OBI:0000245')
        self.assertTrue(found)
        session = dal.get_session()
        ontologies = session.query(Ontology).filter_by(name='OBI').count()
        # assert that OBI has not been inserted
        self.assertEqual(0, ontologies)

    def testMissingOboId(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://purl.obolibrary.org/obo/PR_P68993', unique=True, silent=True)
            m_term = self.loader.load_term(o_term, 'PR', session)
            self.assertEqual(m_term.accession, 'PR:P68993')

    def testSubsetErrors(self):
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://www.ebi.ac.uk/efo/EFO_0003503')
            m_term = self.loader.load_term(o_term, 'EFO', session)
            session.add(m_term)
            self.assertIsInstance(session.query(Subset).filter_by(name='efo_slim').one(), Subset)

    def testTrickTerm(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = False

        with dal.session_scope() as session:
            # o_term = helpers.Term(ontology_name='fypo', iri='http://purl.obolibrary.org/obo/FYPO_0001330')
            o_term = self.client.term(identifier='http://purl.obolibrary.org/obo/FYPO_0001330', unique=True,
                                      silent=True)
            m_term = self.loader.load_term(o_term, 'fypo', session)
            session.add(m_term)
            found = False
            for relation in m_term.parent_terms:
                found = found or (relation.parent_term.accession == 'CHEBI:24431')
        self.assertTrue(found)

    def testSubsets(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False

        with dal.session_scope() as session:
            term = helpers.Term(ontology_name='go', iri='http://purl.obolibrary.org/obo/GO_0099565')
            o_term = self.client.detail(term)
            m_term = self.loader.load_term(o_term, 'go', session)
            session.add(m_term)
            subsets = session.query(Subset).all()
            for subset in subsets:
                self.assertIsNotNone(subset.definition)

            subset = helpers.Property(ontology_name='go',
                                      iri='http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym')
            details = self.client.detail(subset)
            self.assertNotEqual(details.definition, '')

    def testRelationOtherOntology(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True
        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology('efo', session)
            session.add(m_ontology)
            term = helpers.Term(ontology_name='efo', iri='http://www.ebi.ac.uk/efo/EFO_0002215')
            o_term = self.client.detail(term)
            m_term = self.loader.load_term(o_term, m_ontology, session)
            session.add(m_term)
            self.assertGreaterEqual(session.query(Ontology).count(), 2)
            term = session.query(Term).filter_by(accession='BTO:0000164')
            self.assertEqual(1, term.count())

    def testRelationsShips(self):
        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology('bto', session)
            session.add(m_ontology)
            term = helpers.Term(ontology_name='bto', iri='http://purl.obolibrary.org/obo/BTO_0000005')
            o_term = self.client.detail(term)
            m_term = self.loader.load_term(o_term, m_ontology, session)
            session.add(m_term)
            self.assertGreaterEqual(len(m_term.parent_terms), 0)

    def testSingleTerm(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True

        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology('fypo', session)
            session.add(m_ontology)
            term = helpers.Term(ontology_name='fypo', iri='http://purl.obolibrary.org/obo/FYPO_0000257')
            o_term = self.client.detail(term)
            m_term = self.loader.load_term(o_term, m_ontology, session)
            session.commit()
            self.assertGreaterEqual(len(m_term.parent_terms), 4)

            self.loader.options['process_relations'] = False
            self.loader.options['process_parents'] = False
            o_ontology = self.client.ontology('GO')
            term = helpers.Term(ontology_name='GO', iri='http://purl.obolibrary.org/obo/GO_0000002')
            o_term = self.client.detail(term)
            m_term = self.loader.load_term(o_term, o_ontology, session)
            self.assertEqual(m_term.ontology.name, 'GO')
            with self.assertRaises(RuntimeError):
                self.loader.load_term(o_term, 33, session)

    def testEncodingTerm(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False
        session = dal.get_session()
        m_ontology = self.loader.load_ontology('fypo', session)
        session.add(m_ontology)
        term = helpers.Term(ontology_name='fypo', iri='http://purl.obolibrary.org/obo/FYPO_0005645')
        o_term = self.client.detail(term)
        m_term = self.loader.load_term(o_term, m_ontology, session)
        dal.get_session().commit()
        self.assertTrue(isinstance(m_term.description, str))
        if 'λ' in o_term.description:
            self.assertIn('λ', m_term.description)
        else:
            self.skipTest("Character not present in retrieved term from OLS")


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
                self._BaseRunnable__read_pipe = open('hive.in', mode='rb', buffering=0)
                self._BaseRunnable__write_pipe = open('hive.out', mode='wb', buffering=0)
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
