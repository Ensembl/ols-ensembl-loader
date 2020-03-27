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
import os
import logging
import unittest
import warnings

import sqlalchemy

from bio.ensembl.ontology.loader.db import dal
from bio.ensembl.ontology.loader.models import Ontology, Term, Subset
from bio.ensembl.ontology.loader.ols import OlsLoader, log_format
from ebi.ols.api import helpers as helpers
from ebi.ols.api.client import OlsClient
from tests import read_env

read_env()

logging.basicConfig(level=logging.INFO,
                    format=log_format,
                    datefmt='%m-%d %H:%M:%S')

logger = logging.getLogger(__file__)
log_dir = os.path.join(os.path.dirname(__file__), 'logs')

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
        try:
            dal.wipe_schema(self.db_url)
        except sqlalchemy.exc.InternalError as e:
            logger.info("Unable to wipe schema %s", e)
        self.loader = OlsLoader(self.db_url, echo=False, output_dir=log_dir)
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