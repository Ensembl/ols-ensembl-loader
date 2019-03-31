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
import logging
import unittest
import warnings
from os import getenv

import ebi.ols.api.helpers as helpers
from ebi.ols.api.client import OlsClient

from bio.ensembl.ontology.loader.db import *
from bio.ensembl.ontology.loader.models import *
from bio.ensembl.ontology.loader.ols import OlsLoader

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s : %(name)s.%(funcName)s(%(lineno)d) - %(message)s',
                    datefmt='%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

logging.getLogger('urllib3.connectionpool').setLevel(logging.FATAL)
logging.getLogger('ebi.ols.api').setLevel(logging.WARNING)


class TestOLSLoader(unittest.TestCase):
    _multiprocess_shared_ = False
    db_url = getenv('DB_TEST_URL', 'sqlite://')

    @classmethod
    def setUpClass(cls):
        dal.wipe_schema(cls.db_url)
        logger.info('Using %s connexion string', cls.db_url)

    def setUp(self):
        warnings.simplefilter("ignore", ResourceWarning)
        self.loader = OlsLoader(self.db_url, echo=False, output_dir='/tmp')
        self.client = OlsClient()

    def tearDown(self):
        dal.wipe_schema(self.db_url)

    def testLoadOntology(self):
        # test retrieve
        # test try to create duplicated
        ontology_name = 'CVDO'

        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology(ontology_name, session)
            logger.info('Loaded ontology %s', m_ontology)
            logger.info('number of Terms %s', m_ontology.number_of_terms)
            r_ontology = session.query(Ontology).filter_by(name=ontology_name,
                                                           namespace='cvdo').one()
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

    def testLoadOntologyTerms(self):
        session = dal.get_session()
        ontology_name = 'CIO'
        self.loader.load_ontology(ontology_name, session)
        expected, ignored = self.loader.load_ontology_terms(ontology_name)
        logger.info('Expected terms %s', expected)
        s_terms = session.query(Term).filter(Ontology.name == ontology_name)
        inserted = s_terms.count()
        logger.info('Inserted terms %s', inserted)
        self.assertEqual(expected, inserted)

    def testLoadTimeMeta(self):
        ontology_name = 'BFO'
        self.loader.options['wipe'] = True
        self.loader.options['db_version'] = '99'
        self.loader.init_meta()
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
            [self.assertTrue(term.accession.startswith('T3')) for term in session.query(Term).all()]
            self.assertEqual(0, session.query(Term).filter(Term.ontology_id == 1).count())
            self.assertEqual(session.query(Term).count(), 5)
            self.assertEqual(session.query(Synonym).count(), 0)
            self.assertEqual(session.query(AltId).count(), 0)
            self.assertEqual(session.query(Relation).count(), 0)
            self.assertEqual(session.query(Closure).count(), 0)

    def testMeta(self):
        session = dal.get_session()
        self.loader.init_meta()
        metas = session.query(Meta).all()
        self.assertGreaterEqual(len(metas), 2)

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
        self.assertIn('λ', m_term.description)

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

    def testOntologiesList(self):
        self.assertIsInstance(self.loader.allowed_ontologies, list)
        self.assertIn('GO', self.loader.allowed_ontologies)

    def testRelationsShips(self):
        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology('bto', session)
            session.add(m_ontology)
            term = helpers.Term(ontology_name='bto', iri='http://purl.obolibrary.org/obo/BTO_0000005')
            o_term = self.client.detail(term)
            m_term = self.loader.load_term(o_term, m_ontology, session)
            session.add(m_term)
            self.assertGreaterEqual(len(m_term.parent_terms), 0)

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

    def testAltIds(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False

        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://purl.obolibrary.org/obo/GO_0005261')
            m_term = self.loader.load_term(o_term, 'go', session)
            session.add(m_term)
            self.assertGreaterEqual(len(m_term.alt_ids), 2)

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

    def testRelatedNonExpected(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True
        with dal.session_scope() as session:
            ontology_name = 'ECO'
            expected, ignored = self.loader.load_ontology_terms(ontology_name, start=0, end=50)
            logger.info('Expected terms %s', expected)
            s_terms = session.query(Term).filter(Ontology.name == ontology_name)
            inserted = s_terms.count()
            logger.info('Inserted terms %s', inserted)
            self.assertGreaterEqual(inserted, expected)

    def testRelationSingleTerm(self):
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://purl.obolibrary.org/obo/ECO_0007571')
            m_term = self.loader.load_term(o_term, 'ECO', session)
            session.add(m_term)
            session.commit()

    def testSubsetErrors(self):
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://www.ebi.ac.uk/efo/EFO_0003503')
            m_term = self.loader.load_term(o_term, 'EFO', session)
            session.add(m_term)
            self.assertIsInstance(session.query(Subset).filter_by(name='efo_slim').one(), Subset)

    def testMissingOboId(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://purl.obolibrary.org/obo/PR_P68993', unique=True, silent=True)
            m_term = self.loader.load_term(o_term, 'PR', session)
            self.assertEqual(m_term.accession, 'PR:P68993')

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
            [self.assertIsNotNone(definition) for definition in subsets]

    def testChebi(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False
        self.loader.load_ontology_terms('CHEBI', start=1200, end=1250)
        session = dal.get_session()
        subsets = session.query(Subset).all()
        [self.assertNotEqual(subset.definition, subset.name) for subset in subsets]

    def testLoadRelatedSynonyms(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False

    def testUpperCase(self):
        ontology_name = 'ogms'
        self.loader.options['process_relations'] = False
        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology(ontology_name, session)
            session.add(m_ontology)
            self.assertEqual(m_ontology.name, 'OGMS')
            onto_id = m_ontology.id
            logger.info("Ontololgy name in DB %s", m_ontology.name)
            self.loader.load_ontology_terms('ogms', 0, 50)
            terms = session.query(Term).all()
            [self.assertTrue(term.ontology_id == onto_id) for term in terms if term.ontology.name == 'OGMS']

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
        with dal.session_scope() as session:
            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/GO_0032042",
                                        ontology_name='GO', type=helpers.Term)
            m_term = self.loader.load_term(o_term, 'GO', session)
            self.assertIn('part_of', o_term.relations_types)
            self.assertIn('part_of', [relation.relation_type.name for relation in m_term.parent_terms])
            self.assertIn('occurs_in', [relation.relation_type.name for relation in m_term.parent_terms])
