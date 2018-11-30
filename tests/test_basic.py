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
from os.path import isfile

import ebi.ols.api.helpers as helpers
from bio.ensembl.ontology.loader import OlsLoader
from bio.ensembl.ontology.loader.db import *
from bio.ensembl.ontology.loader.models import *
from ebi.ols.api.client import OlsClient

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s : %(name)s.%(funcName)s(%(lineno)d) - %(message)s',
                    datefmt='%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

logging.getLogger('urllib3.connectionpool').setLevel(logging.FATAL)


class TestOLSLoader(unittest.TestCase):
    _multiprocess_shared_ = False
    db_url = getenv('DB_TEST_URL', 'sqlite://')

    def setUp(self):
        dal.wipe_schema(self.db_url)
        warnings.simplefilter("ignore", ResourceWarning)
        self.loader = OlsLoader(self.db_url, echo=False)
        self.client = OlsClient()

    def testLoadOntology(self):
        # test retrieve
        # test try to create duplicated
        ontology_name = 'cvdo'

        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology(ontology_name)
            session.add(m_ontology)
            logger.info('Loaded ontology %s', m_ontology)
            logger.info('number of Terms %s', m_ontology.number_of_terms)
            r_ontology = session.query(Ontology).filter_by(name=ontology_name,
                                                           namespace='cvdo').one()
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
        self.assertEqual(5, session.query(Term).count())
        ontologies = session.query(Ontology).filter_by(name=ontology_name)
        self.assertEqual(ontologies.count(), 2)
        if self.db_url.startswith('mysql'):
            session = dal.get_session()
            self.loader.wipe_ontology(ontology_name=ontology_name)
            ontologies = session.query(Ontology).filter_by(name=ontology_name).count()
            self.assertEqual(ontologies, 0)

    def testLoadOntologyTerms(self):
        session = dal.get_session()
        ontology_name = 'cio'
        onto = self.loader.load_ontology(ontology_name)
        expected = self.loader.load_ontology_terms(ontology_name)
        logger.info('Expected terms %s', expected)
        s_terms = session.query(Term).filter(Ontology.name == ontology_name)
        inserted = s_terms.count()
        logger.info('Inserted terms %s', inserted)
        self.assertEqual(expected, inserted)

    def testLoadTimeMeta(self):
        ontology_name = 'bfo'
        self.loader.options['wipe'] = True
        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology(ontology_name)
            session.add(m_ontology)
            self.assertIsInstance(m_ontology, Ontology)
        session = dal.get_session()
        meta_file_date = session.query(Meta).filter_by(meta_key=ontology_name + '_file_date').one()
        meta_start = session.query(Meta).filter_by(meta_key=ontology_name + '_load_date').one()
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

        if self.db_url.startswith('mysql'):

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
        else:
            self.skipTest('No suitable engine for testing')

    def testMeta(self):
        session = dal.get_session()
        self.loader.init_meta()
        metas = session.query(Meta).all()
        self.assertGreaterEqual(len(metas), 2)

    def testEncodingTerm(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False
        session = dal.get_session()
        m_ontology = self.loader.load_ontology('fypo')
        session.add(m_ontology)
        term = helpers.Term(ontology_name='fypo', iri='http://purl.obolibrary.org/obo/FYPO_0005645')
        o_term = self.client.detail(term)
        m_term = self.loader.load_term(o_term, m_ontology, session)
        self.assertIn('λ', m_term.description)

    def testSingleTerm(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True

        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology('fypo')
            session.add(m_ontology)
            term = helpers.Term(ontology_name='fypo', iri='http://purl.obolibrary.org/obo/FYPO_0000257')
            o_term = self.client.detail(term)
            m_term = self.loader.load_term(o_term, m_ontology, session)
            session.commit()
            self.assertGreaterEqual(len(m_term.child_terms), 4)

    def testOntologiesList(self):
        self.assertIsInstance(self.loader.allowed_ontologies, list)
        self.assertIn('go', self.loader.allowed_ontologies)

    def testRelationsShips(self):
        with dal.session_scope() as session:
            m_ontology = self.loader.load_ontology('bto')
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
            m_ontology = self.loader.load_ontology('efo')
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
            self.assertIsNone(details.definition, '')

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
            for relation in m_term.child_terms:
                found = found or (relation.child_term.accession == 'CHEBI:24431')
        self.assertTrue(found)

    def testRelatedNonExpected(self):
        with dal.session_scope() as session:
            ontology_name = 'eco'
            expected = self.loader.load_ontology_terms(ontology_name, start=0, end=50)
            logger.info('Expected terms %s', expected)
            s_terms = session.query(Term).filter(Ontology.name == ontology_name)
            inserted = s_terms.count()
            logger.info('Inserted terms %s', inserted)
            self.assertGreaterEqual(inserted, expected)

    def testRelationSingleTerm(self):
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://purl.obolibrary.org/obo/ECO_0007571')
            m_term = self.loader.load_term(o_term, 'eco', session)
            session.add(m_term)
            session.commit()

    def testSubsetErrors(self):
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://www.ebi.ac.uk/efo/EFO_0003503')
            m_term = self.loader.load_term(o_term, 'efo', session)
            session.add(m_term)
            self.assertIsInstance(session.query(Subset).filter_by(name='efo_slim').one(), Subset)

    def testMissingOboId(self):
        self.loader.options['process_relations'] = False
        self.loader.options['process_parents'] = False
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://purl.obolibrary.org/obo/PR_P68993', unique=True, silent=True)
            m_term = self.loader.load_term(o_term, 'pr', session)
            self.assertEqual(m_term.accession, 'PR:P68993')

    def testExternalRelationship(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True
        with dal.session_scope() as session:
            o_term = self.client.term(identifier='http://www.ebi.ac.uk/efo/EFO_0002911', unique=True, silent=True)
            m_term = self.loader.load_term(o_term, 'efo', session)
            session.add(m_term)
            found = False
            for relation in m_term.child_terms:
                found = found or (relation.child_term.accession == 'OBI:0000245')
        self.assertTrue(found)
        session = dal.get_session()
        ontologies = session.query(Ontology).count()
        # assert that OBI has not been inserted
        self.assertEqual(1, ontologies)

    def testReport(self):
        size = 100
        self.loader = OlsLoader(self.db_url, page_size=size, output_dir='/tmp')
        o_ontology = self.client.ontology('ogms')
        ranges = range(o_ontology.number_of_terms)
        for i in ranges[::size]:
            self.loader.load_ontology_terms('ogms', i, min(i + size - 1, o_ontology.number_of_terms))
        self.loader.final_report('ogms')
        self.assertTrue(isfile('/tmp/ogms_report.log'))

    def testGoTerm(self):
        self.loader.options['process_relations'] = True
        self.loader.options['process_parents'] = True
        with dal.session_scope() as session:
            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/GO_0030118",
                                        ontology_name='go', type=helpers.Term)
            m_term = self.loader.load_term(o_term, 'go', session)
            session.add(m_term)
            self.assertIn('GO:0030117', [rel.parent_term.accession for rel in m_term.parent_terms])
            o_term = self.client.detail(iri="http://purl.obolibrary.org/obo/GO_0030131",
                                        ontology_name='go', type=helpers.Term)
            m_term = self.loader.load_term(o_term, 'go', session)
            session.add(m_term)
            self.assertIn('GO:0030119', [rel.parent_term.accession for rel in m_term.parent_terms if
                                         rel.relation_type.name == 'is_a'])
            self.assertIn('GO:0030118', [rel.parent_term.accession for rel in m_term.parent_terms if
                                         rel.relation_type.name == 'part_of'])
