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

import logging

from bio.ensembl.ontology.hive.OLSHiveLoader import OLSHiveLoader
from bio.ensembl.ontology.loader.db import dal
from bio.ensembl.ontology.loader.models import Ontology, Term, Relation, RelationType, get_one_or_create

logger = logging.getLogger(__name__)


class OLSLoadPhiBaseIdentifier(OLSHiveLoader):

    def run(self):
        self.input_job.transient_error = False
        logger.info('Loading PHIBASe Identifier terms')
        with dal.session_scope() as session:
            # delete phi-base-identifier namespaces ontology
            if self.param_required('_start_term_index') == 0:
                # only delete for first chunk
                ontologies = session.query(Ontology).filter_by(name='phi', namespace='phibase_identifier').all()
                for ontology in ontologies:
                    logger.info('Deleting namespaced ontology %s - %s', ontology.name, ontology.namespace)
                    rel = session.query(Relation).filter_by(ontology=ontology).delete()
                    logger.info('Wiped %s Relations', rel)
                    res = session.query(Term).filter_by(ontology=ontology).delete()
                    logger.info('Wiped %s Terms', res)
                    logger.debug('...Done')
            m_ontology, created = get_one_or_create(Ontology, session,
                                                    name='phi',
                                                    namespace='phibase_identifier',
                                                    create_method_kwargs=dict(
                                                        version='1.0',
                                                        title='PHI-base Identifiers')
                                                    )
            relation_type, created = get_one_or_create(RelationType,
                                                       session,
                                                       name='is_a')
            for i in range(self.param_required('_start_term_index'), self.param_required('_end_term_index') + 1):
                accession = 'PHI:{}'.format(i)
                term = Term(accession=accession, name='{}'.format(i))
                if i == 0:
                    term.name = 'phibase identifier'
                    term.is_root = 1
                    m_related = term
                else:
                    m_related = session.query(Term).filter_by(accession='PHI:0').one()
                logger.debug('Adding Term %s', accession)
                session.add(term)
                m_ontology.terms.append(term)
                if i != 0:
                    term.add_parent_relation(m_related, relation_type, session)
                else:
                    m_related = term
                if i % 100 == 0:
                    logger.info('Committing transaction')
                    session.commit()

    def write_output(self):
        logger.info('Ontology %s done...', self.param_required('ontology_name'))
