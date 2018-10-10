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
# @author Marc Chakiachvili
import logging

from bio.ensembl.ontology.loader.db import dal
from .OLSHiveLoader import OLSHiveLoader

logger = logging.getLogger(__name__)


class OLSOntologyLoader(OLSHiveLoader):
    """ Dedicated loader for Ontology main page"""

    def run(self):
        # False => erreur marque le job en failed, i.e pas de retry
        self.input_job.transient_error = False
        logger.info('Loading ontology info %s', self.param_required('ontology_name'))
        if self.param_required('wipe_one') == 1:
            self.ols_loader.wipe_ontology(self.param_required('ontology_name'))

        with dal.session_scope() as session:

            m_ontology = self.ols_loader.load_ontology(self.param_required('ontology_name'))
            session.add(m_ontology)
            self.dataflow({'nb_terms': m_ontology.number_of_terms,
                           'ontology_name': self.param_required('ontology_name')
                           })

    def write_output(self):
        logger.info('Ontology %s done...', self.param_required('ontology_name'))
