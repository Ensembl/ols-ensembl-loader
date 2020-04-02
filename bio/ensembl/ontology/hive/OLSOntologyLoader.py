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

import eHive
from eHive import JobFailedException

from bio.ensembl.ontology.loader.db import dal
from bio.ensembl.ontology.loader.ols import OlsLoader
from . import param_defaults, log_levels


class OLSOntologyLoader(eHive.BaseRunnable):
    """ Dedicated loader for Ontology main page"""

    def run(self):
        options = param_defaults()
        options['wipe'] = self.param('wipe_one')
        self.input_job.transient_error = False
        options['ols_api_url'] = self.param('ols_api_url')
        options['page_size'] = self.param('page_size')
        options['output_dir'] = self.param('output_dir')
        ols_loader = OlsLoader(self.param_required('db_url'), **options)
        # TODO update options with loader params
        logging.basicConfig(level=log_levels.get(self.param('verbosity'), logging.ERROR),
                            datefmt='%m-%d %H:%M:%S')
        logger = ols_loader.get_ontology_logger(self.param_required('ontology_name'))
        logger.info('Loading ontology info %s', self.param_required('ontology_name'))
        if self.param_required('wipe_one') == 1:
            logger.info("Wiping existing ontology data %s", self.param_required('ontology_name'))
            ols_loader.wipe_ontology(self.param_required('ontology_name'))
        try:
            assert self.param_required('ontology_name').upper() in ols_loader.allowed_ontologies
        except AssertionError:
            raise JobFailedException("Ontology %s not implemented" % self.param_required('ontology_name'))

        with dal.session_scope() as session:
            m_ontology = ols_loader.load_ontology(self.param_required('ontology_name'), session=session)
            session.add(m_ontology)
            self.dataflow({"ontology_name": self.param_required('ontology_name'), "nb_terms": m_ontology.number_of_terms})
