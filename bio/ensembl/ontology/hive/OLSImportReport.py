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

import eHive
from eHive import JobFailedException

from . import param_defaults
from ..loader.ols import OlsLoader

logger = logging.getLogger(__name__)


class OLSImportReport(eHive.BaseRunnable):
    """ Dedicated loader for Ontology main page"""

    def run(self):
        # False => erreur marque le job en failed, i.e pas de retry
        options = param_defaults()
        options['wipe'] = self.param('wipe_one')
        options['ols_api_url'] = self.param('ols_api_url')
        options['page_size'] = self.param('page_size')
        options['output_dir'] = self.param('output_dir')
        self.input_job.transient_error = False
        logger.info('Creating loading report for %s', self.param_required('ontology_name'))
        ols_loader = OlsLoader(self.param_required('db_url'), **options)
        if not self.param_required('ontology_name').upper() in ols_loader.allowed_ontologies:
            raise JobFailedException("Ontology %s not implemented" % self.param_required('ontology_name'))
        ols_loader.final_report(self.param_required('ontology_name'))
        self.dataflow({
            'ontology_name': self.param_required('ontology_name'),
            'report_file': ols_loader.get_ontology_logger(self.param_required('ontology_name')).handlers[0].name}
        )

    def write_output(self):
        logger.info('Ontology %s done...', self.param_required('ontology_name'))
