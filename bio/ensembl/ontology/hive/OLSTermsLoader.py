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

from ebi.ols.api import exceptions
from . import param_defaults, log_levels
from ..loader.ols import OlsLoader


class OLSTermsLoader(eHive.BaseRunnable):
    """ OLS MySQL loader runnable class for eHive integration """

    def run(self):
        options = param_defaults()
        options['wipe'] = self.param('wipe_one')
        options['ols_api_url'] = self.param('ols_api_url')
        options['page_size'] = self.param('page_size')
        options['output_dir'] = self.param('output_dir')
        options['page_size'] = 200
        log_level = log_levels.get(self.param('verbosity'), logging.ERROR)
        log_level = logging.DEBUG
        options['verbosity'] = log_level
        logging.basicConfig(level=log_level, datefmt='%m-%d %H:%M:%S')
        ols_loader = OlsLoader(self.param_required('db_url'), **options)
        logger = ols_loader.get_ontology_logger(self.param_required('ontology_name'))
        self.input_job.transient_error = False
        logger.info('HiveTermsLoader: Loading %s ontology terms [%s..%s]',
                    self.param_required('ontology_name'),
                    self.param_required('_start_term_index'),
                    self.param_required('_end_term_index'))
        try:
            ols_loader.load_ontology_terms(self.param_required('ontology_name'),
                                           start=self.param_required('_start_term_index'),
                                           end=self.param_required('_end_term_index'))
            logger.info('Loaded %s ontology terms [%s..%s]',
                        self.param_required('ontology_name'),
                        self.param_required('_start_term_index'),
                        self.param_required('_end_term_index'))
        except exceptions.OlsException as e:
            message = "%s[%s:%s] %s" % (self.param_required('ontology_name'),
                                        self.param_required('_start_term_index'),
                                        self.param_required('_end_term_index'), e)
            raise JobFailedException("Error loading slice %s" % message)
