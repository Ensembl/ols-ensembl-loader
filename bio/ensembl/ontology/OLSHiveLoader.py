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
from os.path import join
from urllib import parse

import eHive

from loader import OlsLoader


class OLSHiveLoader(eHive.BaseRunnable):
    """ OLS MySQL loader runnable class for eHive integration """
    db_base_name = 'ensembl_ontology'
    log_levels = [
        logging.ERROR,
        logging.WARNING,
        logging.INFO,
        logging.DEBUG
    ]
    log_file = 'ols_loader_%s.log'
    ols_loader = None

    def param_defaults(self):
        return {
            'drop_before': True,  # currently not used in pipeline configuration
            'echo': False,
            'verbosity': 0,
            'log_to': 'dev > null'
        }

    def fetch_input(self):
        options = self.param_defaults()
        options['db_version'] = self.param_required('ens_version')
        if self.param_required('drop_before') is False:
            options['wipe'] = False

        db_url_parts = parse.urlparse(self.param_required('db_url'))
        assert db_url_parts.scheme in ('mysql')
        assert db_url_parts.path != ''
        if db_url_parts.scheme == 'mysql':
            assert db_url_parts.port != ''
            assert db_url_parts.username != ''
            assert db_url_parts.password != ''

        logging.basicConfig(level=self.log_levels[self.param('verbosity')],
                            format='%(asctime)s %(levelname)s : %(name)s.%(funcName)s(%(lineno)d) - %(message)s',
                            datefmt='%m-%d %H:%M - %s',
                            filename=join(self.param_required('output_dir'),
                                          self.log_file % self.param_required('ontology_name')),
                            filemode='w')
        self.ols_loader = OlsLoader(self.param_required('db_url'), **options)
        self.ols_loader.init_meta()
        assert self.param_required('ontology_name') in self.ols_loader.allowed_ontologies

    def run(self):
        # False => erreur marque le job en failed, i.e pas de retry
        self.input_job.transient_error = False
        # TODO add default options
        self.ols_loader.load_all(self.param_required('ontology_name'))

    def write_output(self):
        pass
