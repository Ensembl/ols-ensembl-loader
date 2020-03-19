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
import os
from os.path import join
from urllib import parse

import eHive

from bio.ensembl.ontology.loader.ols import OlsLoader, OlsMetaLoader, init_meta


class OLSHiveLoader(eHive.BaseRunnable):
    """ OLS MySQL loader runnable class for eHive integration """
    db_base_name = 'ensembl_ontology'
    log_levels = [
        logging.FATAL,
        logging.ERROR,
        logging.WARNING,
        logging.INFO,
        logging.DEBUG
    ]
    log_file = '%s_ontology.log'
    ols_loader = None

    def param_defaults(self):
        return {
            'drop_before': True,  # currently not used in pipeline configuration
            'echo': False,
            'verbosity': 1,
            'log_to': 'dev > null'
        }

    def fetch_input(self):
        options = self.param_defaults()
        options['db_version'] = self.param_required('ens_version')
        if self.param_required('wipe_one'):
            options['wipe'] = True
        # add loader option such as page_size, base_site for testing
        db_url_parts = parse.urlparse(self.param_required('db_url'))
        assert db_url_parts.scheme in ('mysql',)
        assert db_url_parts.path != ''
        if db_url_parts.scheme == 'mysql':
            assert db_url_parts.port != ''
            assert db_url_parts.username != ''
            assert db_url_parts.password != ''
        os.makedirs(self.param_required('output_dir'), exist_ok=True)
        logging.basicConfig(level=self.log_levels[self.param('verbosity')],
                            format='%(asctime)s %(levelname)s : %(name)s(%(lineno)d) - \t%(message)s',
                            datefmt='%m-%d %H:%M:%S',
                            filename=join(self.param_required('output_dir'),
                                          self.log_file % self.param_required('ontology_name')))

        ols_error_handler = logging.FileHandler(join(self.param_required('output_dir'),
                                                     self.log_file % self.param_required('ontology_name')))
        logger = logging.getLogger('ols_errors')
        logger.addHandler(ols_error_handler)
        logger.setLevel(logging.ERROR)
        options['output_dir'] = self.param_required('output_dir')
        init_meta(self.param_required('db_url'), **options)

    def run(self):
        raise RuntimeError('This class is not meant to be an actual Hive Wrapper')

    def write_output(self):
        raise RuntimeError('This class is not meant to be an actual Hive Wrapper')
