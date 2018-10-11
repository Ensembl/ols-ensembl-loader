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

logger = logging.getLogger(__name__)

from .OLSHiveLoader import OLSHiveLoader


class OLSTermsLoader(OLSHiveLoader):
    """ OLS MySQL loader runnable class for eHive integration """

    def run(self):
        # False => erreur marque le job en failed, i.e pas de retry
        self.input_job.transient_error = False
        # TODO add default options
        logger.info('Loading %s ontology terms [%s..%s]',
                    self.param_required('ontology_name'),
                    self.param_required('_start_term_index'),
                    self.param_required('_end_term_index'))
        self.ols_loader.load_ontology_terms(self.param_required('ontology_name'),
                                            start=self.param_required('_start_term_index'),
                                            end=self.param_required('_end_term_index'))

    def write_output(self):
        logger.info('Done %s ontology terms [%s..%s]',
                    self.param_required('ontology_name'),
                    self.param_required('_start_term_index'),
                    self.param_required('_end_term_index'))
