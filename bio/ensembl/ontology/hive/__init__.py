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

db_base_name = 'ensembl_ontology'
log_levels = {
    '1': logging.FATAL,
    '2': logging.ERROR,
    '3': logging.WARNING,
    '4': logging.INFO,
    '5': logging.DEBUG
}

log_file = '%s_ontology.log'
err_file = '%s_ontology.err'


def param_defaults():
    return {
        'drop_before': True,
        'verbosity': 2
    }
