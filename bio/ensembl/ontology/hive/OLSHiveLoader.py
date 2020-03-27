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
import os
from urllib import parse

import eHive

from bio.ensembl.ontology.loader.ols import init_schema
from . import param_defaults


class OLSHiveLoader(eHive.BaseRunnable):
    """ OLS MySQL loader: initialise basics info in Ontology DB """

    def run(self):
        options = param_defaults()
        options['ens_version'] = self.param_required('ens_version')
        # add loader option such as page_size, base_site for testing
        db_url_parts = parse.urlparse(self.param_required('db_url'))
        assert db_url_parts.scheme in ('mysql', 'mysql+pymysql')
        assert db_url_parts.path != ''
        if db_url_parts.scheme == 'mysql':
            assert db_url_parts.port != ''
            assert db_url_parts.username != ''
            assert db_url_parts.password != ''
        os.makedirs(self.param_required('output_dir'), exist_ok=True)
        init_schema(self.param_required('db_url'), **options)
