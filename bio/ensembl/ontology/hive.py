# -*- coding: utf-8 -*-
# @author Marc Chakiachvili
from urllib import parse

import eHive

from .loader import OlsLoader


class OLSHiveLoader(eHive.BaseRunnable):
    """ OLS MySQL loader runnable class for eHive integration """

    def param_defaults(self):
        return {
            'db_url': 'sqlite://',
            'wipe_before_run': True  # currently not used in pipeline configuration
        }

    def fetch_input(self):
        assert self.param_required('ontology_name') in OlsLoader.ONTOLOGIES_LIST
        # TODO Check db exists
        db_url_parts = parse.urlparse(self.param_required('db_url'))
        assert db_url_parts.scheme in ('mysql', 'sqlite')
        assert db_url_parts.path != ''
        if db_url_parts.scheme == 'mysql':
            assert db_url_parts.port != ''
            assert db_url_parts.username != ''
            assert db_url_parts.password != ''
        # TODO Delete it if exists

    def run(self):
        self.warning("Ontology {} will be wiped from database " % self.param_required('wipe_before'))
        # False => erreur marque le job en failed, i.e pas de retry
        self.input_job.transient_error = False
        # TODO add default options
        ols_loader = OlsLoader(self.param_required('base_url'))
        if self.param_required('wipe_before_run'):
            ols_loader.wipe_ontology(self.param_required('ontology_name'))
        ols_loader.load_ontology(self.param_required('ontology_name'))

    def write_output(self):
        pass
