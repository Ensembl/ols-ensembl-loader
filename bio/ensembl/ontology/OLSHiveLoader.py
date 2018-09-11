# -*- coding: utf-8 -*-
# @author Marc Chakiachvili

from urllib import parse

import eHive

from loader import OlsLoader


class OLSHiveLoader(eHive.BaseRunnable):
    """ OLS MySQL loader runnable class for eHive integration """
    db_base_name = 'ensembl_ontology'

    def param_defaults(self):
        return {
            'drop_before': True,  # currently not used in pipeline configuration
        }

    def fetch_input(self):
        pass
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
        self.warning("Ontology {} will be wiped from database ".format(self.param_required('wipe_before_run')))
        # False => erreur marque le job en failed, i.e pas de retry
        self.input_job.transient_error = False
        # TODO add default options
        db_url = self.param_required('db_host') + '/' + self.db_base_name + self.param_required('db_version')
        ols_loader = OlsLoader(db_url)
        if self.param_required('drop_before') is True:
            ols_loader.wipe_ontology(self.param_required('ontology_name'))
        ols_loader.load(self.param_required('ontology_name'))

    def write_output(self):
        pass
