#!/usr/bin/env python
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
import argparse
import logging
import os
import sys
from os.path import expanduser

from bio.ensembl.ontology.loader.db import dal
from bio.ensembl.ontology.loader.ols import OlsLoader

# allow ols.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

logging.basicConfig(
    filename='loader.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Produce a release calendar')
    parser.add_argument('-v', '--verbose', help='Verbose output', action='store_true')
    parser.add_argument('-o', '--ontology', help='Ontology short name', required=True, dest='ontology', default='all')
    parser.add_argument('-e', '--release', type=int, required=True, help='Release number')
    parser.add_argument('-k', '--keep', required=False, default=False, help='Keep database', action='store_true')
    parser.add_argument('-u', '--host_url', type=str, required=True,
                        help='Db Host Url format engine:///user:pass@host:port')
    parser.add_argument('-s', '--slice', help='Only load a slice of data format START-STOP', required=False)

    arguments = parser.parse_args(sys.argv[1:])
    logger.setLevel(logging.INFO)
    # logging.ERROR if arguments.verbose is None else logging.INFO if arguments.verbose is False else logging.DEBUG)
    logger.info('Script arguments: {}'.format(arguments))
    args = vars(parser.parse_args())
    db_name = 'ensembl_ontology_{}'.format(arguments.release)
    options = {'drop': not arguments.keep, 'echo': arguments.verbose, 'db_version': arguments.release}
    if arguments.host_url is None:
        db_url = 'sqlite:///' + expanduser("~") + '/' + db_name + '.sqlite'
        options.update({'pool_size': None})
    else:
        db_url = rreplace('{}/{}?charset=utf8'.format(arguments.host_url, db_name), '//', '/', 1)
    if arguments.slice is not None:
        slices = arguments.slice.split('-')
    else:
        slices = None
    logger.info('Db Url set to: {}'.format(db_url))
    logger.info('Loader arguments: {} {}'.format(db_url, options))
    logger.info('Slices: {}'.format(slices))

    response = input("Confirm to proceed (y/N)? ")

    if response.upper() != 'Y':
        logger.info('Process cancelled')
        exit(0)

    loader = OlsLoader(db_url, **options)

    if not arguments.keep:
        logger.info('Wiping %s ontology', arguments.ontology)
        loader.wipe_ontology(ontology_name=arguments.ontology)
        logger.info('Ontology %s reset', arguments.ontology)
    logger.info('Loading ontology %s', arguments.ontology)
    with dal.session_scope() as session:
        if slices is not None:
            n_terms, n_ignored = loader.load_ontology_terms(arguments.ontology, int(slices[0]), int(slices[1]))
        else:
            n_terms, n_ignored = loader.load_ontology_terms(arguments.ontology)
    logger.info('...Done')
