#!/usr/bin/env python
import argparse
import logging
import os
import sys
from os.path import expanduser


from bio.ensembl.ontology.loader import OlsLoader
from bio.ensembl.ontology.loader.db import dal

# allow ols.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Produce a release calendar')
    parser.add_argument('-v', '--verbose', help='Verbose output', action='store_true')
    parser.add_argument('-o', '--ontology', help='Ontology short name', required=True, dest='ontology', default='all')
    parser.add_argument('-e', '--release', type=int, required=True, help='Release number')
    parser.add_argument('-k', '--keep', required=False, help='Keep database', action='store_true')
    parser.add_argument('-u', '--host_url', type=str, required=False,
                        help='Db Host Url format engine:///user:pass@host:port')

    arguments = parser.parse_args(sys.argv[1:])
    logger.setLevel(
        logging.ERROR if arguments.verbose is None else logging.INFO if arguments.verbose is False else logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logger.debug('Script arguments %s', arguments)
    args = vars(parser.parse_args())
    db_name = 'ensembl_ontology_{}'.format(arguments.release)
    options = {'drop': not arguments.keep, 'echo': arguments.verbose}
    if arguments.host_url is None:
        db_url = 'sqlite:///' + expanduser("~") + '/' + db_name + '.sqlite'
        options.update({'pool_size':None})
    else:
        db_url = '{}/{}'.format(arguments.host_url, db_name)
    logger.debug('Db Url set to %s', db_url)
    logger.info('Loader arguments %s %s ', db_url, options)
    response = input("Confirm to proceed (y/N)? ")

    if response.upper() != 'Y':
        logging.info('Process cancelled')
        exit(0)

    loader = OlsLoader(db_url, **options)

    if not arguments.keep:
        logger.info('Wiping %s ontology', arguments.ontology)
        loader.wipe_ontology(ontology_name=arguments.ontology)
        logger.info('Ontology %s reset', arguments.ontology)
    logger.info('Loading ontology %s', arguments.ontology)
    with dal.session_scope() as session:
        n_terms = loader.load_ontology_terms(arguments.ontology)
    logger.info('...Done')
