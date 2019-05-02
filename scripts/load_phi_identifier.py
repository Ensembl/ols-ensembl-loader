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
from bio.ensembl.ontology.loader.models import Ontology, Term, Relation, RelationType, get_one_or_create

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
    parser.add_argument('-e', '--release', type=int, required=True, help='Release number')
    parser.add_argument('-u', '--host_url', type=str, required=False,
                        help='Db Host Url format engine:///user:pass@host:port')

    args = parser.parse_args(sys.argv[1:])
    logger.setLevel(
        logging.ERROR if args.verbose is None else logging.INFO if args.verbose is False else logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logger.debug('Script arguments %s', args)
    db_name = 'ensembl_ontology_{}'.format(args.release)
    if args.host_url is None:
        db_url = 'sqlite:///' + expanduser("~") + '/' + db_name + '.sqlite'
    else:
        db_url = '{}{}'.format(args.host_url, db_name)
    logger.debug('Db Url set to %s', db_url)

    response = input("Confirm to proceed (y/N)? ")

    if response.upper() != 'Y':
        logging.info('Process cancelled')
        exit(0)
    dal.db_init(db_url)
    with dal.session_scope() as session:
        ontologies = session.query(Ontology).filter_by(name='phi', namespace='phibase_identifier').all()
        for ontology in ontologies:
            logger.info('Deleting namespaced ontology %s - %s', ontology.name, ontology.namespace)
            rel = session.query(Relation).filter_by(ontology=ontology).delete()
            res = session.query(Term).filter_by(ontology=ontology).delete()
            logger.info('Wiped %s Terms', res)
            logger.debug('...Done')
        m_ontology, created = get_one_or_create(Ontology, session,
                                                name='phi',
                                                namespace='phibase_identifier',
                                                create_method_kwargs=dict(
                                                    version='1.0',
                                                    title='PHIBase identifier')
                                                )
        relation_type, created = get_one_or_create(RelationType,
                                                   session,
                                                   name='is_a')
        for i in range(10000):
            accession = 'PHI:{}'.format(i)
            term = Term(accession=accession, name='{}' % i)
            if i == 0:
                term.is_root = 1

            logger.debug('Adding Term %s', accession)
            session.add(term)
            m_ontology.terms.append(term)
            if i != 0:
                term.add_parent_relation(m_related, relation_type, session)
            else:
                m_related = term
            if i % 100 == 0:
                logger.info('Committing transaction')
                session.commit()

    logger.info('...Done')
