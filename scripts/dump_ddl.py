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
import os
import sys
from contextlib import redirect_stdout

import sqlalchemy

from bio.ensembl.ontology.loader.models import *

base_dir = os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir))
os.chdir(base_dir)

file = os.path.abspath(os.path.join(os.path.dirname(base_dir), 'sql', 'tables.sql'))


def dump(sql, *multiparams, **params):
    global out_file
    with open(out_file, 'a') as f:
        with redirect_stdout(f):
            print(sql.compile(dialect=engine.dialect))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Produce a release calendar')
    parser.add_argument('-f', '--out_file', help='Output file', default=file)
    args = parser.parse_args(sys.argv[1:])
    print('Will write DDL to:', args.out_file)
    response = input("Confirm to proceed (y/N)? ")
    out_file = args.out_file
    if response.upper() != 'Y':
        exit(0)

    engine = sqlalchemy.create_engine('mysql://', strategy='mock', executor=dump)
    open(out_file, 'w').close()
    Base.metadata.create_all(engine, checkfirst=False)
