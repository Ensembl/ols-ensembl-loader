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
import functools

from ensembl.ontology.models import *
from ebi.ols.api.helpers import *


def rgetattr(obj, attr, *args):
    def _getattr(obj, attr):
        return getattr(obj, attr, *args)

    return functools.reduce(_getattr, [obj] + attr.split('.'))


def set_namespace(ontology):
    if getattr(ontology.config.annotations, 'default_namespace', None):
        return ontology.config.annotations.default_namespace
    else:
        return ontology.config.namespace


def set_version(ontology):
    return ontology.verion if ontology.version else ontology.config.version


def load_ontology(ontology):
    m_ontology = Ontology(ontology)
    return m_ontology

