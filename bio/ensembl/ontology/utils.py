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

