# utils/__init__.py
from .gcp_utils import *

import inspect, sys

def is_public_function(obj):
    return inspect.isfunction(obj) and not obj.__name__.startswith('_')

# Obtém todas as funções públicas do módulo gcp_utils
functions_list = [name for name, func in inspect.getmembers(sys.modules[__name__], is_public_function)]

__all__ = functions_list