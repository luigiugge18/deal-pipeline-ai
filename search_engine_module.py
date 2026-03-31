"""
Alias module: exposes functions from 03_search_engine.py
which cannot be imported directly because its name starts with a digit.
"""
import importlib.util
import os
import sys

_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "03_search_engine.py")
_spec = importlib.util.spec_from_file_location("_search_engine_impl", _path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["_search_engine_impl"] = _mod
_spec.loader.exec_module(_mod)

# Export the functions needed by 06_api.py
search = _mod.search
get_company = _mod.get_company
get_embedding = _mod.get_embedding
search_structured = _mod.search_structured
search_semantic = _mod.search_semantic
