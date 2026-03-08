# conftest.py  ← sits at project root, same level as src/
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))