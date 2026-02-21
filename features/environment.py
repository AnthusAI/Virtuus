import os
import sys

# Add python/src to sys.path so 'import virtuus' works without pip install
_features_dir = os.path.dirname(os.path.abspath(__file__))
_repo_root = os.path.dirname(_features_dir)
_python_src = os.path.join(_repo_root, "python", "src")
if _python_src not in sys.path:
    sys.path.insert(0, _python_src)
