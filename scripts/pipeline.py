##[debug]Evaluating: secrets.GOOGLE_CREDENTIALS
##[debug]Evaluating Index:
##[debug]..Evaluating secrets:
##[debug]..=> Object
##[debug]..Evaluating String:
##[debug]..=> 'GOOGLE_CREDENTIALS'
##[debug]=> '***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]***
##[debug]'
##[debug]Result: '***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]  ***
##[debug]***
##[debug]'
##[debug]Evaluating condition for step: 'Run pipeline'
##[debug]Evaluating: success()
##[debug]Evaluating success:
##[debug]=> true
##[debug]Result: true
##[debug]Starting: Run pipeline
##[debug]Loading inputs
##[debug]Loading env
Run python scripts/pipeline.py
##[debug]/usr/bin/bash -e /home/runner/work/_temp/e49f62d4-daa6-4200-aa80-17bbd5eefa7d.sh
/opt/hostedtoolcache/Python/3.10.19/x64/lib/python3.10/site-packages/google/api_core/_python_version_support.py:275: FutureWarning: You are using a Python version (3.10.19) which Google will stop supporting in new releases of google.api_core once it reaches its end of life (2026-10-04). Please upgrade to the latest Python version, or at least Python 3.11, to continue receiving updates for google.api_core past that date.
  warnings.warn(message, FutureWarning)
Traceback (most recent call last):
  File "/home/runner/work/data-pipeline-project/data-pipeline-project/scripts/pipeline.py", line 27, in <module>
    logging.basicConfig(
  File "/opt/hostedtoolcache/Python/3.10.19/x64/lib/python3.10/logging/__init__.py", line 2040, in basicConfig
    h = FileHandler(filename, mode,
  File "/opt/hostedtoolcache/Python/3.10.19/x64/lib/python3.10/logging/__init__.py", line 1169, in __init__
    StreamHandler.__init__(self, self._open())
  File "/opt/hostedtoolcache/Python/3.10.19/x64/lib/python3.10/logging/__init__.py", line 1201, in _open
    return open_func(self.baseFilename, self.mode,
FileNotFoundError: [Errno 2] No such file or directory: '/home/runner/work/data-pipeline-project/data-pipeline-project/logs/pipeline.log'
Error: Process completed with exit code 1.
