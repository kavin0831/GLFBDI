@echo off
REM Install test deps and run the test suite.
pip install -r requirements-test.txt
pytest tests/ -v
