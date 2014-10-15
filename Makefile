.PHONY: help clean clean-pyc clean-build list test test-all coverage docs release sdist

help:
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "lint - check style with flake8"
	@echo "test - run tests quickly with the default Python"
	@echo "testall - run tests on every Python version with tox"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "docs - generate Sphinx HTML documentation, including API docs"
	@echo "release - package and upload a release"
	@echo "sdist - package"

clean: clean-build clean-pyc

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

lint:
	pylint -E dht3k
	pylint -E lazymq
	pylint -E test

devtest:
	py.test -x -s -v -m "not slowtest"

test:
	py.test --cov dht3k --cov-report=term-missing

test-all:
	tox

coverage:
	coverage run --source dht3k setup.py test
	coverage report -m
	coverage html
	open htmlcov/index.html

docs:
	sphinx-apidoc -o docs/ dht3k
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	open docs/_build/html/index.html

release: clean
	python setup.py sdist upload
	python setup.py bdist_wheel upload

sdist: clean
	python setup.py sdist
	python setup.py bdist_wheel upload
	ls -l dist
