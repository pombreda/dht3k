#!/usr/bin/env python

import os
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

readme = open('README.rst').read()
doclink = """
Documentation
-------------

The full documentation is at http://dht3k.rtfd.org."""
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    name='dht3k',
    version='0.1.0',
    description=readme + '\n\n' + doclink + '\n\n' + history,
    author='Jean-Louis Fuchs',
    author_email='ganwell@fangorn.ch',
    url='https://github.com/ganwell/dht3k',
    packages=[
        'dht3k',
    ],
    package_dir={'dht3k': 'dht3k'},
    include_package_data=True,
    install_requires=[
        "msgpack-python",
        "six",
        "futures",
        "ipaddress",
    ],
    license='MIT',
    zip_safe=False,
    keywords='dht3k',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
    ],
)
