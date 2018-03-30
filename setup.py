#!/usr/bin/env python3

# Always prefer setuptools over distutils
from setuptools import setup
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='roisto',
    version='3.0.0',
    description='Poll predictions from PubTrans SQL and publish to MONO via MQTT.',
    long_description=long_description,
    url='https://github.com/hsldevcom/roisto',
    author='haphut',
    author_email='haphut@gmail.com',
    license='AGPLv3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Programming Language :: Python :: 3.5',
        'Topic :: Database',
        'Topic :: Internet',
    ],
    keywords='doi roi noptis jore mqtt',
    packages=[
        'roisto',
        'roisto.match',
    ],
    install_requires=[
        'cachetools>=2.0.0,<2.1.0',
        'isodate>=0.5.4,<1',
        'paho-mqtt>=1.2,<2',
        'pymssql>=2.1.3,<3',
        'PyYAML>=3.12,<4',
    ],
    data_files=[('', [
        'LICENSE',
        'LICENSE_AGPL',
        'config.yaml.template',
    ]), ],
    entry_points={'console_scripts': ['roisto=roisto.roisto:main', ], }, )
