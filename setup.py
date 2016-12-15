#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()

requirements = [
    'requests',
    'gnomic',
    'potion-client',
    'goodtables',
    'pandas'
]

test_requirements = [
    # TODO: put package requirements here
]

setup(
    name='upload',
    version='0.2.3',
    description="uploading data to iloop for dd-decaf",
    long_description=readme,
    author="Henning Redestig",
    author_email='henred@biosustain.dtu.dk',
    url='https://github.com/dd-decaf/upload',
    packages=[
        'upload',
    ],
    package_dir={'upload':
                     'upload'},
    include_package_data=True,
    install_requires=requirements,
    license="GNU General Public License v3",
    zip_safe=False,
    keywords='upload',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements,
)
