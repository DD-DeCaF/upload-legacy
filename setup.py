# -*- coding: utf-8 -*-
# Copyright 2013 Novo Nordisk Foundation Center for Biosustainability,
# Technical University of Denmark.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
    version='0.2.15',
    description="uploading data to iloop for dd-decaf",
    long_description=readme,
    author="Henning Redestig",
    author_email='henred@biosustain.dtu.dk',
    url='https://github.com/dd-decaf/upload',
    packages=[
        'upload',
    ],
    package_dir={'upload': 'upload'},
    include_package_data=True,
    install_requires=requirements,
    license='Apache License Version 2.0',
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
