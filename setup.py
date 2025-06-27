###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

import io
import os
import re
from setuptools import Command, find_packages, setup
import shutil


class PyCleanBuild(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        remove_files = [
            'debian/files',
            'debian/wis2box-api.debhelper.log',
            'debian/wis2box-api.postinst.debhelper',
            'debian/wis2box-api.prerm.debhelper',
            'debian/wis2box-api.substvars'
        ]

        remove_dirs = [
            'debian/wis2box-api'
        ]

        for file_ in remove_files:
            try:
                os.remove(file_)
            except OSError:
                pass

        for dir_ in remove_dirs:
            try:
                shutil.rmtree(dir_)
            except OSError:
                pass

        for file_ in os.listdir('..'):
            if file_.endswith(('.deb', '.build', '.changes')):
                os.remove('../{}'.format(file_))


def read(filename, encoding='utf-8'):
    """read file contents"""
    full_path = os.path.join(os.path.dirname(__file__), filename)
    with io.open(full_path, encoding=encoding) as fh:
        contents = fh.read().strip()
    return contents


def get_package_version():
    """get version from top-level package init"""
    version_file = read('wis2box_api/__init__.py')
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError('Unable to find version string.')


LONG_DESCRIPTION = read('README.md')

DESCRIPTION = 'wis2box API'


if os.path.exists('MANIFEST'):
    os.unlink('MANIFEST')

setup(
    name='wis2box-api',
    version=get_package_version(),
    description=DESCRIPTION.strip(),
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    license='MIT',
    platforms='all',
    keywords=' '.join([
        'wmo',
        'wis2',
        'api'
    ]),
    author='Tom Kralidis',
    author_email='tomkralidis@gmail.com',
    maintainer='Tom Kralidis',
    maintainer_email='tomkralidis@gmail.com',
    url='https://github.com/World-Meteorological-Organization/wis2box-api',
    install_requires=read('requirements.txt').splitlines(),
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python'
    ],
    cmdclass={
        'cleanbuild': PyCleanBuild
    }
)
