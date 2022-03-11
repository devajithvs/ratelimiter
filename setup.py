# Original work Copyright 2013 Arnaud Porterie
# Modified work Copyright 2015 Frazer McLean
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
from setuptools import setup

FILE = "ratelimiter/__init__.py"
init_data = open(FILE).read()

metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", init_data))

AUTHOR_EMAIL = metadata["author"]
VERSION = metadata["version"]
LICENSE = metadata["license"]
DESCRIPTION = metadata["description"]

AUTHOR, EMAIL = re.match(r"(.*) <(.*)>", AUTHOR_EMAIL).groups()

extras_require = dict()

extras_require["test"] = {"pytest>=3.0"}

extras_require['test:python_version>="3.5"'] = {"pytest-asyncio"}

setup(
    name="ratelimiter",
    version=VERSION,
    description=DESCRIPTION,
    long_description=open("README.rst").read(),
    author=AUTHOR,
    author_email=EMAIL,
    url="https://github.com/RazerM/ratelimiter",
    packages=["ratelimiter"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
    ],
    license=LICENSE,
    extras_require=extras_require,
)
