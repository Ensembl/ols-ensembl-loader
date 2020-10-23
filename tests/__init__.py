"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import configparser
import os


def read_env():
    """
    Reads a INI file named .env in the same directory manage.py is invoked and
    loads it as environment variables.
    Note: At least one section must be present. If the environment variable
    TEST_ENV is not set then the [DEFAULT] section will be loaded.
    More info: https://docs.python.org/3/library/configparser.html
    """
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(os.path.join(os.path.dirname(__file__), '.env'))
    section = os.environ.get("TEST_ENV", "DEFAULT")

    for var, value in config[section].items():
        os.environ.setdefault(var, value)


