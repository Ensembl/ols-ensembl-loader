# -*- coding: utf-8 -*-
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
import contextlib
import logging

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from .models import Base

logger = logging.getLogger(__name__)

__all__ = ['dal']


class DataAccessLayer:
    connection = None
    engine = None
    conn_string = None
    metadata = Base.metadata
    options = {}
    session = None

    def db_init(self, conn_string, **options):
        extra_params = {}
        if 'mysql' in conn_string:
            extra_params = dict(
                pool_recycle=options.get('pool_recycle', 280),
                pool_size=options.get('pool_size', 100)
            )

        self.engine = sqlalchemy.create_engine(conn_string,
                                               echo=options.get('echo', False),
                                               encoding='utf8',
                                               convert_unicode=True,
                                               **extra_params)
        self.options = options or {}
        self.connection = self.engine.connect()

    def create_schema(self):
        if not self.engine:
            raise RuntimeError('Please call db_init first')
        self.metadata.create_all(self.engine)

    def wipe_schema(self, conn_string):
        engine = sqlalchemy.create_engine(conn_string, echo=False)
        if not engine:
            raise RuntimeError("Can't wipe schema prior to init db")
        Base.metadata.drop_all(engine)

    def get_session(self):
        Session = sessionmaker()
        session = Session(bind=self.engine, autoflush=self.options.get('autoflush', False),
                         autocommit=self.options.get('autocommit', False))
        logger.debug('Create a new session ...%s ', session)
        return session

    @contextlib.contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.get_session()
        logger.debug('Open session')
        try:
            yield session
            session.commit()
            logger.debug('Commit session')
        except Exception as e:
            session.rollback()
            logger.error('Rollback session %s', e)
            raise
        finally:
            session.close()
            logger.debug('Closing session')


dal = DataAccessLayer()
