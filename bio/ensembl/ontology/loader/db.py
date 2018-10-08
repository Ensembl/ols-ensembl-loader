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
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from .models import Base

logger = logging.getLogger(__name__)

Session = sessionmaker()

__all__ = ['dal', 'get_one_or_create']


class DataAccessLayer:
    connection = None
    engine = None
    conn_string = None
    metadata = Base.metadata
    options = {}
    session = None

    def db_init(self, conn_string, **options):
        self.engine = sqlalchemy.create_engine(conn_string,
                                               pool_recycle=options.get('pool_recycle', 280),
                                               pool_size=options.get('pool_size', 100),
                                               echo=options.get('echo', False),
                                               encoding='utf8',
                                               convert_unicode=True)
        self.options = options or {}
        self.metadata.create_all(self.engine)
        self.connection = self.engine.connect()

    def wipe_schema(self, conn_string):
        engine = sqlalchemy.create_engine(conn_string, echo=False)
        Base.metadata.drop_all(engine)

    def get_session(self):
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
            logger.exception('Error in session %s', e)
            raise
        finally:
            session.close()
            logger.debug('Closing session')


def get_one_or_create(model,
                      session=None,
                      create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    c_session = session or dal.get_session()
    create_kwargs = create_method_kwargs or {}
    try:
        obj = c_session.query(model).filter_by(**kwargs).one()
        logger.debug('Exists %s', obj)
        if 'helper' in create_kwargs:
            obj.update_from_helper(helper=create_kwargs.get('helper'))
        else:
            [setattr(obj, attribute, create_kwargs.get(attribute)) for attribute in create_kwargs if
             attribute is not None]
        logger.debug('Updated %s', obj)
        return obj, False
    except NoResultFound:
        try:
            create_kwargs.update(kwargs)
            new_obj = getattr(model, create_method, model)(**create_kwargs)
            c_session.add(new_obj)
            c_session.commit()
            logger.debug('Create %s', new_obj)
            return new_obj, True
        except IntegrityError:
            logger.error('Integrity error upon flush')
            c_session.rollback()
            return c_session.query(model).filter_by(**kwargs).one(), False


dal = DataAccessLayer()
