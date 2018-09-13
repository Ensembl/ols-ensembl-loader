# -*- coding: utf-8 -*-
import contextlib
import logging
from urllib.parse import urlparse

import sqlalchemy
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from .models import Base

logger = logging.getLogger(__name__)

Session = sessionmaker()


class DataAccessLayer:
    connection = None
    engine = None
    conn_string = None
    metadata = Base.metadata
    options = {}
    session = None

    def db_init(self, conn_string, **options):

        self.engine = sqlalchemy.create_engine(conn_string,
                                               pool_recycle=options.get('timeout', 36000),
                                               echo=False,
                                               convert_unicode=True)
        self.options = options or {}
        self.metadata.create_all(self.engine)
        self.connection = self.engine.connect()

    def wipe_schema(self, conn_string):
        engine = sqlalchemy.create_engine(conn_string, echo=False)
        Base.metadata.drop_all(engine)

    def get_session(self):
        if not self.session or not self.session.is_active:
            self.session = Session(bind=self.engine, autoflush=self.options.get('autoflush', False),
                                   autocommit=self.options.get('autocommit', False))
        return self.session

    @contextlib.contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        # get_session = Session(bind=self.engine)
        session = Session(bind=self.engine, autoflush=self.options.get('autoflush', False),
                          autocommit=self.options.get('autocommit', False))
        logger.debug('Open session')
        try:
            yield session
            logger.debug('Commit session')
            session.commit()
        except Exception as e:
            logger.exception('Error in session %s', e)
            session.rollback()
            raise
        finally:
            logger.debug('Closing session')
            session.close()


def get_one_or_create(model,
                      create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    session = dal.get_session()
    try:
        obj = session.query(model).filter_by(**kwargs).one()
        logger.debug('Existing entity %s: %s', model, kwargs)
        return obj, False
    except NoResultFound:
        try:
            kwargs.update(create_method_kwargs or {})
            logger.debug('Not existing %s entity for params %s', model, kwargs)
            created = getattr(model, create_method, model)(**kwargs)
            session.add(created)
            session.flush([created])
            session.commit()
            # print('object is get_session', created in get_session)
            logger.debug('Created: %s', created)
            return created, True
        except IntegrityError:
            logger.info('Integrity error upon flush')
            session.rollback()
            if create_method_kwargs is not None:
                [kwargs.pop(key) for key in create_method_kwargs.keys()]
            logger.debug('%s', kwargs)
            return session.query(model).filter_by(**kwargs).one(), False


dal = DataAccessLayer()
