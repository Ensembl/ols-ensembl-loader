# -*- coding: utf-8 -*-
import contextlib
import logging

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
                                               echo=False, encoding='utf8', convert_unicode=True)
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
        logger.debug('Exists %s', obj)
        return obj, False
    except NoResultFound:
        try:
            create_kwargs = create_method_kwargs or {}
            create_kwargs.update(kwargs)
            new_obj = getattr(model, create_method, model)(**create_kwargs)
            session.add(new_obj)
            session.commit()
            logger.debug('Create %s', new_obj)
            return new_obj, True
        except IntegrityError:
            logger.error('Integrity error upon flush')
            session.rollback()
            return session.query(model).filter_by(**kwargs).one(), False


dal = DataAccessLayer()
