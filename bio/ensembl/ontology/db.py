# -*- coding: utf-8 -*-
import contextlib
import logging
from os import getenv

import sqlalchemy
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from ensembl.ontology.models import Base

autocommit = getenv('autocommit', False)
autoflush = getenv('autoflush', False)

logger = logging.getLogger(__name__)

Session = sessionmaker(autocommit=True, autoflush=True)


class DataAccessLayer:
    connection = None
    engine = None
    conn_string = None
    metadata = Base.metadata
    session = None

    def db_init(self, conn_string, **options):
        # TODO add autocommit / autoflush in options
        print('in init', conn_string)
        self.engine = sqlalchemy.create_engine(conn_string or self.conn_string,
                                               pool_recycle=options.get('timeout', 36000),
                                               echo=options.get('echo', False))
        self.session = Session(bind=self.engine)
        self.session.close()
        print(self.session_scope())
        self.metadata.create_all(self.engine)
        # self.session.close()
        # self.connection = engine.connect()
        # self.connection.close()

    @contextlib.contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = Session(bind=self.engine)
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


dal = DataAccessLayer()


def get_one_or_create(model, session,
                      create_method='',
                      create_method_kwargs=None,
                      **kwargs):
    # with session_scope() as session:
    try:
        # print('querying model', model, kwargs)
        obj = session.query(model).filter_by(**kwargs).one()
        return obj, False
    except NoResultFound:
        kwargs.update(create_method_kwargs or {})
        # print('created', kwargs)
        created = getattr(model, create_method, model)(**kwargs)
        try:
            session.add(created)
            session.commit()
            # print('object created and flushed', created)
            return created, True
        except IntegrityError:
            session.rollback()
            if create_method_kwargs:
                [kwargs.pop(key) for key in create_method_kwargs.keys()]
            # print('search', kwargs)
            return session.query(model).filter_by(**kwargs).one(), False
