# -*- coding: utf-8 -*-
import contextlib
from os import getenv

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.exc import NoResultFound

autocommit = getenv('autocommit', False)
autoflush = getenv('autoflush', False)
Session = sessionmaker(autocommit=autocommit, autoflush=autoflush)

Base = declarative_base()


@contextlib.contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = Session()
    # print('Session binded to', session.bind)
    try:
        yield session
        # print(session.new)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


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


def exists(model, **kwargs):
    with session_scope() as session:
        try:
            obj = session.query(model).filter_by(**kwargs).one(), False
            return obj
        except NoResultFound:
            return False
