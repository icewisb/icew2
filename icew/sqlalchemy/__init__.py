from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.orm.scoping import scoped_session


class ICEWSQLAlchemy(object):
    def __init__(self):
        self._config = {}
        self._config.setdefault('SQLALCHEMY_ECHO', True)

        self._engines = {}

        self._sessions = {}

        class MagicSessionCaller(object):
            def __init__(self, inst):
                self.inst = inst

            def __call__(self, bind_key=None):
                return self.inst.get_session(bind_key=bind_key)

            def __getattr__(self, attr):
                rv = getattr(self.inst.get_session(bind_key=None), attr)
                return rv

        self.session = MagicSessionCaller(self)

        class MagicEngineCaller(object):
            def __init__(self, inst):
                self.inst = inst

            def __call__(self, bind_key=None):
                return self.inst.get_engine(bind_key=bind_key)

            def __getattr__(self, attr):
                rv = getattr(self.inst.get_engine(bind_key=None), attr)
                return rv

        self.engine = MagicEngineCaller(self)

    @property
    def config(self):
        return self._config

    def configure(self, config=None, **kwargs):
        if config:
            self._config.update(config)
        if kwargs:
            self._config.update(kwargs)

    def get_engine(self, bind_key=None):
        if bind_key in self._engines:
            return self._engines[bind_key]

        if bind_key is None:
            config = self.config['SQLALCHEMY_BINDS']['default']
        else:
            config = self.config['SQLALCHEMY_BINDS'][bind_key]

        engine = self.create_engine(config)
        self._engines[bind_key] = engine
        return engine

    def create_engine(self, uri):
        options = {}
        if self.config['SQLALCHEMY_ECHO']:
            options.update({
                'echo': True,
            })
        return create_engine(uri, **options)

    def get_session(self, bind_key=None):
        if bind_key in self._sessions:
            return self._sessions[bind_key]

        engine = self.get_engine(bind_key)
        session = self.create_session(engine)
        self._sessions[bind_key] = session
        return session

    def create_session(self, engine=None):
        factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        return scoped_session(factory)

    def commit_all(self):
        for ses in self._sessions.values():
            ses.commit()

    def rollback_all(self):
        for ses in self._sessions.values():
            ses.rollback()

    def reset(self):
        for ses in self._sessions.values():
            ses.remove()
