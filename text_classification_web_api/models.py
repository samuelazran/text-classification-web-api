__author__ = 'Samuel'
from sqlalchemy import MetaData, Table, Column, LargeBinary, Date, DateTime, Float, Boolean, Integer, Unicode, BigInteger
from sqlalchemy import ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql.expression import ClauseElement
from sqlalchemy.types import TypeDecorator, VARCHAR
from sqlalchemy import select, func

from settings import config

connection_string = 'mysql://%s:%s@%s/%s?charset=utf8' % (
    config['MYSQL_DATABASE_USER'],
    config['MYSQL_DATABASE_PASSWORD'],
    config['MYSQL_DATABASE_HOST'],
    config['MYSQL_DATABASE_DB']
    )

engine = create_engine(connection_string, convert_unicode=True, poolclass=NullPool, pool_recycle=60*60) #pool_recycle=60*60 if the connection is open for more than 60 minutes, replace it with new one
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

mysession = scoped_session(Session)
metadata = MetaData()

Base = declarative_base()
Base.metadata.bind = engine


import json

class JSONEncodedDict(TypeDecorator):
    "Represents an immutable structure as a json-encoded string."

    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value

def dispose(**kw):
    "release connection resources, untill next use of the session"
    mysession.close()
    mysession.bind.dispose()


class Datum(Base):
    "epresent single record of `data` table, can be article, post, status, tweet and such"
    __tablename__ = 'data'
    id = Column(Integer, primary_key = True)
    external_id = Column(BigInteger)
    language = Column(Unicode(3))
    source = Column(Unicode(25))
    domain = Column(Unicode(25))
    class_value = Column(Integer)
    gold = Column(Boolean)
    text = Column(Unicode)
    created_by = Column(Integer, ForeignKey('users.id'))
    hashtags = Column(JSONEncodedDict)
    urls = Column(JSONEncodedDict)
    media = Column(JSONEncodedDict)
    fetched_data = Column(JSONEncodedDict)
    fetched_at = Column(DateTime)
    created_at = Column(DateTime)
    user = relationship('User', backref='users')

class User(Base):
    "represent single record of 'users' table"
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    external_id = Column(BigInteger)
    name = Column(Unicode(50))
    screen_name = Column(Unicode(50))
    description = Column(Unicode(512))
    profile_image_url = Column(Unicode(512))
    verified = Column(Boolean)
    language = Column(Unicode(3))
    source = Column(Unicode(25))
    fetched_data = Column(JSONEncodedDict)
    fetched_at = Column(DateTime)
    created_at = Column(DateTime)


# columns from `data` table used to represent graph item
graph_data = Table('data', metadata,
    Column("id", Integer, primary_key = True),
    Column("language", Unicode(3)),
    Column("source", Unicode(25)),
    Column("domain", Unicode(25)),
    Column("class_value",Integer),
    Column("created_at", DateTime)
)

# build the graph query
try:
    GRAPH_MIN_VALUES_PER_DATUM = int(config['GRAPH_MIN_VALUES_PER_DATUM'])
except KeyError:
    GRAPH_MIN_VALUES_PER_DATUM = 5

values_sum = func.sum(graph_data.c.class_value).label('values_sum')
values_count = func.count(graph_data.c.class_value).label('values_count')
day_date = func.date(graph_data.c.created_at).label('datetime')

graph_data_select = select([
        graph_data.c.id,
        graph_data.c.language,
        graph_data.c.source,
        graph_data.c.domain,
        day_date,
        values_count,
        (values_sum / values_count * 100).label('value')
    ])\
    .group_by(day_date)\
    .having(values_count >= GRAPH_MIN_VALUES_PER_DATUM)\
    .order_by('datetime')\
    .alias()

class GrpahDatum(Base):
    "represent single item of the graph"
    __table__ = graph_data_select

#GrpahItem example usage
# print(graph_data_select)
#
#q = mysession.query(GrpahDatum).filter(graph_data.c.language=="en")
#
#print("filtered query:")
#print(q.statement)
#
# graph_data_result = q.all()
#
# for graph_datum in graph_data_result:
#     print ("id: {0:s}, value: {1:s}, datetime: {2:s}"
#            .format(str(graph_datum.id),
#                    str(graph_datum.value),
#                    str(graph_datum.datetime)
#            )
#     )



from text_classification.models import BaseModel
class Fetch(BaseModel):
    "represent fetch definitions - later will be moved to DB"
    def __init__(self, model_id=None, language=None, domain=None, source=None):
        BaseModel.__init__(self, model_id, language, domain, source)
        self.search_terms = []
        self.result_type = "popular" #"mixed"#"recent"
        self.results_amount = 20
        self._interval = 30*60 #default to 30 minutes
    @property
    def interval(self):
        return self._interval
    @property
    def interval_in_min(self):
        return self._interval/60
    @interval_in_min.setter
    def interval_in_min(self, value):
        self._interval = value * 60


def get_or_create(session, model, defaults=None, **kwargs):
    """
    get the record represented in the given model and if not exist create it
    used for INSERT IF NOT EXIST operations
    """
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance, False
    else:
        params = dict((k, v) for k, v in kwargs.iteritems() if not isinstance(v, ClauseElement))
        params.update(defaults or {})
        instance = model(**params)
        session.add(instance)
        return instance, True
