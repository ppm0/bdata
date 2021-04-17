import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from bdata_model import Base

cfg = json.loads(open('config.json').read())

engine = create_engine(cfg['db'], echo=False, echo_pool=False, poolclass=NullPool)
Session = sessionmaker(bind=engine)

if __name__ == '__main__':
    Base.metadata.create_all(engine)

