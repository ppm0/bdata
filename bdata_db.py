import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from bdata_model import Base

cfg = json.loads(open('config.json').read())

engine = create_engine(cfg['db'], echo=False, echo_pool = False, pool_size=8, max_overflow = 64)  # , pool_size=20, max_overflow=100
Session = sessionmaker(bind=engine)

if __name__ == '__main__':
    Base.metadata.create_all(engine)
