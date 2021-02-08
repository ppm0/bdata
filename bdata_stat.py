import logging
import time
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import text

from bdata_db import engine


def make_stat_step() -> None:
    with engine.connect().execution_options(autocommit=True) as connection:
        connection.execute(text("""
            do $$
                declare
                    an   numeric[] := array [0.01, 0.02, 0.03, 0.05, 0.08, 0.1, 0.2, 0.3, 0.5, 0.8, 1, 2, 3, 5, 8, 10, 20, 30, 50, 80, 100];
                    n    numeric;
                    bsid bigint;
                    code varchar(20);
                    bsida bigint[];
                    cur record;
                begin
                    for cur in select * from book_snap where not stat limit 100 for update skip locked loop
                        bsid := cur.book_snap_id;
                        if bsid is null
                        then
                            raise notice 'nothing to do';
                            return;
                        end if;
                        bsida = array_append(bsida, bsid);
                        raise notice 'bsid=%', bsid;
                        delete from book_snap_stat where book_snap_id = bsid;
                        foreach n in array an
                            loop
                                code := 'r' || n::text;
                                -- raise notice 'code=%', code;
                                insert into book_snap_stat(book_snap_id, code, data)
                                select bsid, code, bookSnapStat(bsid, n);
                            end loop;
                        --update book_snap set stat = true where book_snap_id = bsid;
                        update book_snap set stat = true where book_snap_id = cur.book_snap_id;
                    end loop;
                    delete from book_snap_bid where book_snap_id = any(bsida);
                    delete from book_snap_ask where book_snap_id = any(bsida);
                end
            $$;
                    """))


def make_stats():
    while True:
        try:
            logging.info('start stats calculation')
            with ThreadPoolExecutor(max_workers=8) as ex:
                for i in range(0, 8):
                    ex.submit(make_stat_step)
            logging.info('stop stats calculation')
        except Exception as e:
            logging.error(str(e))
            pass
        time.sleep(0.5)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    make_stats()
