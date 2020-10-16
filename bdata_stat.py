import logging
import time

from sqlalchemy import text

from bdata_db import Session, engine


def make_stat():
    while True:
        logging.info('start stats calculation')
        with engine.connect() as con:
            con.execute(text("""
do
$$
    declare
        an   int[] := array [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        n    int;
        bsid bigint;
        code varchar(20);
    begin
        for i in 1..10
            loop
                bsid := (select max(book_snap_id) from book_snap where stat = false);
                if bsid is null
                then
                    raise notice 'nothing to do';
                    return;
                end if;
                raise notice 'bsid=%', bsid;
                delete from book_snap_stat where book_snap_id = bsid;
                foreach n in array an
                    loop
                        code := 'r' || n::text;
                        insert into book_snap_stat(book_snap_id, code, data) select bsid, code, bookSnapStat(bsid, n);
                        --raise notice 'code=%', code;
                    end loop;
                update book_snap set stat = true where book_snap_id = bsid;
            end loop;
        delete from book_snap_bid where book_snap_id = bsid;
        delete from book_snap_ask where book_snap_id = bsid;
    end
$$;
            """))
        logging.info('stop stats calculation')
        time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    make_stat()