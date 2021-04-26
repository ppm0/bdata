import logging
import time
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import text

from bdata_db import engine


def make_stat_step_book() -> None:
    with engine.connect().execution_options(autocommit=True) as connection:
        connection.execute(text("""
            do $$
                declare
                    an   numeric[] := array [0.01, 0.02, 0.03, 0.05, 0.08, 0.1, 0.2, 0.3, 0.5, 0.8, 
                        1, 2, 3, 5, 8, 10, 20, 30, 50, 80, 100];
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


def make_stat_step_trade(m: int, n : int):
    with engine.connect().execution_options(autocommit=True) as connection:
        connection.execute(text(
            f"""
do
$$
    declare
        cur    record;
        cur2   record;
        last_c numeric;
    begin
        for cur in
            with s1 as (select exchange_market_id,
                            coalesce((select max(dt) from trade1m where exchange_market_id = em.exchange_market_id) +
                                     interval '1m'
                                , date_trunc('minute', (select min(to_timestamp(ts::numeric / 1000::numeric))
                                                        from trade
                                                        where exchange_market_id = em.exchange_market_id))
                                ) gst,
                                date_trunc('minute', to_timestamp(trade_ts::numeric / 1000::numeric)) -
                                interval '1m' gen
                        from exchange_market em
                        where em.trade_ts is not null and not coalesce(em.disabled, false)
                        order by exchange_market_id desc)
            select exchange_market_id, gst, gen
            from s1
            where gst <= gen
                and exchange_market_id % {m} = {n}
            order by exchange_market_id
            loop
                --raise notice '% % %', cur.exchange_market_id, cur.gst, cur.gen;
                last_c = (select c
                          from trade1m
                          where exchange_market_id = cur.exchange_market_id and dt = cur.gst - interval '1m');
                for cur2 in
                    select dt as dt,
                           t.o, t.h, t.l, t.c, t.s, t.sb, t.ss, t.z, t.zb, t.zs, t.n, t.nb, t.ns, t.d
                    from generate_series(cur.gst::timestamp, cur.gen::timestamp, interval '1m') dt,
                        tradeohlc(cur.exchange_market_id, dt.dt, interval '1m') t
                    limit 89
                    loop
                        if cur2.n > 0
                        then
                            insert into trade1m(dt, exchange_market_id, o, h, l, c, s, sb, ss, z, zb, zs, n, nb, ns, d)
                            values (cur2.dt, cur.exchange_market_id,
                                    cur2.o, cur2.h, cur2.l, cur2.c,
                                    cur2.s, cur2.sb, cur2.ss,
                                    cur2.z, cur2.zb, cur2.zs,
                                    cur2.n, cur2.nb, cur2.ns,
                                    cur2.d);
                            last_c = cur2.c;
                        else
                            insert into trade1m(dt, exchange_market_id, o, h, l, c, s, sb, ss, z, zb, zs, n, nb, ns, d)
                            values (cur2.dt, cur.exchange_market_id,
                                    last_c, last_c, last_c, last_c,
                                    cur2.s, cur2.sb, cur2.ss,
                                    cur2.z, cur2.zb, cur2.zs,
                                    0, 0, 0,
                                    0);
                        end if;
                    end loop;
            end loop;
    end;
$$;
            """))


WORKERS = 16
BOOK_WORKERS = 2
TRADE_WORKERS = 16
DELAY = 5


def make_stats():
    while True:
        try:
            logging.info('start stats calculation')
            with ThreadPoolExecutor(max_workers=BOOK_WORKERS + TRADE_WORKERS) as ex:
                for i in range(0, BOOK_WORKERS):
                    ex.submit(make_stat_step_book)
                for i in range(0, TRADE_WORKERS):
                    ex.submit(make_stat_step_trade, TRADE_WORKERS, i)
            logging.info('stop stats calculation')
        except Exception as e:
            logging.error(str(e))
            pass
        time.sleep(DELAY)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    make_stats()
