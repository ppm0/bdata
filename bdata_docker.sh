#!/bin/sh

EXCHANGES='bitfinex binance bittrex kraken hitbtc huobipro poloniex kucoin bitflyer bitstamp cex coinbasepro upbit gemini'

case $1 in
run)
  for EXCHANGE in $EXCHANGES; do
    docker run -itd --name="bdata_""$EXCHANGE""_all_btc" --restart=unless-stopped --memory=1g -v /home/bot/bdata/config.json:/app/config.json bdata python -OO bdata.py --interval=60 --exchange=$EXCHANGE --base=* --quote=BTC --snap_target=trade
  done
  docker run -itd --name=bdata_all_btc_usd --restart=unless-stopped --memory=1g -v /home/bot/bdata/config.json:/app/config.json bdata python -OO bdata.py --interval=60 --exchange=* --base=BTC --quote=USD --snap_target=trade
  docker run -itd --name=bdata_all_btc_eur --restart=unless-stopped --memory=1g -v /home/bot/bdata/config.json:/app/config.json bdata python -OO bdata.py --interval=60 --exchange=* --base=BTC --quote=EUR --snap_target=trade
  docker run -itd --name=bdata_all_btc_usdt --restart=unless-stopped --memory=1g -v /home/bot/bdata/config.json:/app/config.json bdata python -OO bdata.py --interval=60 --exchange=* --base=BTC --quote=USDT --snap_target=trade
  docker run -itd --name=bdata_all_nano_btc --restart=unless-stopped --memory=1g -v /home/bot/bdata/config.json:/app/config.json bdata python -OO bdata.py --interval=60 --exchange=* --base=NANO --quote=BTC --snap_target=all
  docker run -itd --name=bdata_coinbasepro_btc_usd_book --restart=unless-stopped --memory=1g -v /home/bot/bdata/config.json:/app/config.json bdata python -OO bdata.py --interval=60 --exchange=coinbasepro --base=BTC --quote=USD --snap_target=book
  docker run -itd --name=bdata_binance_all_bnb --restart=unless-stopped --memory=1g -v /home/bot/bdata/config.json:/app/config.json bdata python -OO bdata.py --interval=30 --exchange=binance --base=* --quote=BNB --snap_target=trade
  docker run -itd --name=bdata_agent --restart=unless-stopped -v /home/bot/bdata/config.json:/app/config.json bdata python -OO bdata_stat.py
  ;;

start)
  for EXCHANGE in $EXCHANGES; do
    docker start "bdata_""$EXCHANGE""_all_btc"
  done
  docker start bdata_all_btc_usd
  docker start bdata_all_btc_eur
  docker start bdata_all_btc_usdt
  docker start bdata_all_nano_btc
  docker start bdata_coinbasepro_btc_usd_book
  docker start bdata_binance_all_bnb
  docker start bdata_agent
  ;;

stop)
  for EXCHANGE in $EXCHANGES; do
    docker stop "bdata_""$EXCHANGE""_all_btc"
  done
  docker stop bdata_all_btc_usd
  docker stop bdata_all_btc_eur
  docker stop bdata_all_btc_usdt
  docker stop bdata_all_nano_btc
  docker stop bdata_coinbasepro_btc_usd_book
  docker stop bdata_binance_all_bnb
  docker stop bdata_agent
  ;;

kill)
  for EXCHANGE in $EXCHANGES; do
    docker kill "bdata_""$EXCHANGE""_all_btc"
  done
  docker kill bdata_all_btc_usd
  docker kill bdata_all_btc_eur
  docker kill bdata_all_btc_usdt
  docker kill bdata_all_nano_btc
  docker kill bdata_coinbasepro_btc_usd_book
  docker kill bdata_binance_all_bnb
  docker kill bdata_agent
  ;;

rm)
  for EXCHANGE in $EXCHANGES; do
    docker rm "bdata_""$EXCHANGE""_all_btc"
  done
  docker rm bdata_all_btc_usd
  docker rm bdata_all_btc_eur
  docker rm bdata_all_btc_usdt
  docker rm bdata_all_nano_btc
  docker rm bdata_coinbasepro_btc_usd_book
  docker rm bdata_binance_all_bnb
  docker rm bdata_agent
  ;;

build)
  cd /home/bot/bdata || exit
  docker build -t bdata .
  ;;

esac
