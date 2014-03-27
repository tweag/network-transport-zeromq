#!/bin/sh
sleep 2
ROOT=~/tmp/zeromq-4.0.4/perf
OUT=c-zmq-throughput-2.data
echo > $OUT
N=1000000
for i in 1 20 10 30 40 64 79 85 100 200 256 528 600 728 1000 1024 1360 2000 10000 20000 40000 80000 100000; do
  echo -n "$i 0 0 " >> $OUT
  echo "${ROOT}/remote_thr tcp://127.0.0.1:8080 $i $N &"
  ${ROOT}/remote_thr tcp://127.0.0.1:8080 $i $N &
  echo "${ROOT}/local_thr  tcp://127.0.0.1:8080 $i $N | tail -n 1 | awk '{ print $3 }' >> $OUT"
  ${ROOT}/local_thr  tcp://127.0.0.1:8080 $i $N | tail -n 1 | awk '{ print $3 }' >> $OUT
done
