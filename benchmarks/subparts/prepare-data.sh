#!/bin/sh
rm ${1/exe/data}
for i in 1 10 100 528 600 1000 1024 1360 2000 10000 ; do
  echo "time ./$1 10000 $i 2>>${1/exe/data}"
  time ./$1 10000 $i 2>>${1/exe/data}
done
