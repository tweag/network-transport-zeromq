set title "Roundtrip (bytes/us)"
set terminal postscript color
set output "Throughput.ps"
set logscale y
plot "c-zmq-throughput.data" using 1:(10000*$1/$2) smooth bezier with lines title "c-zmq", \
     "c-tcp-throughput.data" using 1:(10000*$1/$2) smooth bezier with lines title "c-tcp"
     
