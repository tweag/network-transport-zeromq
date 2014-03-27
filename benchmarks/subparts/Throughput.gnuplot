set title "Roundtrip (Mb/s)"
set terminal postscript color
set output "Throughput.ps"
plot "c-zmq-throughput.data" using 1:4 smooth bezier with lines title "c-zmq", \
     "c-zmq-throughput-2.data" using 1:4 smooth bezier with lines title "c-zmq prepared", \
     "c-tcp-throughput.data" using 1:4 smooth bezier with lines title "c-tcp"

#     "c-zmq-throughput-rr.data" smooth bezier with lines title "c-zmq-rr" \
#set logscale y
#plot "c-zmq-throughput.data" using 1:(10000*$1/$2) smooth bezier with lines title "c-zmq", \
#     "c-zmq-throughput-rr.data" using 1:(10000*$1/$2) smooth bezier with lines title "c-zmq-rr", \
#     "c-tcp-throughput.data" using 1:(10000*$1/$2) smooth bezier with lines title "c-tcp"
     
