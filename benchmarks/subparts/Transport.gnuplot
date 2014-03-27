set title "Roundtrip (us)"
set yrange [0:250]
set terminal postscript color
set output "Transport.ps"
plot "n-t-zmq.data" smooth bezier with lines title "NT ZMQ", \
     "n-t-tcp.data" smooth bezier with lines title "NT TCP", \
     "h-tcp.data" smooth bezier with lines title "Haskell TCP", \
     "c-tcp.data" smooth bezier with lines title "c-tcp", \
     "c-zmq-rr.data" smooth bezier with lines title "c-zmq-rr", \
     "c-zmq.data" smooth bezier with lines title "c-zmq"
     
