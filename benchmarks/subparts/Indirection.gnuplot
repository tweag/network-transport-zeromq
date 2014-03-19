set title "Roundtrip (us)"
set yrange [0:350]
set terminal postscript color
set output "Indirection.ps"
plot "h-tcp.data" smooth bezier with lines title "haskell-tcp-no-header", \
     "h-tcp-chan.data" smooth bezier with lines title "haskell-tcp-chan", \
     "h-zmq-chan.data" smooth bezier with lines title "haskell-zmq-chan", \
     "h-zmq-tmchan.data" smooth bezier with lines title "haskell-zmq-tmchan", \
     "h-zmq-threads.data" smooth bezier with lines title "haskell-zmq-two-threads", \
     "n-t-zmq.data" smooth bezier with lines title "n-t-zmq"
