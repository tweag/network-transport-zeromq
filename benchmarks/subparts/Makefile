# The "Just Ping" tests measure ping latency with as little overhead as possible

NUMPINGS=100000
ROOT=../../..
INCLUDES=-i${ROOT}/network-transport/src -i${ROOT}/network-transport-zeromq/src

# Enable for profiling
# PROF_GHC=-prof -fprof-auto
# PROF_EXE=+RTS -pa -RTS

GCC=gcc 
GHC=ghc -rtsopts -XRankNTypes -XScopedTypeVariables -XDeriveDataTypeable -XCPP -XGeneralizedNewtypeDeriving -XLambdaCase -optP-include -optPcabal_macros.h -hide-package stateref

all: Transport.ps Indirection.ps Throughput.ps

Throughput.ps: c-zmq-throughput.data \
	  		   c-tcp-throughput.data \
			   Throughput.gnuplot
	gnuplot ./Throughput.gnuplot

Transport.ps: h-tcp.data \
              n-t-zmq.data \
              c-tcp.data \
              c-zmq.data \
			  c-zmq-rr.data \
              Transport.gnuplot
	gnuplot ./Transport.gnuplot

Indirection.ps: c-zmq.data \
                h-tcp-chan.data \
                h-zmq.data \
                h-zmq-chan.data \
                h-zmq-tmchan.data \
                h-zmq-threads.data \
                n-t-zmq.data \
                Indirection.gnuplot
	gnuplot ./Indirection.gnuplot

c-zmq.exe: c-zmq.c
	$(GCC) -O2 -o c-zmq.exe c-zmq.c -lzmq

c-zmq-throughput.exe: c-zmq-throughput.c
	$(GCC) -O2 -o c-zmq-throughput.exe c-zmq-throughput.c -lzmq

c-zmq-rr.exe: c-zmq-rr.c
	$(GCC) -O2 -o c-zmq-rr.exe c-zmq-rr.c -lzmq

c-tcp.exe: c-tcp.c
	$(GCC) -O2 -o c-tcp.exe c-tcp.c

c-tcp-throughput.exe: c-tcp-throughput.c
	$(GCC) -O2 -o c-tcp-throughput.exe c-tcp-throughput.c

JustPingHaskellNT.exe: JustPingHaskell.hs
	$(GHC) -O2 $(PROF_GHC) -o JustPingHaskellNT.exe JustPingHaskell.hs

c-tcp-throughput.data: c-tcp-throughput.exe\
					   prepare-data.sh
	./prepare-data.sh $<

c-zmq-throughput.data: c-zmq-throughput.exe\
					   prepare-data.sh
	./prepare-data.sh $<

%.data : %.exe
	time ./$< $(NUMPINGS) $(PROF_EXE) 2>$@

%.exe :: %.hs
	$(GHC) -O2 $(PROF_GHC) -threaded -o $@ --make $< $(INCLUDES)

.PHONY: clean
clean:
	rm -f *.data *.ps *.pdf *.o *.hi *.exe

# vi:set noexpandtab:
