# The "Just Ping" tests measure ping latency with as little overhead as possible

NUMPINGS=100000
ROOT=../../..
INCLUDES=-i${ROOT}/network-transport/src -i${ROOT}/network-transport-zeromq/src

# Enable for profiling
# PROF_GHC=-prof -fprof-auto
# PROF_EXE=+RTS -pa -RTS

GCC=gcc 
GHC=ghc -rtsopts -XRankNTypes -XScopedTypeVariables -XDeriveDataTypeable -XCPP -XGeneralizedNewtypeDeriving -XLambdaCase -optP-include -optPcabal_macros.h -hide-package stateref

all: Transport.ps Indirection.ps

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

c-tcp.exe: c-tcp.c
	$(GCC) -O2 -o c-tcp.exe c-tcp.c

c-zmq-rr.exe: c-zmq-rr.c
	$(GCC) -O2 -o c-zmq-rr.exe c-zmq-rr.c -lzmq

JustPingHaskellNT.exe: JustPingHaskell.hs
	$(GHC) -O2 $(PROF_GHC) -o JustPingHaskellNT.exe JustPingHaskell.hs

%.data : %.exe
	time ./$< $(NUMPINGS) $(PROF_EXE) 2>$@

%.exe :: %.hs
	$(GHC) -O2 $(PROF_GHC) -threaded -o $@ --make $< $(INCLUDES)

.PHONY: clean
clean:
	rm -f *.data *.ps *.pdf *.o *.hi *.exe

# vi:set noexpandtab:
