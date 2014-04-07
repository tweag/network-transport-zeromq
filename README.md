network-transport-zeromq
========================

[![Build Status][Build Status Image]][Build Status]

`network-transport-zmq` provides
a [network-transport][network-transport] implementation based on the
ØMQ brokerless message passing system.

Currently all tests from
[network-transport-tests][network-transport-tests] are passing. All
tests from [distributed-process-tests][distributed-process-tests] are
also passing, except monitoring and connection failure tests.

[Build Status Image]: https://secure.travis-ci.org/tweag/network-transport-zeromq.png?branch=master
[Build Status]: http://travis-ci.org/tweag/network-transport-zeromq
[network-transport]: http://hackage.haskell.org/package/network-transport
[network-transport-tests]: http://hackage.haskell.org/package/network-transport-tests
[distributed-process-tests]: https://github.com/haskell-distributed/distributed-process-tests

Features:
--------

    [+] Plain-text authorization
    [+] Reliable connections
    [+] Multicast
    [+] Manual connection break

### Plain text authorization

It's possible to add default user and password for all transport
connection by setting authorization ZMQParameters
```haskell
defaultZMQParameters {authorizationType=ZMQAuthPlain "user" "password"}
```

### Reliable connections

n-t-zmq uses Push-Pull pattern to implement reliable connections.

### Multicast protocol

n-t-zmq provide multicast group based on Pub Sub protocol, this
protocol is semi reliable, i.e. all messages will be delivered if they
will not reach High Water Mark event in presence of disconnects.

### Manual connection break

ØMQ automatically reconnects when connection is down and resend
messages so no message will be lost, however this may break some user
assumptions thus it's possible to mark.
