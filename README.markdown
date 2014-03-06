Network-transprt 0mq
====================

network-transport-zmq provides a network transport implementation based on
0mq brokerless message passing system. It allowes to have a full featured
sequre and fast network-transport implementation.

Currently all network-transport tests are passing, however distributed-process-tests
that use monitoring and connection break doesn't.

Features:

    [+] Plain-text authorization
    [+] Reliable connections

Not yet implemented features:

    [ ] Certificate authorization
    [ ] Unreliable connections. (A branch exists but is not merged into mainline)
    [ ] Heartbeating protocol (catching of network failures).
