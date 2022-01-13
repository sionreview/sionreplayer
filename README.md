# SionReplayer

SionReplayer is the workload replayer for SION project.

## Simulation

A sample of IBM docker registry trace is included. To run the simulation using sample trace:

~~~
go get
make simulate
~~~

## Replay

To replay trace, run following command:

~~~
go get
make build
bin/playback [trace file]
~~~