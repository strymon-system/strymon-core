An Online Stream Processor for Timely Dataflow
==============================================

This repository contains the source code of a system for deploying Timely
Dataflow application.

## Requirements

A recent nightly of the Rust compiler and Cargo are required to build this
project, as the source makes of the type name instrinsic.

## Getting started

To spawn a simple example query on the local machine, run the following
commands in the root directory of this repository:

    # build the whole system and the example queries
    cargo build --release
    (cd examples ; cargo build --release)

    # not strictly necessary, but simplifies things
    export PATH=$PATH:$(readlink -f ./target/release)
    export TIMELY_SYSTEM_HOSTNAME=$(hostname)

    # start the coordinator on the default port (9189) and one executor
    coordinator &
    RUST_LOG=info executor -c localhost:9189 &

    # check that the system is running
    status

    # spawn an example query
    submit -p ./target/release/hello

See `submit -h` for more information about query submission.

## Networking considerations

The coordinator, the executor and the submission program all need to know the
external hostname of the machine. It can be either set using the `-e` option,
or by specifying the `TIMELY_SYSTEM_HOSTNAME` environment variable. If you
encounter connectivity problems, please make sure that the hostname specified
in `$TIMELY_SYSTEM_HOSTNAME` is reachable from any machine participating in
the cluster.

If you run more than one executor on the same machine, make please make sure
that their ports will not overlap (using the `-p` option).

## Code Structure

### `./src`

This folder contains the system library, `timely_system`. It contains all shared
functionality of all the components.
                
### `./src/bin`
    
System binaries, calling and initializing the `timely_system` for the
different components:

  - `coordinator` The coordinator program, to be started first.
  - `executor` Process to spawn queries, to be launch on each machine.
  - `submit` The submission client to spawn new queries.
  - `status` Connects to the coordinator and dumps the current system state.

### `./lib`

Contains the query library, `timely_query`. Timely programs which are
supposed to participate in the system are supposed to link against this library
and call `timely_query::execute`. Implementation-wise just minimal facade of the
`timely_system` library.
 
### `./examples`

Example project containing some simple queries, all queries link against
`timely_query`.

### `./evaluation/sessionize`

Queries used for the evaluation section of the thesis. Uses code of the
`trace_analyzer` repository. See README for building instructions.

### `./evaluation/demo`

Some additional example queries, used during at the thesis presentation.
