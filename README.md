![mpr_logo](https://github.com/GMAP/MPR/assets/40276058/858d8734-21fa-462f-8ea7-a849c4392730)

# MPR: An MPI Framework for Distributed Self-Adaptive Stream Processing

MPR (Message Passing Runtime) is a framework and runtime system for implementing and assessing self-adaptive algorithms and optimizations. MPR is implemented in C++ on top of MPI's dynamic process management targeting high performance, low latency, and flexibility.

## Introduction

Adaptability is crucial in distributed stream processing systems for performance and efficiency. MPR facilitates this adaptability by allowing applications to scale horizontally: adding or removing processes dynamically based on real-time monitored performance metrics.

## How It Works

MPR operates by monitoring a JSON file that specifies the configuration of the running processes. When this file is updated, MPR adjusts the number of processes accordingly, ensuring that the application adapts to changing workloads and conditions. Here’s a breakdown of the process:

1. **Monitoring**: The runtime system continuously monitors the `parameters.json` file for changes. This file contains the configuration details that determine how many processes should be running per stage at any given time.
2. **Self-Adaptation**: When an update is detected in the `parameters.json` file, MPR automatically adjusts the number of processes. This involves either adding new processes or removing existing ones to match the updated configuration.
3. **Statistics Gathering**: During execution, MPR collects statistics for each pipeline stage on the number of items consumed and produced. The gathered data is stored in the `config` folder and provides an overview of the system’s execution.
4. **Decision Making**: Self-adaptive algorithms analyze the collected statistics to make informed decisions about further adaptations. Based on this analysis, the algorithms update the parameters in the `parameters.json` file. This update triggers MPR to self-adapt, creating a continuous loop of monitoring, adaptation, and decision-making.

![mpr_framework](https://github.com/GMAP/MPR/assets/40276058/143355ea-f159-469f-a859-4671b6023ce6)

## Getting Started

### Prerequisites

OpenMPI v4.1 or above - Documentation [here](https://docs.open-mpi.org/en/v5.0.x/installing-open-mpi/quickstart.html).

*Note: Our experiments were conducted using OpenMPI.*

### Setup

MPR is a header-only C++ library. Examples are available [here](examples).

**Example Applications**
- **Prime numbers**: Simply counts the number of primes from 0 up to the specified number.
- **Mandelbrot Set**: Generates an image with the Mandelbrot Set.

**Compiling**:
```shell
$ cd examples
$ make prime_mpr
$ make mandel_mpr
```

**Executing**:
```shell
$ mpirun --np 1 prime_mpr 100000
$ mpirun --np 1 mandel_mpr 1000
```

*Note:
If OpenMPI complains (depending on how it was built), please include the hostfile ([hostfile.txt](examples/hostfile.txt)) pointing to your **localhost**.*
`
$ mpirun --hostfile hostfile.txt --map-by slot --bind-to core --np 1 prime_mpr 100000
`

**Self-adaptation**:

To observe MPR's self-adaptation capabilities:

1. Open another terminal and monitor CPU usage, e.g., using `htop` on Linux.

2. Run the prime number calculation with a larger input, e.g., 1.2 million numbers:
```shell
$ mpirun --np 1 prime_mpr 1200000
```
3. While the application is running, modify the number of active processes in the `stage2` section of `parameters.json`.

    **Note**: Currently, MPR supports adapting only the number of computational processes (`stage2`), not the source and sink processes.

4. Observe `htop` to see MPR automatically adjusting the number of computational processes (set to every 1 sec) without dropping any data messages.

5. Statistics are generated in the `config` folder. The `stats_stage2.json` file contains statistics for each running process, including the number of running processes at a given timestamp.

6. Run the plot script to visualize the collected data. This will generate the figure `adaptability_mpr.png` in the same folder. *Note: Ensure you have installed the necessary Python dependencies.*
```shell
$ cd ../config
$ python3 plot_stats.py
```

# How to cite

To be included.

# Limitations

MPR is currently implemented as a prototype to demonstrate the functionality of the proposed framework in our paper. This means that many corner cases are not yet implemented.

- The available API supports implementing only streaming pipelines. Although data-flows and complex DAGs are supported by the runtime system, there is no MPR API for implementing such streaming applications.

- MPR supports three-stage pipelines (Source, Compute, and Sink). Adaptation is allowed only on the computational processes. Due to the complexity of our experimental evaluation, we limited the prototype implementation to studying distributed stream processing adaptability in a well-defined scenario.

- We noticed undefined behaviours when using MPI's dynamic process management, e.g., `MPI_Comm_spawn()`. Executions may print the following message after the application has finished and the result has been successfully generated: `mpirun noticed that process rank 0 with PID 0 on node localhost exited on signal 13 (Broken pipe)`. Likewise, some dynamically spawned MPI processes may get blocked in `MPI_Finalize()`, requiring manual termination.



