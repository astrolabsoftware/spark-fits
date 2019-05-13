---
permalink: /docs/batch/
layout: splash
title: "Tutorial: Include spark-fits in your application"
date: 2018-06-15 22:31:13 +0200
---

# Tutorial: Include spark-fits in your application

We provide example application which uses spark-fits: local and cluster modes.
In addition, we provide scripts to run spark-fits applications at NERSC, California.

## Local launch

See the two shell scripts at the root of the package

```bash
./run_scala-2.1X.sh  # Scala (bintable HDU)
./run_python.sh # Python (bintable HDU)
./run_image python.sh # Python (image HDU)
```

Just make sure that you set up correctly the paths and the different
variables.

## Cluster mode

See the two shell scripts at the root of the package

```bash
./run_scala_cluster.sh  # Scala
./run_python_cluster.sh # Python
```

Just make sure that you set up correctly the paths and the different
variables.

## Using at NERSC

Although HPC systems are not designed for IO intensive jobs, Spark
standalone mode and filesystem-agnostic approach makes it also a
candidate to process data stored in HPC-style shared file systems such
as Lustre. Two scripts are provided at the root of the project to launch
a Spark Job on Cori at NERSC: :

```bash
sbatch run_scala_cori.sh  # Scala
sbatch run_python_cori.sh # Python
```

Keep in mind that raw performances (i.e. without any attempt to take
into account that we read from Lustre and not from HDFS for example) can
be worst than in HTC systems.
