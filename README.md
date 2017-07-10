# Apache Spark on Nomad

This repository is a fork of Apache Spark that natively supports using HashiCorp's 
[Nomad](https://www.nomadproject.io/intro/) as Spark's cluster manager  (as an 
alternative to Hadoop YARN and Mesos). When running on Nomad, the Spark executors
 that run tasks for your Spark application, and optionally the application 
 driver itself, run as Nomad tasks in a Nomad job.

Sample `spark-submit` command when using Nomad:

```bash
spark-submit \
  --class org.apache.spark.examples.JavaSparkPi \
  --master nomad \
  --deploy-mode cluster \
  --conf spark.executor.instances=4 \
  --conf spark.nomad.sparkDistribution=https://s3.amazonaws.com/nomad-spark/spark-2.1.0-bin-nomad.tgz \
  https://s3.amazonaws.com/nomad-spark/spark-examples_2.11-2.1.0-SNAPSHOT.jar 100
 ```

## Benefits of Spark on Nomad

Nomad's design is heavily inspired by Google's work on both 
[Borg](https://research.google.com/pubs/pub43438.html) and 
[Omega](https://research.google.com/pubs/pub41684.html). This has enabled a set 
of features that make Nomad well-suited to run analytical applications. 
Particularly relevant are its native support for 
[batch workloads](https://www.nomadproject.io/docs/runtime/schedulers.html#batch)
 and parallelized, [high throughput scheduling](https://www.hashicorp.com/c1m/) 
 (more on scheduler internals [here](https://www.nomadproject.io/docs/internals/scheduling.html)).

Nomad is easy to set up and use. It consists of a single binary/process, has a 
simple and intuitive data model, utilizes a 
[declarative job specification](https://www.nomadproject.io/docs/job-specification/index.html)
 and supports high availability and 
[multi-datacenter federation](https://www.nomadproject.io/guides/cluster/federation.html)
 out-of-the-box. Nomad also integrates seamlessly with HashiCorp's other runtime
 tools: [Consul](https://www.nomadproject.io/docs/service-discovery/index.html) 
and [Vault](https://www.nomadproject.io/docs/vault-integration/index.html).

## Getting Started

To get started, see Nomad's official 
[Apache Spark Integration Guide](https://www.nomadproject.io/guides/spark/spark.html).
 You can also use Nomad's [example Terraform configuration](https://github.com/hashicorp/nomad/terraform) 
and embedded Spark walkthrough to give the integration a quick test drive on AWS. 
Builds are currently available for Spark [2.1.0](https://s3.amazonaws.com/nomad-spark/spark-2.1.0-bin-nomad.tgz) 
and 2.1.1](https://s3.amazonaws.com/nomad-spark/spark-2.1.1-bin-nomad.tgz).
