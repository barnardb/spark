/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.nomad

import com.hashicorp.nomad.apimodel.Job
import com.hashicorp.nomad.javasdk.NomadApiConfiguration
import com.hashicorp.nomad.javasdk.NomadApiConfiguration.nomadAddressAsHttpHost
import org.apache.http.HttpHost

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.nomad.ApplicationRunCommand
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.cluster.nomad.NomadClusterManagerConf._
import org.apache.spark.scheduler.cluster.nomad.SparkNomadJob.{CommonConf, SPARK_NOMAD_CLUSTER_MODE}

/**
 * Configuration for running a Spark application on a Nomad cluster.
 *
 * This contains all the configuration used by [[NomadClusterSchedulerBackend]],
 * other than configuration that is simply passed through to executors.
 *
 * By extracting all of the configuration as early as possible,
 * we can fail fast in the face of configuration errors,
 * rather than encountering them after we've already started interacting with Nomad.
 */
private[spark] case class NomadClusterManagerConf(
    jobConf: CommonConf,
    jobDescriptor: JobDescriptor,
    nomadApi: NomadApiConfiguration,
    staticExecutorInstances: Option[Int]
)

private[spark] object NomadClusterManagerConf {

  val AUTH_TOKEN =
    ConfigBuilder("spark.nomad.authToken")
      .doc("The secret ket of the Nomad auth token to use when accessing the API " +
        "(falls back to the NOMAD_TOKEN environment variable). " +
        "Note that if this configuration setting is set and the cluster deploy mode is used, " +
        "this setting will be propagated to the driver application in the Nomad job spec. " +
        "If it is not set and an auth token is taken from the NOMAD_TOKEN environment variable, " +
        "that token will not be propagated to the driver, which will have to pick up its token " +
        "from an environment variable.")
      .stringConf
      .createOptional

  val TLS_CA_CERT =
    ConfigBuilder("spark.nomad.tls.caCert")
      .doc("Path to a .pem file containing the certificate authority to validate the Nomad " +
        "server's TLS certificate against (falls back to the NOMAD_CA_CERT environment variable)")
      .stringConf
      .createOptional

  val TLS_CERT =
    ConfigBuilder("spark.nomad.tls.cert")
      .doc("Path to a .pem file containing the TLS certificate to present to the Nomad server " +
        "(falls back to the NOMAD_CLIENT_CERT environment variable)")
      .stringConf
      .createOptional

  val TLS_KEY =
    ConfigBuilder("spark.nomad.tls.trustStorePassword")
      .doc("Path to a .pem file containing the private key corresponding to the certificate in " +
        TLS_CERT.key + " (falls back to the NOMAD_CLIENT_KEY environment variable)")
      .stringConf
      .createOptional

  val DEFAULT_EXECUTOR_INSTANCES = 2

  sealed trait JobDescriptor {
    def region: Option[String]
    def namespace: Option[String]
  }

  case class ExistingJob(
      allocId: String,
      override val region: Option[String],
      override val namespace: Option[String]
  ) extends JobDescriptor

  case class NewJob(job: Job) extends JobDescriptor {
    override def region: Option[String] = Option(job.getRegion)
    override def namespace: Option[String] = Option(job.getNamespace)
  }

  case class KeyPair(certFile: String, keyFile: String)

  object KeyPair {
    def apply(conf: SparkConf,
        certEntry: ConfigEntry[Option[String]],
        keyEntry: ConfigEntry[Option[String]]): Option[KeyPair] =
      (conf.get(certEntry), conf.get(keyEntry)) match {
        case (Some(cert), Some(file)) => Some(KeyPair(cert, file))
        case (None, None) => None
        case _ => throw new SparkException(
          s"You can either provide both ${certEntry.key} and ${keyEntry.key}, " +
            "or neither of them, but you can't provide one an not the other."
        )
      }
  }

  def extractNomadUrl(conf: SparkConf): Option[HttpHost] = (conf.get("spark.master") match {
    case "nomad" => sys.env.get("NOMAD_ADDR").map(_.trim).filter(_.nonEmpty)
    case master if master.startsWith("nomad:") => Some(master.stripPrefix("nomad:"))
    case invalid => throw new SparkException(
      "Nomad master can either be \"nomad:\" followed by an explicit HTTP or HTTPS URL " +
        "(e.g. nomad:http://nomad.example.com), or simply \"nomad\" which signals that " +
        "the NOMAD_ADDR environment variable should be used if set and falls back to  " +
        "http://127.0.0.1:4646"
    )
  }).map(nomadAddressAsHttpHost)

  def extractApiConf(
      nomadUrl: Option[HttpHost],
      authToken: Option[String],
      region: Option[String],
      namespace: Option[String],
      conf: SparkConf
  ): NomadApiConfiguration = {
    val builder = new NomadApiConfiguration.Builder()

    builder.setFromEnvironmentVariables(System.getenv())

    nomadUrl.foreach(address => builder.setAddress(address))

    authToken.foreach(builder.setAuthToken)

    conf.get(TLS_CA_CERT).foreach(builder.setTlsCaFile)
    KeyPair.apply(conf, TLS_CERT, TLS_KEY)
      .foreach(p => builder.setTlsCertAndKeyFiles(p.certFile, p.keyFile))

    region.foreach(builder.setRegion)
    namespace.foreach(builder.setNamespace)

    builder.build()
  }

  def apply(
      conf: SparkConf,
      command: Option[ApplicationRunCommand]
  ): NomadClusterManagerConf = {

    val nomadUrl = extractNomadUrl(conf)

    val authToken = conf.get(AUTH_TOKEN)

    val jobConf = SparkNomadJob.CommonConf(conf)

    val jobDescriptor =
      if (conf.getBoolean(SPARK_NOMAD_CLUSTER_MODE, defaultValue = false)) {
        ExistingJob(
          allocId = Option(conf.getenv("NOMAD_ALLOC_ID")).getOrElse(throw new RuntimeException(
            s"$SPARK_NOMAD_CLUSTER_MODE is set, but the NOMAD_ALLOC_ID environment variable is not."
          )),
          region = conf.get(SparkNomadJob.REGION),
          namespace = conf.get(SparkNomadJob.NAMESPACE)
        )
      } else {
        NewJob(SparkNomadJob(conf, jobConf, nomadUrl, command))
    }

    NomadClusterManagerConf(
      jobConf = jobConf,
      jobDescriptor = jobDescriptor,
      nomadApi = extractApiConf(
        nomadUrl = nomadUrl,
        authToken = authToken,
        region = jobDescriptor.region,
        namespace = jobDescriptor.namespace,
        conf = conf
      ),
      staticExecutorInstances = conf.get(EXECUTOR_INSTANCES)
    )
  }

}
