/*
 * Copyright 2017-2020 47 Degrees Open Source <https://www.47deg.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package higherkindness.mu.rpc.prometheus

import cats.effect.IO
import cats.implicits._
import cats.effect.unsafe.implicits._
import higherkindness.mu.rpc.internal.interceptors.GrpcMethodInfo
import higherkindness.mu.rpc.internal.metrics.MetricsOpsGenerators._
import io.grpc.Status
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.{Collector, CollectorRegistry}
import org.scalacheck.Prop.forAllNoShrink
import org.scalacheck.{Gen, Properties}

import scala.jdk.CollectionConverters._

class PrometheusMetricsTests extends Properties("PrometheusMetrics") {

  val prefix     = "testPrefix"
  val classifier = "classifier"

  def performAndCheckMetrics(
      numberOfCalls: Int,
      registry: CollectorRegistry,
      metricName: String,
      op: IO[Unit]
  )(checkSamples: List[MetricFamilySamples.Sample] => Boolean): IO[Boolean] =
    (1 to numberOfCalls).toList
      .map(_ => op)
      .sequence_
      .map(_ => checkMetrics(registry, metricName)(checkSamples))

  def findRecordedMetric(
      metricName: String,
      registry: CollectorRegistry
  ): Option[Collector.MetricFamilySamples] =
    registry.metricFamilySamples.asScala.find(_.name == metricName)

  def findRecordedMetricOrThrow(
      metricName: String,
      registry: CollectorRegistry
  ): Collector.MetricFamilySamples =
    findRecordedMetric(metricName, registry).getOrElse(
      throw new IllegalArgumentException(s"Could not find metric with name: $metricName")
    )

  def checkMetrics(
      registry: CollectorRegistry,
      metricName: String
  )(checkSamples: List[MetricFamilySamples.Sample] => Boolean): Boolean =
    checkSamples(findRecordedMetricOrThrow(metricName, registry).samples.asScala.toList)

  def checkSingleSamples(metricName: String, value: Double)(
      samples: List[MetricFamilySamples.Sample]
  ): Boolean =
    samples.find(_.name == metricName).exists(_.value == value)

  // Prometheus can return the value with a difference of 0.0000000001
  def compareWithMinError(v1: Double, v2: Double): Boolean =
    Math.abs(v1 - v2) < Math.pow(10, -10)

  def checkSeriesSamples(metricName: String, numberOfCalls: Int, elapsed: Int)(
      samples: List[MetricFamilySamples.Sample]
  ): Boolean = {
    samples.find(_.name == metricName + "_count").exists(_.value == numberOfCalls.toDouble) &&
    samples
      .find(_.name == metricName + "_sum")
      .map(_.value * Collector.NANOSECONDS_PER_SECOND)
      .exists(compareWithMinError(_, (numberOfCalls * elapsed).toDouble))
  }

  // Prometheus creates two metrics per each Counter
  // See https://github.com/prometheus/client_java/pull/615
  def checkCounterSamples(metricName: String, value: Double)(
      samples: List[MetricFamilySamples.Sample]
  ): Boolean =
    samples.find(_.name == metricName + "_total").exists(_.value == value) &&
      samples.exists(_.name == metricName + "_created")

  property("creates and updates counter when registering an active call") =
    forAllNoShrink(methodInfoGen, Gen.chooseNum[Int](1, 10)) {
      (methodInfo: GrpcMethodInfo, numberOfCalls: Int) =>
        val registry   = new CollectorRegistry()
        val metricName = s"${prefix}_active_calls"

        (for {
          metrics <- PrometheusMetrics.build[IO](registry, prefix)
          op1 <- performAndCheckMetrics(
            numberOfCalls,
            registry,
            metricName,
            metrics.increaseActiveCalls(methodInfo, Some(classifier))
          )(checkSingleSamples(metricName, numberOfCalls.toDouble))
          op2 <- performAndCheckMetrics(
            numberOfCalls,
            registry,
            metricName,
            metrics.decreaseActiveCalls(methodInfo, Some(classifier))
          )(checkSingleSamples(metricName, 0L))
        } yield op1 && op2).unsafeRunSync()
    }

  property("creates and updates counter when registering a received message") =
    forAllNoShrink(methodInfoGen, Gen.chooseNum[Int](1, 10)) {
      (methodInfo: GrpcMethodInfo, numberOfCalls: Int) =>
        val registry   = new CollectorRegistry()
        val metricName = s"${prefix}_messages_received"

        (for {
          metrics <- PrometheusMetrics.build[IO](registry, prefix)
          op1 <- performAndCheckMetrics(
            numberOfCalls,
            registry,
            metricName,
            metrics.recordMessageReceived(methodInfo, Some(classifier))
          )(checkCounterSamples(metricName, numberOfCalls.toDouble))
        } yield op1).unsafeRunSync()
    }

  property("creates and updates timer for headers time") =
    forAllNoShrink(methodInfoGen, Gen.chooseNum[Int](1, 10), Gen.chooseNum(100, 1000)) {
      (methodInfo: GrpcMethodInfo, numberOfCalls: Int, elapsed: Int) =>
        val registry   = new CollectorRegistry()
        val metricName = s"${prefix}_calls_header"

        (for {
          metrics <- PrometheusMetrics.build[IO](registry, prefix)
          op1 <- performAndCheckMetrics(
            numberOfCalls,
            registry,
            metricName,
            metrics.recordHeadersTime(methodInfo, elapsed.toLong, Some(classifier))
          )(checkSeriesSamples(metricName, numberOfCalls, elapsed))
        } yield op1).unsafeRunSync()
    }

  property("creates and updates timer for total time metrics") =
    forAllNoShrink(methodInfoGen, Gen.chooseNum[Int](1, 10), statusGen, Gen.chooseNum(100, 1000)) {
      (methodInfo: GrpcMethodInfo, numberOfCalls: Int, status: Status, elapsed: Int) =>
        val registry   = new CollectorRegistry()
        val metricName = s"${prefix}_calls_total"

        (for {
          metrics <- PrometheusMetrics.build[IO](registry, prefix)
          op1 <-
            (1 to numberOfCalls).toList
              .traverse_(_ =>
                metrics.recordTotalTime(methodInfo, status, elapsed.toLong, Some(classifier))
              )
              .map { _ =>
                checkMetrics(
                  registry,
                  metricName
                )(checkSeriesSamples(metricName, numberOfCalls, elapsed))
              }
        } yield op1).unsafeRunSync()
    }

  property("creates and updates timer for full total time metrics") =
    forAllNoShrink(methodInfoGen, Gen.chooseNum[Int](1, 10), statusGen, Gen.chooseNum(100, 1000)) {
      (methodInfo: GrpcMethodInfo, numberOfCalls: Int, status: Status, elapsed: Int) =>
        val registry   = new CollectorRegistry()
        val metricName = s"${prefix}_calls_total"

        (for {
          metrics <- PrometheusMetrics.buildFullTotal[IO](registry, prefix)
          op1 <-
            (1 to numberOfCalls).toList
              .traverse_(_ =>
                metrics.recordTotalTime(methodInfo, status, elapsed.toLong, Some(classifier))
              )
              .map { _ =>
                checkMetrics(
                  registry,
                  metricName
                )(checkSeriesSamples(metricName, numberOfCalls, elapsed))
              }
        } yield op1).unsafeRunSync()
    }
}
