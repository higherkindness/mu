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

package higherkindness.mu.rpc.dropwizard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.codahale.metrics.MetricRegistry
import higherkindness.mu.rpc.dropwizard.DropWizardMetrics._
import higherkindness.mu.rpc.internal.interceptors.GrpcMethodInfo
import higherkindness.mu.rpc.internal.metrics.MetricsOps
import higherkindness.mu.rpc.internal.metrics.MetricsOps._
import higherkindness.mu.rpc.internal.metrics.MetricsOpsGenerators._
import io.grpc.Status
import org.scalacheck.{Gen, Properties}
import org.scalacheck.Prop._

import scala.jdk.CollectionConverters._

object DropwizardMetricsTests extends Properties("DropWizardMetrics") {

  val prefix     = "testPrefix"
  val classifier = "classifier"

  def performAndCheckMetrics(
      numberOfCalls: Int,
      expectedCount: Int,
      registry: MetricRegistry,
      gaugeName: String,
      gaugeType: GaugeType,
      op: IO[Unit]
  ): IO[Boolean] =
    (1 to numberOfCalls).toList
      .map(_ => op)
      .sequence_
      .map(_ => checkMetrics(expectedCount, registry, gaugeName, gaugeType))

  def checkMetrics(
      expectedCount: Int,
      registry: MetricRegistry,
      gaugeName: String,
      gaugeType: GaugeType
  ): Boolean = {
    val (gaugeMap, gaugeCount) =
      gaugeType match {
        case Timer =>
          (registry.getTimers().asScala.toMap, registry.timer(gaugeName).getCount)
        case Counter =>
          (registry.getCounters().asScala.toMap, registry.counter(gaugeName).getCount)
        case _ =>
          (Map.empty[String, GaugeType], 0)
      }
    gaugeMap.contains(gaugeName) && gaugeCount == expectedCount
  }

  property("creates and updates counter when registering an active call") =
    forAllNoShrink(methodInfoGen, Gen.chooseNum[Int](1, 10)) {
      (methodInfo: GrpcMethodInfo, numberOfCalls: Int) =>
        val registry        = new MetricRegistry()
        val metrics         = DropWizardMetrics[IO](registry, prefix)
        val activeCallsName = s"$prefix.$classifier.active.calls"

        (for {
          op1 <- performAndCheckMetrics(
            numberOfCalls,
            numberOfCalls,
            registry,
            activeCallsName,
            Counter,
            metrics.increaseActiveCalls(methodInfo, Some(classifier))
          )
          op2 <- performAndCheckMetrics(
            numberOfCalls,
            expectedCount = 0,
            registry,
            activeCallsName,
            Counter,
            metrics.decreaseActiveCalls(methodInfo, Some(classifier))
          )
        } yield op1 && op2).unsafeRunSync()
    }

  property("creates and updates counter when registering a sent message") =
    forAllNoShrink(methodInfoGen, Gen.chooseNum[Int](1, 10)) {
      (methodInfo: GrpcMethodInfo, numberOfCalls: Int) =>
        val registry = new MetricRegistry()
        val metrics  = DropWizardMetrics[IO](registry, prefix)
        val messagesSentName =
          s"$prefix.$classifier.${methodInfo.serviceName}.${methodInfo.methodName}.messages.sent"

        performAndCheckMetrics(
          numberOfCalls,
          numberOfCalls,
          registry,
          messagesSentName,
          Counter,
          metrics.recordMessageSent(methodInfo, Some(classifier))
        ).unsafeRunSync()
    }

  property("creates and updates counter when registering a received message") =
    forAllNoShrink(methodInfoGen, Gen.chooseNum[Int](1, 10)) {
      (methodInfo: GrpcMethodInfo, numberOfCalls: Int) =>
        val registry = new MetricRegistry()
        val metrics  = DropWizardMetrics[IO](registry, prefix)
        val messagesReceivedName =
          s"$prefix.$classifier.${methodInfo.serviceName}.${methodInfo.methodName}.messages.received"

        performAndCheckMetrics(
          numberOfCalls,
          numberOfCalls,
          registry,
          messagesReceivedName,
          Counter,
          metrics.recordMessageReceived(methodInfo, Some(classifier))
        ).unsafeRunSync()
    }

  property("creates and updates timer for headers time") =
    forAllNoShrink(methodInfoGen, Gen.chooseNum[Int](1, 10), Gen.chooseNum(100, 1000)) {
      (methodInfo: GrpcMethodInfo, numberOfCalls: Int, elapsed: Int) =>
        val registry    = new MetricRegistry()
        val metrics     = DropWizardMetrics[IO](registry, prefix)
        val headersName = s"$prefix.$classifier.calls.header"

        performAndCheckMetrics(
          numberOfCalls,
          numberOfCalls,
          registry,
          headersName,
          Timer,
          metrics.recordHeadersTime(methodInfo, elapsed.toLong, Some(classifier))
        ).unsafeRunSync()
    }

  property("creates and updates timer for total time") =
    forAllNoShrink(methodInfoGen, Gen.chooseNum[Int](1, 10), statusGen) {
      (methodInfo: GrpcMethodInfo, numberOfCalls: Int, status: Status) =>
        val registry = new MetricRegistry()
        val metrics  = DropWizardMetrics[IO](registry, prefix)

        (1 to numberOfCalls).toList
          .map(_ => metrics.recordTotalTime(methodInfo, status, 1L, Some(classifier)))
          .sequence_
          .map { _ =>
            checkMetrics(numberOfCalls, registry, s"$prefix.$classifier.calls.total", Timer) &&
            checkMetrics(
              numberOfCalls,
              registry,
              s"$prefix.$classifier.${methodTypeDescription(methodInfo)}",
              Timer
            ) &&
            checkMetrics(
              numberOfCalls,
              registry,
              s"$prefix.$classifier.${statusDescription(MetricsOps.grpcStatusFromRawStatus(status))}",
              Timer
            )
          }
          .unsafeRunSync()
    }

}
