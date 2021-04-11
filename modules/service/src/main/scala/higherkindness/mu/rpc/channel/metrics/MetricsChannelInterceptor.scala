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

package higherkindness.mu.rpc
package channel.metrics

import cats.effect.kernel.{Async, Clock, Resource, Sync}
import cats.effect.std.Dispatcher
import cats.implicits._
import higherkindness.mu.rpc.internal.interceptors.GrpcMethodInfo
import higherkindness.mu.rpc.internal.metrics.MetricsOps
import io.grpc._

private class MetricsChannelInterceptor[F[_]: Sync: Clock](
    metricsOps: MetricsOps[F],
    classifier: Option[String] = None,
    dispatcher: Dispatcher[F]
) extends ClientInterceptor {

  override def interceptCall[Req, Res](
      methodDescriptor: MethodDescriptor[Req, Res],
      callOptions: CallOptions,
      channel: Channel
  ): ClientCall[Req, Res] =
    MetricsClientCall[F, Req, Res](
      channel.newCall(methodDescriptor, callOptions),
      GrpcMethodInfo(methodDescriptor),
      metricsOps,
      classifier,
      dispatcher
    )
}

object MetricsChannelInterceptor {
  def apply[F[_]: Async: Clock](
      metricsOps: MetricsOps[F],
      classifier: Option[String] = None
  ): Resource[F, ClientInterceptor] =
    // TODO I don't think we really want to share this Dispatcher
    Dispatcher[F].map(apply(metricsOps, classifier, _))

  def apply[F[_]: Sync: Clock](
      metricsOps: MetricsOps[F],
      classifier: Option[String],
      dispatcher: Dispatcher[F]
  ): ClientInterceptor =
    new MetricsChannelInterceptor(metricsOps, classifier, dispatcher)
}

private case class MetricsClientCall[F[_]: Clock, Req, Res](
    clientCall: ClientCall[Req, Res],
    methodInfo: GrpcMethodInfo,
    metricsOps: MetricsOps[F],
    classifier: Option[String],
    dispatcher: Dispatcher[F]
)(implicit E: Sync[F])
    extends ForwardingClientCall.SimpleForwardingClientCall[Req, Res](clientCall) {

  override def start(responseListener: ClientCall.Listener[Res], headers: Metadata): Unit =
    dispatcher.unsafeRunSync {
      for {
        _ <- metricsOps.increaseActiveCalls(methodInfo, classifier)
        listener <- MetricsChannelCallListener.build[F, Res](
          delegate = responseListener,
          methodInfo = methodInfo,
          metricsOps = metricsOps,
          classifier = classifier,
          dispatcher = dispatcher
        )
        st <- E.delay(delegate.start(listener, headers))
      } yield st
    }

  override def sendMessage(requestMessage: Req): Unit =
    dispatcher.unsafeRunSync {
      metricsOps.recordMessageSent(methodInfo, classifier) *>
        E.delay(delegate.sendMessage(requestMessage))
    }
}

private class MetricsChannelCallListener[F[_], Res](
    val delegate: ClientCall.Listener[Res],
    methodInfo: GrpcMethodInfo,
    metricsOps: MetricsOps[F],
    startTime: Long,
    classifier: Option[String],
    dispatcher: Dispatcher[F]
)(implicit
    E: Sync[F],
    C: Clock[F]
) extends ForwardingClientCallListener[Res] {

  override def onHeaders(headers: Metadata): Unit =
    dispatcher.unsafeRunSync {
      for {
        now <- C.monotonic.map(_.toNanos)
        _   <- metricsOps.recordHeadersTime(methodInfo, now - startTime, classifier)
        onH <- E.delay(delegate.onHeaders(headers))
      } yield onH
    }

  override def onMessage(responseMessage: Res): Unit =
    dispatcher.unsafeRunSync {
      metricsOps.recordMessageReceived(methodInfo, classifier) *>
        E.delay(delegate.onMessage(responseMessage))
    }

  override def onClose(status: Status, metadata: Metadata): Unit =
    dispatcher.unsafeRunSync {
      for {
        now <- C.monotonic.map(_.toNanos)
        _   <- metricsOps.recordTotalTime(methodInfo, status, now - startTime, classifier)
        _   <- metricsOps.decreaseActiveCalls(methodInfo, classifier)
        onC <- E.delay(delegate.onClose(status, metadata))
      } yield onC
    }
}

private object MetricsChannelCallListener {
  def build[F[_]: Sync, Res](
      delegate: ClientCall.Listener[Res],
      methodInfo: GrpcMethodInfo,
      metricsOps: MetricsOps[F],
      classifier: Option[String],
      dispatcher: Dispatcher[F]
  )(implicit C: Clock[F]): F[MetricsChannelCallListener[F, Res]] =
    C.monotonic
      .map(_.toNanos)
      .map(
        new MetricsChannelCallListener[F, Res](
          delegate,
          methodInfo,
          metricsOps,
          _,
          classifier,
          dispatcher
        )
      )
}
