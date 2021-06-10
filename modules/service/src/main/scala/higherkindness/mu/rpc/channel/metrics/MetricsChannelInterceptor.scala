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

import cats.effect.std.Dispatcher
import cats.effect.{Async, Clock, Sync}
import cats.syntax.all._
import higherkindness.mu.rpc.internal.interceptors.GrpcMethodInfo
import higherkindness.mu.rpc.internal.metrics.MetricsOps
import io.grpc._

case class MetricsChannelInterceptor[F[_]: Async](
    metricsOps: MetricsOps[F],
    disp: Dispatcher[F],
    classifier: Option[String] = None
) extends ClientInterceptor {

  override def interceptCall[Req, Res](
      methodDescriptor: MethodDescriptor[Req, Res],
      callOptions: CallOptions,
      channel: Channel
  ): ClientCall[Req, Res] = {

    val methodInfo: GrpcMethodInfo = GrpcMethodInfo(methodDescriptor)

    MetricsClientCall[F, Req, Res](
      channel.newCall(methodDescriptor, callOptions),
      methodInfo,
      metricsOps,
      classifier,
      disp
    )
  }
}

case class MetricsClientCall[F[_]: Clock, Req, Res](
    clientCall: ClientCall[Req, Res],
    methodInfo: GrpcMethodInfo,
    metricsOps: MetricsOps[F],
    classifier: Option[String],
    disp: Dispatcher[F]
)(implicit F: Async[F])
    extends ForwardingClientCall.SimpleForwardingClientCall[Req, Res](clientCall) {

  override def start(responseListener: ClientCall.Listener[Res], headers: Metadata): Unit =
    disp.unsafeRunSync {
      for {
        _ <- metricsOps.increaseActiveCalls(methodInfo, classifier)
        listener <- MetricsChannelCallListener.build[F, Res](
          delegate = responseListener,
          methodInfo = methodInfo,
          metricsOps = metricsOps,
          classifier = classifier,
          disp
        )
        st <- F.delay(delegate.start(listener, headers))
      } yield st
    }

  override def sendMessage(requestMessage: Req): Unit =
    disp.unsafeRunSync {
      metricsOps.recordMessageSent(methodInfo, classifier) *>
        F.delay(delegate.sendMessage(requestMessage))
    }
}

class MetricsChannelCallListener[F[_], Res](
    val delegate: ClientCall.Listener[Res],
    methodInfo: GrpcMethodInfo,
    metricsOps: MetricsOps[F],
    startTime: Long,
    classifier: Option[String],
    disp: Dispatcher[F]
)(implicit
    F: Sync[F]
) extends ForwardingClientCallListener[Res] {

  override def onHeaders(headers: Metadata): Unit =
    disp.unsafeRunSync {
      for {
        now <- Clock[F].monotonic.map(_.toNanos)
        _   <- metricsOps.recordHeadersTime(methodInfo, now - startTime, classifier)
        onH <- F.delay(delegate.onHeaders(headers))
      } yield onH
    }

  override def onMessage(responseMessage: Res): Unit =
    disp.unsafeRunSync {
      metricsOps.recordMessageReceived(methodInfo, classifier) *>
        F.delay(delegate.onMessage(responseMessage))
    }

  override def onClose(status: Status, metadata: Metadata): Unit =
    disp.unsafeRunSync {
      for {
        now <- Clock[F].monotonic.map(_.toNanos)
        _   <- metricsOps.recordTotalTime(methodInfo, status, now - startTime, classifier)
        _   <- metricsOps.decreaseActiveCalls(methodInfo, classifier)
        onC <- F.delay(delegate.onClose(status, metadata))
      } yield onC
    }
}

object MetricsChannelCallListener {
  def build[F[_]: Sync, Res](
      delegate: ClientCall.Listener[Res],
      methodInfo: GrpcMethodInfo,
      metricsOps: MetricsOps[F],
      classifier: Option[String],
      disp: Dispatcher[F]
  ): F[MetricsChannelCallListener[F, Res]] = {
    Clock[F].monotonic
      .map(_.toNanos)
      .map(
        new MetricsChannelCallListener[F, Res](
          delegate,
          methodInfo,
          metricsOps,
          _,
          classifier,
          disp
        )
      )
  }
}
