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

package higherkindness.mu.rpc.server.metrics

import cats.effect.std.Dispatcher
import cats.effect.{Clock, Sync}
import cats.syntax.all._
import higherkindness.mu.rpc.internal.interceptors.GrpcMethodInfo
import higherkindness.mu.rpc.internal.metrics.MetricsOps
import io.grpc._

case class MetricsServerInterceptor[F[_]: Sync](
    metricsOps: MetricsOps[F],
    disp: Dispatcher[F],
    classifier: Option[String] = None
) extends ServerInterceptor {

  override def interceptCall[Req, Res](
      call: ServerCall[Req, Res],
      requestHeaders: Metadata,
      next: ServerCallHandler[Req, Res]
  ): ServerCall.Listener[Req] = {

    val methodInfo: GrpcMethodInfo = GrpcMethodInfo(call.getMethodDescriptor)

    disp.unsafeRunSync {
      MetricsServerCall
        .build[F, Req, Res](call, methodInfo, metricsOps, classifier, disp)
        .map(metricsCall =>
          MetricsServerCallListener[F, Req](
            next.startCall(metricsCall, requestHeaders),
            methodInfo,
            metricsOps,
            classifier,
            disp
          )
        )
    }
  }
}

case class MetricsServerCall[F[_]: Sync, Req, Res](
    serverCall: ServerCall[Req, Res],
    methodInfo: GrpcMethodInfo,
    metricsOps: MetricsOps[F],
    startTime: Long,
    classifier: Option[String],
    disp: Dispatcher[F]
) extends ForwardingServerCall.SimpleForwardingServerCall[Req, Res](serverCall) {

  override def close(status: Status, responseHeaders: Metadata): Unit =
    disp.unsafeRunSync {
      for {
        now <- Clock[F].monotonic.map(_.toNanos)
        _   <- metricsOps.recordTotalTime(methodInfo, status, now - startTime, classifier)
        onC <- Sync[F].delay(delegate.close(status, responseHeaders))
      } yield onC
    }

  override def sendMessage(message: Res): Unit =
    disp.unsafeRunSync {
      metricsOps.recordMessageSent(methodInfo, classifier) *>
        Sync[F].delay(delegate.sendMessage(message))
    }
}

object MetricsServerCall {
  def build[F[_]: Sync, Req, Res](
      serverCall: ServerCall[Req, Res],
      methodInfo: GrpcMethodInfo,
      metricsOps: MetricsOps[F],
      classifier: Option[String],
      disp: Dispatcher[F]
  ): F[MetricsServerCall[F, Req, Res]] = {
    Clock[F].monotonic
      .map(_.toNanos)
      .map(
        new MetricsServerCall[F, Req, Res](
          serverCall,
          methodInfo,
          metricsOps,
          _,
          classifier,
          disp
        )
      )
  }
}

case class MetricsServerCallListener[F[_]: Sync, Req](
    delegate: ServerCall.Listener[Req],
    methodInfo: GrpcMethodInfo,
    metricsOps: MetricsOps[F],
    classifier: Option[String],
    disp: Dispatcher[F]
) extends ForwardingServerCallListener[Req] {

  override def onMessage(request: Req): Unit =
    disp.unsafeRunSync {
      for {
        _   <- metricsOps.recordMessageReceived(methodInfo, classifier)
        _   <- metricsOps.increaseActiveCalls(methodInfo, classifier)
        onM <- Sync[F].delay(delegate.onMessage(request))
      } yield onM
    }

  override def onComplete(): Unit =
    disp.unsafeRunSync {
      metricsOps.decreaseActiveCalls(methodInfo, classifier) *>
        Sync[F].delay(delegate.onComplete())
    }
}
