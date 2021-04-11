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

import cats.effect.kernel.{Async, Clock, Resource, Sync}
import cats.effect.std.Dispatcher
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import higherkindness.mu.rpc.internal.interceptors.GrpcMethodInfo
import higherkindness.mu.rpc.internal.metrics.MetricsOps
import io.grpc._

object MetricsServerInterceptor {
  def apply[F[_]: Async: Clock](
      metricsOps: MetricsOps[F],
      classifier: Option[String] = None
  ): Resource[F, ServerInterceptor] =
    Dispatcher[F].map(apply(metricsOps, _, classifier))

  def apply[F[_]: Sync: Clock](
      metricsOps: MetricsOps[F],
      dispatcher: Dispatcher[F],
      classifier: Option[String]
  ): ServerInterceptor =
    new MetricsServerInterceptor(metricsOps, dispatcher, classifier)
}

private class MetricsServerInterceptor[F[_]: Clock](
    metricsOps: MetricsOps[F],
    dispatcher: Dispatcher[F],
    classifier: Option[String]
)(implicit E: Sync[F])
    extends ServerInterceptor {

  override def interceptCall[Req, Res](
      call: ServerCall[Req, Res],
      requestHeaders: Metadata,
      next: ServerCallHandler[Req, Res]
  ): ServerCall.Listener[Req] = {
    val methodInfo: GrpcMethodInfo = GrpcMethodInfo(call.getMethodDescriptor)
    // ugh
    dispatcher.unsafeRunSync(
      MetricsServerCall
        .build[F, Req, Res](call, methodInfo, metricsOps, classifier, dispatcher)
        .map(metricsCall =>
          MetricsServerCallListener[F, Req](
            next.startCall(metricsCall, requestHeaders),
            methodInfo,
            metricsOps,
            classifier,
            dispatcher
          )
        )
    )
  }
}

private case class MetricsServerCall[F[_], Req, Res](
    serverCall: ServerCall[Req, Res],
    methodInfo: GrpcMethodInfo,
    metricsOps: MetricsOps[F],
    startTime: Long,
    classifier: Option[String],
    dispatcher: Dispatcher[F]
)(implicit
    E: Sync[F],
    C: Clock[F]
) extends ForwardingServerCall.SimpleForwardingServerCall[Req, Res](serverCall) {

  override def close(status: Status, responseHeaders: Metadata): Unit =
    dispatcher.unsafeRunSync {
      for {
        now <- C.monotonic.map(_.toNanos)
        _   <- metricsOps.recordTotalTime(methodInfo, status, now - startTime, classifier)
        onC <- E.delay(delegate.close(status, responseHeaders))
      } yield onC
    }

  override def sendMessage(message: Res): Unit =
    dispatcher.unsafeRunSync {
      metricsOps.recordMessageSent(methodInfo, classifier) *>
        E.delay(delegate.sendMessage(message))
    }
}

private object MetricsServerCall {
  def build[F[_]: Sync, Req, Res](
      serverCall: ServerCall[Req, Res],
      methodInfo: GrpcMethodInfo,
      metricsOps: MetricsOps[F],
      classifier: Option[String],
      dispatcher: Dispatcher[F]
  )(implicit C: Clock[F]): F[MetricsServerCall[F, Req, Res]] =
    C.monotonic
      .map(_.toNanos)
      .map(
        new MetricsServerCall[F, Req, Res](
          serverCall,
          methodInfo,
          metricsOps,
          _,
          classifier,
          dispatcher
        )
      )
}

private case class MetricsServerCallListener[F[_], Req](
    delegate: ServerCall.Listener[Req],
    methodInfo: GrpcMethodInfo,
    metricsOps: MetricsOps[F],
    classifier: Option[String],
    dispatcher: Dispatcher[F]
)(implicit E: Sync[F])
    extends ForwardingServerCallListener[Req] {

  override def onMessage(request: Req): Unit =
    dispatcher.unsafeRunSync {
      for {
        _   <- metricsOps.recordMessageReceived(methodInfo, classifier)
        _   <- metricsOps.increaseActiveCalls(methodInfo, classifier)
        onM <- E.delay(delegate.onMessage(request))
      } yield onM
    }

  override def onComplete(): Unit =
    dispatcher.unsafeRunSync {
      metricsOps.decreaseActiveCalls(methodInfo, classifier) *>
        E.delay(delegate.onComplete())
    }
}
