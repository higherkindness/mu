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

package higherkindness.mu.rpc.internal.server

import cats.effect.kernel.Sync
import cats.effect.std.Dispatcher
import cats.data.Kleisli
import cats.syntax.all._
import io.grpc._
import io.grpc.ServerCall.Listener
import io.grpc.stub.ServerCalls
import io.grpc.stub.ServerCalls.UnaryMethod
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import higherkindness.mu.rpc.protocol.CompressionType
import natchez.{EntryPoint, Span}

object handlers {

  def unary[F[_]: Sync, Req, Res](
      f: Req => F[Res],
      compressionType: CompressionType,
      dispatcher: Dispatcher[F]
  ): ServerCallHandler[Req, Res] =
    ServerCalls.asyncUnaryCall(unaryMethod[F, Req, Res](f, compressionType, dispatcher))

  def tracingUnary[F[_]: Sync, Req, Res](
      f: Req => Kleisli[F, Span[F], Res],
      methodDescriptor: MethodDescriptor[Req, Res],
      entrypoint: EntryPoint[F],
      compressionType: CompressionType,
      dispatcher: Dispatcher[F]
  ): ServerCallHandler[Req, Res] =
    new ServerCallHandler[Req, Res] {
      def startCall(
          call: ServerCall[Req, Res],
          metadata: Metadata
      ): Listener[Req] = {
        val kernel = extractTracingKernel(metadata)
        val spanResource =
          entrypoint.continueOrElseRoot(methodDescriptor.getFullMethodName(), kernel)

        val method = unaryMethod[F, Req, Res](
          req => spanResource.use(span => f(req).run(span)),
          compressionType,
          dispatcher
        )

        ServerCalls.asyncUnaryCall(method).startCall(call, metadata)
      }
    }

  private def unaryMethod[F[_]: Sync, Req, Res](
      f: Req => F[Res],
      compressionType: CompressionType,
      dispatcher: Dispatcher[F]
  ): UnaryMethod[Req, Res] =
    new UnaryMethod[Req, Res] {
      override def invoke(request: Req, observer: StreamObserver[Res]): Unit = {
        val handleRequest = (addCompression(observer, compressionType) *> f(request))
          .map { value =>
            observer.onNext(value)
            observer.onCompleted()
          }
          .handleError { t =>
            observer.onError(t match {
              case s: StatusException        => s
              case s: StatusRuntimeException => s
              case other =>
                Status.INTERNAL.withDescription(other.getMessage).withCause(other).asException()
            })
          }
        val cancel = dispatcher.unsafeRunCancelable(handleRequest)
        observer
          .asInstanceOf[ServerCallStreamObserver[Res]]
          .setOnCancelHandler { () => cancel(); () }
      }
    }
}
