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

package higherkindness.mu.rpc.internal.server.fs2

import cats.data.Kleisli
import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.syntax.functor._
import fs2.Stream
import fs2.grpc.server.{Fs2ServerCallHandler, GzipCompressor, ServerOptions}
import higherkindness.mu.rpc.internal.server.extractTracingKernel
import higherkindness.mu.rpc.protocol.{CompressionType, Gzip, Identity}
import io.grpc.{Metadata, MethodDescriptor, ServerCallHandler}
import natchez.{EntryPoint, Span}

object handlers {

  private def serverCallOptions(compressionType: CompressionType): ServerOptions =
    compressionType match {
      case Identity => ServerOptions.default
      case Gzip =>
        ServerOptions.default.configureCallOptions(_.withServerCompressor(Some(GzipCompressor)))
    }

  // Note: this handler is never actually used anywhere. Delete?
  def unary[F[_]: Async, Req, Res](
      f: (Req, Metadata) => F[Res],
      disp: Dispatcher[F],
      compressionType: CompressionType
  ): ServerCallHandler[Req, Res] =
    Fs2ServerCallHandler[F](disp, serverCallOptions(compressionType)).unaryToUnaryCall[Req, Res](
      f
    )

  def clientStreaming[F[_]: Async, Req, Res](
      f: (Stream[F, Req], Metadata) => F[Res],
      disp: Dispatcher[F],
      compressionType: CompressionType
  ): ServerCallHandler[Req, Res] =
    Fs2ServerCallHandler[F](disp, serverCallOptions(compressionType))
      .streamingToUnaryCall[Req, Res](f)

  def serverStreaming[F[_]: Async, Req, Res](
      f: (Req, Metadata) => F[Stream[F, Res]],
      disp: Dispatcher[F],
      compressionType: CompressionType
  ): ServerCallHandler[Req, Res] =
    Fs2ServerCallHandler[F](disp, serverCallOptions(compressionType))
      .unaryToStreamingCall[Req, Res] { (req, metadata) =>
        Stream.force(f(req, metadata))
      }

  def bidiStreaming[F[_]: Async, Req, Res](
      f: (Stream[F, Req], Metadata) => F[Stream[F, Res]],
      disp: Dispatcher[F],
      compressionType: CompressionType
  ): ServerCallHandler[Req, Res] =
    Fs2ServerCallHandler[F](disp, serverCallOptions(compressionType))
      .streamingToStreamingCall[Req, Res]((stream, metadata) => Stream.force(f(stream, metadata)))

  private type Traced[F[_], A]         = Kleisli[F, Span[F], A]
  private type StreamOfTraced[F[_], A] = Stream[Kleisli[F, Span[F], *], A]

  def tracingClientStreaming[F[_]: Async, Req, Res](
      f: StreamOfTraced[F, Req] => Traced[F, Res],
      descriptor: MethodDescriptor[Req, Res],
      entrypoint: EntryPoint[F],
      disp: Dispatcher[F],
      compressionType: CompressionType
  ): ServerCallHandler[Req, Res] =
    clientStreaming[F, Req, Res](
      { (req: Stream[F, Req], metadata: Metadata) =>
        val kernel                          = extractTracingKernel(metadata)
        val streamK: StreamOfTraced[F, Req] = req.translate(Kleisli.liftK[F, Span[F]])
        entrypoint.continueOrElseRoot(descriptor.getFullMethodName(), kernel).use[Res] { span =>
          f(streamK).run(span)
        }
      },
      disp,
      compressionType
    )

  def tracingServerStreaming[F[_]: Async, Req, Res](
      f: Req => Traced[F, StreamOfTraced[F, Res]],
      descriptor: MethodDescriptor[Req, Res],
      entrypoint: EntryPoint[F],
      disp: Dispatcher[F],
      compressionType: CompressionType
  ): ServerCallHandler[Req, Res] =
    serverStreaming[F, Req, Res](
      { (req: Req, metadata: Metadata) =>
        val kernel = extractTracingKernel(metadata)
        entrypoint
          .continueOrElseRoot(descriptor.getFullMethodName(), kernel)
          .use[Stream[F, Res]] { span =>
            val kleisli: Traced[F, StreamOfTraced[F, Res]] = f(req)
            val fStreamK: F[StreamOfTraced[F, Res]]        = kleisli.run(span)
            fStreamK.map(_.translate(Kleisli.applyK[F, Span[F]](span)))
          }
      },
      disp,
      compressionType
    )

  def tracingBidiStreaming[F[_]: Async, Req, Res](
      f: StreamOfTraced[F, Req] => Traced[F, StreamOfTraced[F, Res]],
      descriptor: MethodDescriptor[Req, Res],
      entrypoint: EntryPoint[F],
      disp: Dispatcher[F],
      compressionType: CompressionType
  ): ServerCallHandler[Req, Res] =
    bidiStreaming[F, Req, Res](
      { (req: Stream[F, Req], metadata: Metadata) =>
        val kernel = extractTracingKernel(metadata)
        val reqStreamK: StreamOfTraced[F, Req] =
          req.translate(Kleisli.liftK[F, Span[F]])
        entrypoint
          .continueOrElseRoot(descriptor.getFullMethodName(), kernel)
          .use[Stream[F, Res]] { span =>
            val kleisli: Traced[F, StreamOfTraced[F, Res]] = f(reqStreamK)
            val fStreamK: F[StreamOfTraced[F, Res]]        = kleisli.run(span)
            fStreamK.map(_.translate(Kleisli.applyK[F, Span[F]](span)))
          }
      },
      disp,
      compressionType
    )

}
