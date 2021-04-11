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

package higherkindness.mu.rpc.internal.client.fs2

import fs2.Stream

import cats.data.Kleisli
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._
import higherkindness.mu.rpc.internal.client.tracingKernelToHeaders
import io.grpc.{CallOptions, Channel, Metadata, MethodDescriptor, StatusRuntimeException}
import org.lyranthe.fs2_grpc.java_runtime.client.Fs2ClientCall
import natchez.Span

object calls {

  private val errorAdapter: StatusRuntimeException => Option[Exception] = _ => None

  def unary[F[_]: Async, Req, Res](
      request: Req,
      descriptor: MethodDescriptor[Req, Res],
      channel: Channel,
      options: CallOptions,
      dispatcher: Dispatcher[F],
      headers: Metadata = new Metadata()
  ): F[Res] =
    Fs2ClientCall[F](channel, descriptor, options, dispatcher, errorAdapter)
      .flatMap(_.unaryToUnaryCall(request, headers))

  def clientStreaming[F[_]: Async, Req, Res](
      input: Stream[F, Req],
      descriptor: MethodDescriptor[Req, Res],
      channel: Channel,
      options: CallOptions,
      dispatcher: Dispatcher[F],
      headers: Metadata = new Metadata()
  ): F[Res] =
    Fs2ClientCall[F](channel, descriptor, options, dispatcher, errorAdapter)
      .flatMap(_.streamingToUnaryCall(input, headers))

  def serverStreaming[F[_]: Async, Req, Res](
      request: Req,
      descriptor: MethodDescriptor[Req, Res],
      channel: Channel,
      options: CallOptions,
      dispatcher: Dispatcher[F],
      headers: Metadata = new Metadata()
  ): F[Stream[F, Res]] =
    Stream
      .eval(Fs2ClientCall[F](channel, descriptor, options, dispatcher, errorAdapter))
      .flatMap(_.unaryToStreamingCall(request, headers))
      .pure[F]

  def bidiStreaming[F[_]: Async, Req, Res](
      input: Stream[F, Req],
      descriptor: MethodDescriptor[Req, Res],
      channel: Channel,
      options: CallOptions,
      dispatcher: Dispatcher[F],
      headers: Metadata = new Metadata()
  ): F[Stream[F, Res]] =
    Stream
      .eval(Fs2ClientCall[F](channel, descriptor, options, dispatcher, errorAdapter))
      .flatMap(_.streamingToStreamingCall(input, headers))
      .pure[F]

  def tracingClientStreaming[F[_]: Async, Req, Res](
      input: Stream[Kleisli[F, Span[F], *], Req],
      descriptor: MethodDescriptor[Req, Res],
      channel: Channel,
      dispatcher: Dispatcher[F],
      options: CallOptions
  ): Kleisli[F, Span[F], Res] =
    Kleisli[F, Span[F], Res] { parentSpan =>
      parentSpan.span(descriptor.getFullMethodName()).use { span =>
        span.kernel.flatMap { kernel =>
          val headers = tracingKernelToHeaders(kernel)
          val streamF: Stream[F, Req] =
            input.translate(Kleisli.applyK[F, Span[F]](span))
          clientStreaming[F, Req, Res](
            streamF,
            descriptor,
            channel,
            options,
            dispatcher,
            headers
          )
        }
      }
    }

  def tracingServerStreaming[F[_]: Async, Req, Res](
      request: Req,
      descriptor: MethodDescriptor[Req, Res],
      channel: Channel,
      dispatcher: Dispatcher[F],
      options: CallOptions
  ): Kleisli[F, Span[F], Stream[Kleisli[F, Span[F], *], Res]] =
    Kleisli[F, Span[F], Stream[Kleisli[F, Span[F], *], Res]] { parentSpan =>
      parentSpan.span(descriptor.getFullMethodName()).use { span =>
        span.kernel.map { kernel =>
          val headers = tracingKernelToHeaders(kernel)
          Stream
            .eval(Fs2ClientCall[F](channel, descriptor, options, dispatcher, errorAdapter))
            .flatMap(_.unaryToStreamingCall(request, headers))
            .translate(Kleisli.liftK[F, Span[F]])
        }
      }
    }

  def tracingBidiStreaming[F[_]: Async, Req, Res](
      input: Stream[Kleisli[F, Span[F], *], Req],
      descriptor: MethodDescriptor[Req, Res],
      channel: Channel,
      dispatcher: Dispatcher[F],
      options: CallOptions
  ): Kleisli[F, Span[F], Stream[Kleisli[F, Span[F], *], Res]] =
    Kleisli[F, Span[F], Stream[Kleisli[F, Span[F], *], Res]] { parentSpan =>
      parentSpan.span(descriptor.getFullMethodName()).use { span =>
        span.kernel.map { kernel =>
          val headers = tracingKernelToHeaders(kernel)
          val streamF: Stream[F, Req] =
            input.translate(Kleisli.applyK[F, Span[F]](span))
          Stream
            .eval(Fs2ClientCall[F](channel, descriptor, options, dispatcher, errorAdapter))
            .flatMap(_.streamingToStreamingCall(streamF, headers))
            .translate(Kleisli.liftK[F, Span[F]])
        }
      }
    }

}
