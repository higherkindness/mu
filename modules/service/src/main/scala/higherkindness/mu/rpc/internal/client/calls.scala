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

package higherkindness.mu.rpc.internal.client

import cats.data.Kleisli
import cats.effect.{Async, Sync}
import cats.syntax.flatMap._
import io.grpc.stub.ClientCalls
import io.grpc.{CallOptions, Channel, Metadata, MethodDescriptor}
import natchez.Span

object calls {

  def unary[F[_]: Async, Req, Res](
      request: Req,
      descriptor: MethodDescriptor[Req, Res],
      channel: Channel,
      options: CallOptions,
      extraHeaders: Metadata = new Metadata()
  ): F[Res] =
    listenableFuture2Async[F, Res](
      Sync[F].delay(
        ClientCalls
          .futureUnaryCall(
            new HeaderAttachingClientCall(channel.newCall(descriptor, options), extraHeaders),
            request
          )
      )
    )

  def tracingUnary[F[_]: Async, Req, Res](
      request: Req,
      descriptor: MethodDescriptor[Req, Res],
      channel: Channel,
      options: CallOptions
  ): Kleisli[F, Span[F], Res] =
    Kleisli[F, Span[F], Res] { parentSpan =>
      parentSpan.span(descriptor.getFullMethodName).use { span =>
        span.kernel.flatMap { kernel =>
          val headers = tracingKernelToHeaders(kernel)
          unary[F, Req, Res](request, descriptor, channel, options, headers)
        }
      }
    }

}
