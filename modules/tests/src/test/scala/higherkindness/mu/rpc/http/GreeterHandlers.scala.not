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

package higherkindness.mu.rpc.http

import cats.{Applicative, MonadError}
import cats.syntax.applicative._
import cats.syntax.functor._
import cats.effect._

class UnaryGreeterHandler[F[_]: Applicative](implicit F: MonadError[F, Throwable])
    extends UnaryGreeter[F] {

  import higherkindness.mu.rpc.protocol.Empty
  import io.grpc.Status._

  def getHello(request: Empty.type): F[HelloResponse] = HelloResponse("hey").pure

  def sayHello(request: HelloRequest): F[HelloResponse] =
    request.hello match {
      case "SE"  => F.raiseError(INVALID_ARGUMENT.withDescription("SE").asException)
      case "SRE" => F.raiseError(INVALID_ARGUMENT.withDescription("SRE").asRuntimeException)
      case "RTE" => F.raiseError(new IllegalArgumentException("RTE"))
      case "TR"  => throw new IllegalArgumentException("Thrown")
      case other => HelloResponse(other).pure
    }

}

class Fs2GreeterHandler[F[_]: Sync] extends Fs2Greeter[F] {

  import fs2.Stream
  import higherkindness.mu.rpc.protocol.Empty

  def sayHellos(requests: Stream[F, HelloRequest]): F[HelloResponse] =
    requests.compile.fold(HelloResponse("")) { case (response, request) =>
      HelloResponse(
        if (response.hello.isEmpty) request.hello else s"${response.hello}, ${request.hello}"
      )
    }

  def rudelyIgnoreStreamOfHellos(requests: Stream[F, HelloRequest]): F[Empty.type] =
    requests.compile.drain.as(Empty)

  def sayHelloAll(request: HelloRequest): F[Stream[F, HelloResponse]] = {
    val stream = {
      if (request.hello.isEmpty) Stream.raiseError(new IllegalArgumentException("empty greeting"))
      else Stream(HelloResponse(request.hello), HelloResponse(request.hello))
    }
    stream.pure[F]
  }

  def sayHelloAllEmptyRequest(request: Empty.type): F[Stream[F, HelloResponse]] =
    Stream(HelloResponse("hey"), HelloResponse("hi again")).covary[F].pure[F]

  def sayHellosAll(requests: Stream[F, HelloRequest]): F[Stream[F, HelloResponse]] =
    requests.map(request => HelloResponse(request.hello)).pure[F]
}
