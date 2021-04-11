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

import cats.effect._
import fs2.Stream
import io.circe.syntax._
import higherkindness.mu.http.implicits._
import org.http4s._
import org.http4s.circe._
import org.http4s.client._

class UnaryGreeterRestClient[F[_]: Sync](uri: Uri) {

  def getHello()(
      client: Client[F]
  )(implicit decoderHelloResponse: io.circe.Decoder[HelloResponse]): F[HelloResponse] = {
    val request = Request[F](Method.GET, uri / "getHello")
    client.expectOr[HelloResponse](request)(handleResponseError)(jsonOf[F, HelloResponse])
  }

  def sayHello(arg: HelloRequest)(client: Client[F])(implicit
      encoderHelloRequest: io.circe.Encoder[HelloRequest],
      decoderHelloResponse: io.circe.Decoder[HelloResponse]
  ): F[HelloResponse] = {
    val request = Request[F](Method.POST, uri / "sayHello")
    client.expectOr[HelloResponse](request.withEntity(arg.asJson))(handleResponseError)(
      jsonOf[F, HelloResponse]
    )
  }

}

class Fs2GreeterRestClient[F[_]: Sync](uri: Uri) {

  def sayHellos(arg: Stream[F, HelloRequest])(client: Client[F])(implicit
      encoderHelloRequest: io.circe.Encoder[HelloRequest],
      decoderHelloResponse: io.circe.Decoder[HelloResponse]
  ): F[HelloResponse] = {
    val request = Request[F](Method.POST, uri / "sayHellos")
    client.expectOr[HelloResponse](request.withEntity(arg.map(_.asJson)))(handleResponseError)(
      jsonOf[F, HelloResponse]
    )
  }

  def sayHelloAll(arg: HelloRequest)(client: Client[F])(implicit
      encoderHelloRequest: io.circe.Encoder[HelloRequest],
      decoderHelloResponse: io.circe.Decoder[HelloResponse]
  ): Stream[F, HelloResponse] = {
    val request = Request[F](Method.POST, uri / "sayHelloAll")
    client.stream(request.withEntity(arg.asJson)).flatMap(_.asStream[HelloResponse])
  }

  def sayHellosAll(arg: Stream[F, HelloRequest])(client: Client[F])(implicit
      encoderHelloRequest: io.circe.Encoder[HelloRequest],
      decoderHelloResponse: io.circe.Decoder[HelloResponse]
  ): Stream[F, HelloResponse] = {
    val request = Request[F](Method.POST, uri / "sayHellosAll")
    client.stream(request.withEntity(arg.map(_.asJson))).flatMap(_.asStream[HelloResponse])
  }

}
