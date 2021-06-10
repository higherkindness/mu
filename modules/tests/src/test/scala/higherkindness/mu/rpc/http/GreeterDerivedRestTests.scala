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

import cats.effect.{IO, _}
import fs2.Stream
import higherkindness.mu.http.{ResponseError, UnexpectedError}
import higherkindness.mu.http.protocol.{HttpServer, RouteMap}
import higherkindness.mu.rpc.common.RpcBaseTestSuite
import io.circe.generic.auto._
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.blaze.server._
import org.scalatest._

class GreeterDerivedRestTests extends RpcBaseTestSuite with OneInstancePerTest with BeforeAndAfter {

  val host            = "localhost"
  val port            = 8080
  val serviceUri: Uri = Uri.unsafeFromString(s"http://$host:$port")

  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  implicit val unaryHandlerIO = new UnaryGreeterHandler[IO]
  implicit val fs2HandlerIO   = new Fs2GreeterHandler[IO]

  val unaryRoute: RouteMap[IO] = UnaryGreeter.route[IO]
  val fs2Route: RouteMap[IO]   = Fs2Greeter.route[IO]

  val server: BlazeServerBuilder[IO] = HttpServer.bind(port, host, unaryRoute, fs2Route)

  var serverTask: Fiber[IO, Throwable, Nothing] = _
  before {
    serverTask = server.resource.use(_ => IO.never).start.unsafeRunSync()
  }
  after(serverTask.cancel)

  "REST Service" should {

    val unaryClient: UnaryGreeter.HttpClient[IO] = UnaryGreeter.httpClient[IO](serviceUri)
    val fs2Client: Fs2Greeter.HttpClient[IO]     = Fs2Greeter.httpClient[IO](serviceUri)

    "serve a GET request" in {
      val response: IO[HelloResponse] =
        BlazeClientBuilder[IO](ec).resource.use(unaryClient.getHello(_))
      response.unsafeRunSync() shouldBe HelloResponse("hey")
    }

    "serve a unary POST request" in {
      val response: IO[HelloResponse] =
        BlazeClientBuilder[IO](ec).resource.use(unaryClient.sayHello(HelloRequest("hey"))(_))
      response.unsafeRunSync() shouldBe HelloResponse("hey")
    }

    "handle a raised gRPC exception in a unary POST request" in {
      val response: IO[HelloResponse] =
        BlazeClientBuilder[IO](ec).resource.use(unaryClient.sayHello(HelloRequest("SRE"))(_))

      the[ResponseError] thrownBy response.unsafeRunSync() shouldBe ResponseError(
        Status.BadRequest,
        Some("INVALID_ARGUMENT: SRE")
      )
    }

    "handle a raised non-gRPC exception in a unary POST request" in {
      val response: IO[HelloResponse] =
        BlazeClientBuilder[IO](ec).resource.use(unaryClient.sayHello(HelloRequest("RTE"))(_))

      the[ResponseError] thrownBy response.unsafeRunSync() shouldBe ResponseError(
        Status.InternalServerError,
        Some("RTE")
      )
    }

    "handle a thrown exception in a unary POST request" in {
      val response: IO[HelloResponse] =
        BlazeClientBuilder[IO](ec).resource.use(unaryClient.sayHello(HelloRequest("TR"))(_))

      the[ResponseError] thrownBy response.unsafeRunSync() shouldBe ResponseError(
        Status.InternalServerError
      )
    }

    "serve a POST request with fs2 streaming request" in {

      val requests = Stream(HelloRequest("hey"), HelloRequest("there"))

      val response: IO[HelloResponse] =
        BlazeClientBuilder[IO](ec).resource.use(fs2Client.sayHellos(requests)(_))
      response.unsafeRunSync() shouldBe HelloResponse("hey, there")
    }

    "serve a POST request with empty fs2 streaming request" in {
      val requests = Stream.empty
      val response =
        BlazeClientBuilder[IO](ec).resource.use(fs2Client.sayHellos(requests)(_))
      response.unsafeRunSync() shouldBe HelloResponse("")
    }

    "serve a POST request with fs2 streaming response" in {
      val request = HelloRequest("hey")
      val responses =
        BlazeClientBuilder[IO](ec).stream
          .evalMap(client => fs2Client.sayHelloAll(request)(client))
          .flatten
      responses.compile.toList
        .unsafeRunSync() shouldBe List(HelloResponse("hey"), HelloResponse("hey"))
    }

    "handle errors with fs2 streaming response" in {
      val request = HelloRequest("")
      val responses =
        BlazeClientBuilder[IO](ec).stream.flatMap(client =>
          Stream.force(fs2Client.sayHelloAll(request)(client))
        )
      the[UnexpectedError] thrownBy responses.compile.toList
        .unsafeRunSync() should have message "java.lang.IllegalArgumentException: empty greeting"
    }

    "serve a POST request with bidirectional fs2 streaming" in {
      val requests = Stream(HelloRequest("hey"), HelloRequest("there"))
      val responses =
        BlazeClientBuilder[IO](ec).stream.flatMap(client =>
          Stream.force(fs2Client.sayHellosAll(requests)(client))
        )
      responses.compile.toList
        .unsafeRunSync() shouldBe List(HelloResponse("hey"), HelloResponse("there"))
    }

    "serve an empty POST request with bidirectional fs2 streaming" in {
      val requests = Stream.empty
      val responses =
        BlazeClientBuilder[IO](ec).stream.flatMap(client =>
          Stream.force(fs2Client.sayHellosAll(requests)(client))
        )
      responses.compile.toList.unsafeRunSync() shouldBe Nil
    }

  }

}
