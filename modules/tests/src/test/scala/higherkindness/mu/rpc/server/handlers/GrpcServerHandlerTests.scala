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
package server.handlers

import cats.effect.IO
import higherkindness.mu.rpc.common.SC
import higherkindness.mu.rpc.server.RpcServerTestSuite
import io.grpc.{Server, ServerServiceDefinition}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.TimeUnit

class GrpcServerHandlerTests extends RpcServerTestSuite {

  val executionContext: ExecutionContext = ExecutionContext.Implicits.global

  import implicits._

  val handler = GrpcServerHandler[IO]

  "GrpcServer.Handler" should {

    "allow to start a GrpcServer" in {

      runK(handler.start, serverMock).attempt.unsafeRunSync().isRight shouldBe true
      (serverMock.start _: () => Server).verify().once()

    }

    "allow to get the port where server is running" in {

      runK(handler.getPort, serverMock).unsafeRunSync() shouldBe SC.port
      (serverMock.getPort _: () => Int).verify().once()

    }

    "allow to get the services running under the Server instance" in {

      runK(handler.getServices, serverMock).unsafeRunSync() shouldBe serviceList
      (serverMock.getServices _: () => java.util.List[ServerServiceDefinition]).verify().once()

    }

    "allow to get the immutable services running under the Server instance" in {

      runK(handler.getImmutableServices, serverMock).unsafeRunSync() shouldBe immutableServiceList
      (serverMock.getImmutableServices _: () => java.util.List[ServerServiceDefinition])
        .verify()
        .once()

    }

    "allow to get the mutable services running under the Server instance" in {

      runK(handler.getMutableServices, serverMock).unsafeRunSync() shouldBe mutableServiceList
      (serverMock.getMutableServices _: () => java.util.List[ServerServiceDefinition])
        .verify()
        .once()

    }

    "allow to stop a started GrpcServer" in {

      runK(handler.shutdown, serverMock).attempt.unsafeRunSync().isRight shouldBe true
      (serverMock.shutdown _: () => Server).verify().once()

    }

    "allow to stop immediately a started GrpcServer" in {

      runK(handler.shutdownNow, serverMock).attempt.unsafeRunSync().isRight shouldBe true
      (serverMock.shutdownNow _: () => Server).verify().once()

    }

    "allow to ask whether a Server is shutdown" in {

      runK(handler.isShutdown, serverMock).unsafeRunSync() shouldBe b
      (serverMock.isShutdown _: () => Boolean).verify().once()

    }

    "allow to ask whether a Server instance has been terminated" in {

      runK(handler.isTerminated, serverMock).unsafeRunSync() shouldBe b
      (serverMock.isTerminated _: () => Boolean).verify().once()

    }

    "allow to terminate after a certain timeout is reached" in {

      runK(handler.awaitTerminationTimeout(timeout, timeoutUnit), serverMock)
        .unsafeRunSync() shouldBe b
      (serverMock.awaitTermination(_: Long, _: TimeUnit)).verify(timeout, timeoutUnit).once()

    }

    "allow stopping a started GrpcServer" in {

      runK(handler.awaitTermination, serverMock).unsafeRunSync() shouldBe unit
      (serverMock.awaitTermination _: () => Unit).verify().once()

    }

  }

}
