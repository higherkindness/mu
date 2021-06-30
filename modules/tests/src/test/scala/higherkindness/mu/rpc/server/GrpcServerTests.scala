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
package server

import java.util.concurrent.TimeUnit

import cats.Apply
import cats.syntax.apply._
import higherkindness.mu.rpc.common.SC
import cats.effect.IO
import io.grpc.{Server, ServerServiceDefinition}

class GrpcServerTests extends RpcServerTestSuite {

  import implicits._

  "GrpcServer" should {

    type Result = (
        Boolean,
        Unit,
        Int,
        List[ServerServiceDefinition],
        List[ServerServiceDefinition],
        List[ServerServiceDefinition],
        Unit,
        Unit,
        Boolean,
        Boolean,
        Boolean,
        Unit
    )

    "behaves as expected" in {

      def program[F[_]: Apply](APP: GrpcServer[F]): F[Result] = {

        import APP._

        val a = isShutdown
        val b = start
        val c = getPort
        val d = getServices
        val e = getImmutableServices
        val f = getMutableServices
        val g = shutdown
        val h = shutdownNow
        val i = isShutdown
        val j = isTerminated
        val k = awaitTerminationTimeout(timeout, timeoutUnit)
        val l = awaitTermination

        (a, b, c, d, e, f, g, h, i, j, k, l).tupled
      }

      program[IO](grpcServerHandlerTests).unsafeRunSync() shouldBe ((
        b,
        (),
        SC.port,
        serviceList,
        immutableServiceList,
        mutableServiceList,
        (),
        (),
        b,
        b,
        b,
        unit
      ): Result)

      (serverMock.start _: () => Server).verify().once()
      (serverMock.getPort _: () => Int).verify().once()
      (serverMock.getServices _: () => java.util.List[ServerServiceDefinition]).verify().once()
      (serverMock.getImmutableServices _: () => java.util.List[ServerServiceDefinition])
        .verify()
        .once()
      (serverMock.getMutableServices _: () => java.util.List[ServerServiceDefinition])
        .verify()
        .once()
      (serverMock.shutdown _: () => Server).verify().once()
      (serverMock.shutdownNow _: () => Server).verify().once()
      (serverMock.isShutdown _: () => Boolean).verify().twice()
      (serverMock.isTerminated _: () => Boolean).verify().once()
      (serverMock.awaitTermination(_: Long, _: TimeUnit)).verify(timeout, timeoutUnit).once()
      (serverMock.awaitTermination _: () => Unit).verify().once()
    }

  }

}
