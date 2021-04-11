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

import java.net.InetSocketAddress

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import higherkindness.mu.rpc.common.SC

class ServerConfigTests extends RpcServerTestSuite {

  import implicits._

  "GrpcServer.default" should {

    "work as expected for a basic configuration" in {

      val configList: List[GrpcConfig] = List(AddService(sd1))
      val server: IO[GrpcServer[IO]]   = GrpcServer.default(SC.port, configList)

      server.flatMap(_.getServices).unsafeRunSync() shouldBe List(sd1)
    }
  }

  "GrpcServer.netty" should {

    "work as expected for port" in {

      val configList: List[GrpcConfig] = List(AddService(sd1))
      val server: IO[GrpcServer[IO]]   = GrpcServer.netty(ChannelForPort(SC.port), configList)

      server.flatMap(_.getServices).unsafeRunSync() shouldBe List(sd1)
    }

    "work as expected for SocketAddress" in {

      val configList: List[GrpcConfig] = List(AddService(sd1))
      val server: IO[GrpcServer[IO]] =
        GrpcServer.netty(
          ChannelForSocketAddress(new InetSocketAddress(SC.host, SC.port)),
          configList
        )

      server.flatMap(_.getServices).unsafeRunSync() shouldBe List(sd1)
    }

    "work as expected for port, with any configuration combination" in {

      val server: IO[GrpcServer[IO]] =
        GrpcServer.netty(ChannelForPort(SC.port), grpcAllConfigList)

      server.flatMap(_.getServices).unsafeRunSync() shouldBe List(sd1)
    }

    "work as expected for an `Int` port" in {

      val configList: List[GrpcConfig] = List(AddService(sd1))
      val server: IO[GrpcServer[IO]]   = GrpcServer.netty[IO](SC.port, configList)

      server.flatMap(_.getServices).unsafeRunSync() shouldBe List(sd1)
    }

    "throw an exception when configuration is not recognized" in {

      case object Unexpected extends GrpcConfig

      val configList: List[GrpcConfig] = List(AddService(sd1), Unexpected)

      val server: IO[GrpcServer[IO]] =
        GrpcServer.netty(ChannelForPort(SC.port), configList)

      an[MatchError] shouldBe thrownBy(server.unsafeRunSync())
    }

    "throw an exception when ChannelFor is not recognized" in {

      val configList: List[GrpcConfig] = List(AddService(sd1))

      val server: IO[GrpcServer[IO]] =
        GrpcServer.netty(ChannelForTarget(SC.host), configList)

      an[IllegalArgumentException] shouldBe thrownBy(server.unsafeRunSync())
    }
  }

}
