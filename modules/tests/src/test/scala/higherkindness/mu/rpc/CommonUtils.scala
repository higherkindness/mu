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

import cats.effect.kernel.Async
import cats.effect.{unsafe, IO, Resource, Sync}
import higherkindness.mu.rpc.common._
import higherkindness.mu.rpc.server._
import higherkindness.mu.rpc.testing.servers.withServerChannel
import io.grpc.{ManagedChannel, ServerServiceDefinition}
import org.slf4j.LoggerFactory

import java.net.ServerSocket
import scala.util.{Failure, Success, Try}

trait CommonUtils {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  object database {

    val i: Int = 5
    val a1: A  = A(1, 2)
    val a2: A  = A(10, 20)
    val a3: A  = A(100, 200)
    val a4: A  = A(1000, 2000)
    val b1: B  = B(a1, a2)
    val c1: C  = C("foo1", a1)
    val c2: C  = C("foo2", a1)
    val e1: E  = E(a3, "foo3")
    val e2: E  = E(a4, "foo4")

    val aList = List(a1, a2)
    val cList = List(c1, c2)
    val eList = List(e1, e2)

    val dResult: D   = D(6)
    val dResult33: D = D(33)
  }

  def createChannelFor: ChannelFor = ChannelForAddress(SC.host, SC.port)

  def createChannelForPort(port: Int): ChannelFor =
    ChannelForAddress(SC.host, port)

  def createServerConf[F[_]: Async](grpcConfigs: List[GrpcConfig]): Resource[F, GrpcServer[F]] =
    GrpcServer.defaultServer[F](SC.port, grpcConfigs)

  def createServerConfOnRandomPort[F[_]: Sync](grpcConfigs: List[GrpcConfig]): F[GrpcServer[F]] =
    GrpcServer.default[F](pickUnusedPort, grpcConfigs)

  def debug(str: String): Unit = logger.debug(str)

  val pickUnusedPort: Int =
    Try {
      val serverSocket: ServerSocket = new ServerSocket(0)
      val port: Int                  = serverSocket.getLocalPort
      serverSocket.close()
      port
    } match {
      case Success(s) => s
      case Failure(e) =>
        throw new RuntimeException(e)
    }

  def withClient[Client, T](
      serviceDef: Resource[IO, ServerServiceDefinition],
      resourceBuilder: IO[ManagedChannel] => Resource[IO, Client]
  )(f: Client => T)(implicit ioRuntime: unsafe.IORuntime): T =
    withServerChannel(serviceDef)
      .flatMap(sc => resourceBuilder(IO(sc.channel)))
      .use(client => IO(f(client)))
      .unsafeRunSync()
}
