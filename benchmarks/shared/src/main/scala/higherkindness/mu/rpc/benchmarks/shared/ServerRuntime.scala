/*
 * Copyright 2017-2020 47 Degrees, LLC. <http://www.47deg.com>
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

package higherkindness.mu.rpc.benchmarks
package shared

import cats.effect._
import cats.syntax.traverse._
import higherkindness.mu.rpc._
import higherkindness.mu.rpc.channel.ManagedChannelInterpreter
import higherkindness.mu.rpc.benchmarks.shared.protocols._
import higherkindness.mu.rpc.benchmarks.shared.server._
import higherkindness.mu.rpc.server._

import scala.concurrent.ExecutionContext
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import java.util.concurrent.TimeUnit
import io.grpc.ManagedChannel
import higherkindness.mu.rpc.channel.UsePlaintext

trait ServerRuntime {

  val grpcPort: Int = 12345

  val EC: ExecutionContext = ExecutionContext.Implicits.global

  implicit val logger: Logger[IO]                    = Slf4jLogger.getLogger[IO]
  protected implicit val ioRuntime: unsafe.IORuntime = unsafe.IORuntime.global

  implicit val persistenceService: PersistenceService[IO] = PersistenceService[IO]

  implicit private val pbHandler: ProtoHandler[IO]  = new ProtoHandler[IO]
  implicit private val avroHandler: AvroHandler[IO] = new AvroHandler[IO]
  implicit private val avroWithSchemaHandler: AvroWithSchemaHandler[IO] =
    new AvroWithSchemaHandler[IO]

  def grpcConfigs: Resource[IO, List[GrpcConfig]] = List(
    PersonServicePB.bindService[IO].map(AddService),
    PersonServiceAvro.bindService[IO].map(AddService),
    PersonServiceAvroWithSchema.bindService[IO].map(AddService)
  ).sequence

  implicit lazy val clientChannel: ManagedChannel =
    new ManagedChannelInterpreter[IO](
      ChannelForAddress("localhost", grpcPort),
      List(UsePlaintext())
    ).build.unsafeRunSync()

  def grpcServer: Resource[IO, GrpcServer[IO]] =
    grpcConfigs.evalMap(conf => GrpcServer.default[IO](grpcPort, conf))

  private var server: GrpcServer[IO]   = null
  private var shutdownServer: IO[Unit] = IO.unit

  def startServer(): Unit = {
    val allocated = grpcServer
      .evalTap { server =>
        for {
          _ <- logger.info("Starting server..")
          _ <- server.start
          _ <- logger.info("Server started..")
        } yield ()
      }
      .allocated
      .unsafeRunSync()
    this.server = server
    shutdownServer = allocated._2

  }

  def tearDown()(implicit channel: ManagedChannel): Unit =
    (for {
      _ <- logger.info("Stopping client..")
      _ <- IO(channel.shutdownNow())
      _ <- IO(channel.awaitTermination(1, TimeUnit.SECONDS))
      _ <- logger.info("Client Stopped..")
      _ <- logger.info("Stopping server..")
      _ <- server.shutdownNow
      _ <- server.awaitTerminationTimeout(1, TimeUnit.SECONDS)
      _ <- logger.info("Server Stopped..")
      _ <- shutdownServer
    } yield ()).unsafeRunSync()
}
