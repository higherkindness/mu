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

package integrationtest.protobuf

import integrationtest._
import weather._
import higherkindness.mu.rpc.server.{AddService, GrpcServer}

import cats.effect.IO

import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.ExecutionContext
import org.scalatest.BeforeAndAfterAll

class ScalaServerHaskellClientSpec
    extends AnyFlatSpec
    with RunHaskellClientInDocker
    with BeforeAndAfterAll {

  def clientExecutableName: String = "protobuf-client"

  implicit val CS: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  implicit val service: WeatherService[IO] = new MyWeatherService[IO]

  private val startServer: IO[Unit] = for {
    serviceDef <- WeatherService.bindService[IO]
    serverDef  <- GrpcServer.default[IO](Constants.ProtobufPort, List(AddService(serviceDef)))
    _          <- GrpcServer.server[IO](serverDef)
  } yield ()

  private var cancelToken: IO[Unit] = IO.unit

  override def beforeAll(): Unit = {
    cancelToken = startServer.unsafeRunCancelable {
      case Left(e) =>
        println(s"Server failed! $e")
      case Right(_) =>
        println("Server completed (this should never happen)")
    }
    Thread.sleep(500) // give the server a chance to start up
  }

  override def afterAll(): Unit =
    // stop the server
    cancelToken.unsafeRunSync()

  behavior of "Mu-Scala server and Mu-Haskell client communication using Protobuf"

  it should "work for a trivial unary call" in {
    val clientOutput = runHaskellClient(List("ping"))
    assert(clientOutput == "pong")
  }

  it should "work for a unary call" in {
    val clientOutput = runHaskellClient(List("get-forecast", "London", "3"))
    assert(
      clientOutput == """2020-03-20T12:00:00Z ["SUNNY","SUNNY","SUNNY"]"""
    )
  }

  it should "work for a client-streaming call" in {
    val clientOutput = runHaskellClient(List("publish-rain-events", "London"))
    assert(clientOutput == "It started raining 3 times")
  }

  it should "work for a server-streaming call" in {
    val clientOutput = runHaskellClient(List("subscribe-to-rain-events", "London"))
    assert(
      clientOutput ==
        """|"STARTED"
         |"STOPPED"
         |"STARTED"
         |"STOPPED"
         |"STARTED"""".stripMargin
    )
  }

}
