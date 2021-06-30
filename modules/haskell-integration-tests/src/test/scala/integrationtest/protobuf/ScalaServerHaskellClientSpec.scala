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

import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import higherkindness.mu.rpc.server.{AddService, GrpcServer}
import integrationtest._
import integrationtest.protobuf.weather._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class ScalaServerHaskellClientSpec
    extends AnyFlatSpec
    with RunHaskellClientInDocker
    with BeforeAndAfterAll {

  def clientExecutableName: String = "protobuf-client"

  implicit val service: WeatherService[IO] = new MyWeatherService[IO]

  private val startServer: Resource[IO, Unit] = for {
    serviceDef <- WeatherService.bindService[IO]
    _          <- GrpcServer.defaultServer[IO](Constants.ProtobufPort, List(AddService(serviceDef)))
  } yield ()

  private var cancelToken: IO[Unit] = IO.unit

  override def beforeAll(): Unit = {
    cancelToken = startServer.allocated.unsafeRunSync()._2
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
