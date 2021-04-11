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

package higherkindness.mu.rpc.channel.metrics

import cats.effect.{IO, Resource}
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import higherkindness.mu.rpc.common.{A => _, _}
import higherkindness.mu.rpc.internal.interceptors.GrpcMethodInfo
import higherkindness.mu.rpc.internal.metrics.{MetricsOps, MetricsOpsRegister}
import higherkindness.mu.rpc.protocol._
import higherkindness.mu.rpc.testing.servers._
import io.grpc.MethodDescriptor.MethodType
import io.grpc.Status
import org.scalatest.Assertion

class MonitoringChannelInterceptorTests extends RpcBaseTestSuite {

  import services._

  val myClassifier: Option[String] = Some("MyClassifier")

  "MonitoringChannelInterceptor" should {

    "generate the right metrics with proto" in {
      (for {
        metricsOps <- MetricsOpsRegister.build
        _          <- makeProtoCalls(metricsOps)(_.serviceOp1(Request()))(protoRPCServiceImpl)
        assertion  <- checkCalls(metricsOps, List(serviceOp1Info))
      } yield assertion).unsafeRunSync()
    }

    "generate the right metrics when calling multiple methods with proto" in {
      (for {
        metricsOps <- MetricsOpsRegister.build
        _ <- makeProtoCalls(metricsOps) { client =>
          client.serviceOp1(Request()) *> client.serviceOp2(Request())
        }(protoRPCServiceImpl)
        assertion <- checkCalls(metricsOps, List(serviceOp1Info, serviceOp2Info))
      } yield assertion).unsafeRunSync()
    }

    "generate the right metrics with proto when the server returns an error" in {
      (for {
        metricsOps <- MetricsOpsRegister.build
        _          <- makeProtoCalls(metricsOps)(_.serviceOp1(Request()))(protoRPCServiceErrorImpl)
        assertion  <- checkCalls(metricsOps, List(serviceOp1Info), serverError = true)
      } yield assertion).unsafeRunSync()
    }

  }

  private[this] def makeProtoCalls[A](metricsOps: MetricsOps[IO])(
      f: ProtoRPCService[IO] => IO[A]
  )(implicit H: ProtoRPCService[IO]): IO[Either[Throwable, A]] = {
    (for {
      dispatcher <- Dispatcher[IO]
      serverChannel <- withServerChannel[IO](
        service = ProtoRPCService.bindService[IO](dispatcher),
        clientInterceptor = Some(MetricsChannelInterceptor(metricsOps, myClassifier, dispatcher))
      )
      client <- createClient(serverChannel)
    } yield client).use(f).attempt
  }

  private[this] def checkCalls(
      metricsOps: MetricsOpsRegister,
      methodCalls: List[GrpcMethodInfo],
      serverError: Boolean = false
  ): IO[Assertion] = {
    for {
      incArgs     <- metricsOps.increaseActiveCallsReg.get
      sentArgs    <- metricsOps.recordMessageSentReg.get
      recArgs     <- metricsOps.recordMessageReceivedReg.get
      headersArgs <- metricsOps.recordHeadersTimeReg.get
      totalArgs   <- metricsOps.recordTotalTimeReg.get
      decArgs     <- metricsOps.decreaseActiveCallsReg.get
    } yield {

      val argList: List[(GrpcMethodInfo, Option[String])] = methodCalls.map((_, myClassifier))

      // Increase Active Calls
      incArgs should contain theSameElementsAs argList
      // Messages Sent
      sentArgs should contain theSameElementsAs argList
      // Messages Received
      if (!serverError) recArgs should contain theSameElementsAs argList
      // Decrease Active Calls
      decArgs should contain theSameElementsAs argList
      // Headers Time
      if (!serverError) {
        headersArgs.map(_._1) should contain theSameElementsAs methodCalls
        headersArgs.map(_._2) shouldBe List.fill(methodCalls.size)(50)
        headersArgs.map(_._3) should contain theSameElementsAs argList.map(_._2)
      }
      // Total Time
      totalArgs.map(_._1) should contain theSameElementsAs methodCalls
      if (serverError) {
        totalArgs.map(_._2.getCode) shouldBe List.fill(methodCalls.size)(Status.INTERNAL.getCode)
        totalArgs.map(_._3) shouldBe List.fill(methodCalls.size)(50)
      } else {
        totalArgs.map(_._2) shouldBe List.fill(methodCalls.size)(Status.OK)
        totalArgs.map(_._3) shouldBe List.fill(methodCalls.size)(100)
      }
      totalArgs.map(_._4) should contain theSameElementsAs argList.map(_._2)
    }
  }
}

object services {

  final case class Request()
  final case class Response()

  val error: Throwable = new RuntimeException("BOOM!")

  @service(Protobuf) trait ProtoRPCService[F[_]] {
    def serviceOp1(r: Request): F[Response]
    def serviceOp2(r: Request): F[Response]
  }

  val serviceOp1Info: GrpcMethodInfo =
    GrpcMethodInfo("ProtoRPCService", "ProtoRPCService/serviceOp1", "serviceOp1", MethodType.UNARY)

  val serviceOp2Info: GrpcMethodInfo =
    GrpcMethodInfo("ProtoRPCService", "ProtoRPCService/serviceOp2", "serviceOp2", MethodType.UNARY)

  val protoRPCServiceImpl: ProtoRPCService[IO] = new ProtoRPCService[IO] {
    def serviceOp1(r: Request): IO[Response] = IO(Response())
    def serviceOp2(r: Request): IO[Response] = IO(Response())
  }

  val protoRPCServiceErrorImpl: ProtoRPCService[IO] = new ProtoRPCService[IO] {
    def serviceOp1(r: Request): IO[Response] = IO.raiseError(error)
    def serviceOp2(r: Request): IO[Response] = IO.raiseError(error)
  }

  def createClient(sc: ServerChannel): Resource[IO, ProtoRPCService[IO]] =
    ProtoRPCService.clientFromChannel[IO](IO(sc.channel))

}
