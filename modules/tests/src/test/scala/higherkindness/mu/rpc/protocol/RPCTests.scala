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
package protocol

import cats.effect.IO
import org.scalatest._
import higherkindness.mu.rpc.common._
import higherkindness.mu.rpc.protocol.Utils.handlers.client._
import higherkindness.mu.rpc.server._

class RPCTests extends RpcBaseTestSuite with BeforeAndAfterAll {

  import higherkindness.mu.rpc.protocol.Utils.client.MyRPCClient
  import higherkindness.mu.rpc.protocol.Utils.database._
  import higherkindness.mu.rpc.protocol.Utils.implicits._

  private var S: GrpcServer[IO]  = null
  private var shutdown: IO[Unit] = IO.unit

  override protected def beforeAll(): Unit = {
    val allocated = grpcServer.allocated.unsafeRunSync()
    S = allocated._1
    shutdown = allocated._2
  }

  override protected def afterAll(): Unit =
    shutdown.unsafeRunSync()

  "mu server" should {

    "allow to startup a server and check if it's alive" in {
      S.isShutdown.unsafeRunSync() shouldBe false
    }

    "allow to get the port where it's running" in {
      S.getPort.unsafeRunSync() shouldBe SC.port
    }

  }

  "mu client" should {

    implicit val muRPCServiceClientHandler: MuRPCServiceClientHandler[IO] =
      new MuRPCServiceClientHandler[IO](
        protoRPCServiceClient,
        avroRPCServiceClient,
        awsRPCServiceClient
      )

    "be able to run unary services" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[C] =
        APP.u(a1.x, a1.y)

      clientProgram[IO].unsafeRunSync() shouldBe c1

    }

    "be able to invoke services with empty requests" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.empty

      clientProgram[IO].unsafeRunSync() shouldBe Empty

    }

    "#71 issue - empty for avro" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.emptyAvro

      clientProgram[IO].unsafeRunSync() shouldBe Empty
    }

    "empty for avro with schema" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.emptyAvroWithSchema

      clientProgram[IO].unsafeRunSync() shouldBe Empty
    }

    "#71 issue - empty response with one param for avro" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.emptyAvroParam(a4)

      clientProgram[IO].unsafeRunSync() shouldBe Empty
    }

    "empty response with one param for avro with schema" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.emptyAvroWithSchemaParam(a4)

      clientProgram[IO].unsafeRunSync() shouldBe Empty
    }

    "#71 issue - response with empty params for avro" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[A] =
        APP.emptyAvroParamResponse

      clientProgram[IO].unsafeRunSync() shouldBe a4

    }

    "response with empty params for avro with schema" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[A] =
        APP.emptyAvroWithSchemaParamResponse

      clientProgram[IO].unsafeRunSync() shouldBe a4

    }

    "#71 issue - empty response with one param for proto" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.emptyParam(a4)

      clientProgram[IO].unsafeRunSync() shouldBe Empty
    }

    "#71 issue - response with empty params for proto" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[A] =
        APP.emptyParamResponse

      clientProgram[IO].unsafeRunSync() shouldBe a4

    }

  }

  "mu client with compression" should {

    implicit val muRPCServiceClientHandler: MuRPCServiceClientCompressedHandler[IO] =
      new MuRPCServiceClientCompressedHandler[IO](
        compressedprotoRPCServiceClient,
        compressedavroRPCServiceClient,
        compressedawsRPCServiceClient
      )

    "be able to run unary services" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[C] =
        APP.u(a1.x, a1.y)

      clientProgram[IO].unsafeRunSync() shouldBe c1

    }

    "be able to invoke services with empty requests" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.empty

      clientProgram[IO].unsafeRunSync() shouldBe Empty

    }

    "#71 issue - empty for avro" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.emptyAvro

      clientProgram[IO].unsafeRunSync() shouldBe Empty
    }

    "empty for avro with schema" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.emptyAvroWithSchema

      clientProgram[IO].unsafeRunSync() shouldBe Empty
    }

    "#71 issue - empty response with one param for avro" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.emptyAvroParam(a4)

      clientProgram[IO].unsafeRunSync() shouldBe Empty
    }

    "empty response with one param for avro with schema" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.emptyAvroWithSchemaParam(a4)

      clientProgram[IO].unsafeRunSync() shouldBe Empty
    }

    "#71 issue - response with empty params for avro" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[A] =
        APP.emptyAvroParamResponse

      clientProgram[IO].unsafeRunSync() shouldBe a4

    }

    "response with empty params for avro with schema" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[A] =
        APP.emptyAvroWithSchemaParamResponse

      clientProgram[IO].unsafeRunSync() shouldBe a4

    }

    "#71 issue - empty response with one param for proto" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[Empty.type] =
        APP.emptyParam(a4)

      clientProgram[IO].unsafeRunSync() shouldBe Empty
    }

    "#71 issue - response with empty params for proto" in {

      def clientProgram[F[_]](implicit APP: MyRPCClient[F]): F[A] =
        APP.emptyParamResponse

      clientProgram[IO].unsafeRunSync() shouldBe a4

    }

  }

}
