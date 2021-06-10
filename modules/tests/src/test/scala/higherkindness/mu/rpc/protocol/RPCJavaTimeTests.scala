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

import java.time._
import cats.Applicative
import cats.effect.IO
import cats.syntax.applicative._
import com.fortysevendeg.scalacheck.datetime.instances.jdk8._
import com.fortysevendeg.scalacheck.datetime.GenDateTime._
import com.fortysevendeg.scalacheck.datetime.jdk8.granularity.seconds
import higherkindness.mu.rpc.common._
import higherkindness.mu.rpc.internal.encoders.avro.javatime._
import higherkindness.mu.rpc.internal.encoders.avro.javatime.marshallers._
import higherkindness.mu.rpc.internal.encoders.pbd.javatime._
import higherkindness.mu.rpc.protocol.Utils._
import org.scalacheck.Arbitrary
import org.scalatest._
import org.scalacheck.Prop._
import org.scalatestplus.scalacheck.Checkers

class RPCJavaTimeTests
    extends RpcBaseTestSuite
    with OneInstancePerTest
    with BeforeAndAfterAll
    with Checkers {

  object RPCDateService {

    case class Request(date: LocalDate, dateTime: LocalDateTime, instant: Instant, label: String)

    case class Response(
        date: LocalDate,
        dateTime: LocalDateTime,
        instant: Instant,
        result: String,
        check: Boolean
    )

    @service(Protobuf)
    trait ProtoRPCDateServiceDef[F[_]] {
      def localDateProto(date: LocalDate): F[LocalDate]
      def localDateTimeProto(dateTime: LocalDateTime): F[LocalDateTime]
      def instantProto(instant: Instant): F[Instant]
      def dateProtoWrapper(req: Request): F[Response]
    }

    @service(Avro)
    trait AvroRPCDateServiceDef[F[_]] {
      def localDateAvro(date: LocalDate): F[LocalDate]
      def localDateTimeAvro(dateTime: LocalDateTime): F[LocalDateTime]
      def instantAvro(instant: Instant): F[Instant]
      def dateAvroWrapper(req: Request): F[Response]
    }

    @service(AvroWithSchema)
    trait AvroWithSchemaRPCDateServiceDef[F[_]] {
      def localDateAvroWithSchema(date: LocalDate): F[LocalDate]
      def localDateTimeAvroWithSchema(dateTime: LocalDateTime): F[LocalDateTime]
      def instantAvroWithSchema(instant: Instant): F[Instant]
      def dateAvroWrapperWithSchema(req: Request): F[Response]
    }

    class RPCDateServiceDefImpl[F[_]: Applicative]
        extends ProtoRPCDateServiceDef[F]
        with AvroRPCDateServiceDef[F]
        with AvroWithSchemaRPCDateServiceDef[F] {

      def localDateProto(date: LocalDate): F[LocalDate]                 = date.pure
      def localDateTimeProto(dateTime: LocalDateTime): F[LocalDateTime] = dateTime.pure
      def instantProto(instant: Instant): F[Instant]                    = instant.pure
      def dateProtoWrapper(req: Request): F[Response] =
        Response(req.date, req.dateTime, req.instant, req.label, check = true).pure
      def localDateAvro(date: LocalDate): F[LocalDate]                 = date.pure
      def localDateTimeAvro(dateTime: LocalDateTime): F[LocalDateTime] = dateTime.pure
      def instantAvro(instant: Instant): F[Instant]                    = instant.pure
      def dateAvroWrapper(req: Request): F[Response] =
        Response(req.date, req.dateTime, req.instant, req.label, check = true).pure
      def localDateAvroWithSchema(date: LocalDate): F[LocalDate]                 = date.pure
      def localDateTimeAvroWithSchema(dateTime: LocalDateTime): F[LocalDateTime] = dateTime.pure
      def instantAvroWithSchema(instant: Instant): F[Instant]                    = instant.pure
      def dateAvroWrapperWithSchema(req: Request): F[Response] =
        Response(req.date, req.dateTime, req.instant, req.label, check = true).pure
    }

  }

  "A RPC server" should {

    import RPCDateService._

    implicit val H: RPCDateServiceDefImpl[IO] =
      new RPCDateServiceDefImpl[IO]

    val from: ZonedDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    val range: Duration     = Duration.ofDays(365 * 200)

    "be able to serialize and deserialize LocalDate using proto format" in {

      withClient(
        ProtoRPCDateServiceDef.bindService[IO],
        ProtoRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range)) { zdt: ZonedDateTime =>
            val date = zdt.toLocalDate
            client.localDateProto(date).unsafeRunSync() == date
          }
        }
      }

    }

    "be able to serialize and deserialize LocalDateTime using proto format" in {

      withClient(
        ProtoRPCDateServiceDef.bindService[IO],
        ProtoRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range)) { zdt: ZonedDateTime =>
            val dateTime = zdt.toLocalDateTime
            client.localDateTimeProto(dateTime).unsafeRunSync() == dateTime
          }
        }
      }

    }

    "be able to serialize and deserialize Instant using proto format" in {

      withClient(
        ProtoRPCDateServiceDef.bindService[IO],
        ProtoRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range)) { zdt: ZonedDateTime =>
            val instant = zdt.toInstant
            client.instantProto(instant).unsafeRunSync() == instant
          }
        }
      }

    }

    "be able to serialize and deserialize LocalDate, LocalDateTime, and Instant in a Request using proto format" in {

      withClient(
        ProtoRPCDateServiceDef.bindService[IO],
        ProtoRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range), Arbitrary.arbitrary[String]) {
            (zdt: ZonedDateTime, s: String) =>
              val date     = zdt.toLocalDate
              val dateTime = zdt.toLocalDateTime
              val instant  = zdt.toInstant
              client
                .dateProtoWrapper(Request(date, dateTime, instant, s))
                .unsafeRunSync() == Response(date, dateTime, instant, s, check = true)
          }
        }

      }

    }

    "be able to serialize and deserialize LocalDate using avro format" in {

      withClient(
        AvroRPCDateServiceDef.bindService[IO],
        AvroRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range)) { zdt: ZonedDateTime =>
            val date = zdt.toLocalDate
            client.localDateAvro(date).unsafeRunSync() == date
          }
        }

      }

    }

    "be able to serialize and deserialize LocalDateTime using avro format" in {

      withClient(
        AvroRPCDateServiceDef.bindService[IO],
        AvroRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range)) { zdt: ZonedDateTime =>
            val dateTime = zdt.toLocalDateTime
            client.localDateTimeAvro(dateTime).unsafeRunSync() == dateTime
          }
        }
      }

    }

    "be able to serialize and deserialize Instant using avro format" in {

      withClient(
        AvroRPCDateServiceDef.bindService[IO],
        AvroRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range)) { zdt: ZonedDateTime =>
            val instant = zdt.toInstant
            client.instantAvro(instant).unsafeRunSync() == instant
          }
        }

      }

    }

    "be able to serialize and deserialize LocalDate, LocalDateTime, and Instant in a Request using avro format" in {

      withClient(
        AvroRPCDateServiceDef.bindService[IO],
        AvroRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range), Arbitrary.arbitrary[String]) {
            (zdt: ZonedDateTime, s: String) =>
              val date     = zdt.toLocalDate
              val dateTime = zdt.toLocalDateTime
              val instant  = zdt.toInstant
              client
                .dateAvroWrapper(Request(date, dateTime, instant, s))
                .unsafeRunSync() == Response(date, dateTime, instant, s, check = true)
          }
        }

      }

    }

    "be able to serialize and deserialize LocalDate using avro format with schema" in {

      withClient(
        AvroWithSchemaRPCDateServiceDef.bindService[IO],
        AvroWithSchemaRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range)) { zdt: ZonedDateTime =>
            val date = zdt.toLocalDate
            client.localDateAvroWithSchema(date).unsafeRunSync() == date
          }
        }

      }

    }

    "be able to serialize and deserialize LocalDateTime using avro format with schema" in {

      withClient(
        AvroWithSchemaRPCDateServiceDef.bindService[IO],
        AvroWithSchemaRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range)) { zdt: ZonedDateTime =>
            val dateTime = zdt.toLocalDateTime
            client.localDateTimeAvroWithSchema(dateTime).unsafeRunSync() == dateTime
          }
        }

      }

    }

    "be able to serialize and deserialize Instant using avro format with schema" in {

      withClient(
        AvroWithSchemaRPCDateServiceDef.bindService[IO],
        AvroWithSchemaRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range)) { zdt: ZonedDateTime =>
            val instant = zdt.toInstant
            client.instantAvroWithSchema(instant).unsafeRunSync() == instant
          }
        }

      }

    }

    "be able to serialize and deserialize LocalDate, LocalDateTime, and Instant in a Request using avro format with schema" in {

      withClient(
        AvroWithSchemaRPCDateServiceDef.bindService[IO],
        AvroWithSchemaRPCDateServiceDef.clientFromChannel[IO](_)
      ) { client =>
        check {
          forAll(genDateTimeWithinRange(from, range), Arbitrary.arbitrary[String]) {
            (zdt: ZonedDateTime, s: String) =>
              val date     = zdt.toLocalDate
              val dateTime = zdt.toLocalDateTime
              val instant  = zdt.toInstant
              client
                .dateAvroWrapperWithSchema(Request(date, dateTime, instant, s))
                .unsafeRunSync() == Response(date, dateTime, instant, s, check = true)
          }
        }

      }

    }

  }

}
