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

package higherkindness.mu.http

import cats.{Applicative, ApplicativeError, ApplicativeThrow}
import cats.implicits._
import cats.syntax.either._
import fs2.{RaiseThrowable, Stream}
import io.grpc.Status.Code._
import org.typelevel.jawn.ParseException
import io.circe._
import io.circe.jawn.CirceSupportParser.facade
import io.circe.syntax._
import io.grpc.{Status => _, _}
import org.http4s._
import org.http4s.dsl.Http4sDsl
import org.http4s.Status.Ok
import org.typelevel.jawn.fs2._

import scala.util.control.NoStackTrace

object implicits {

  implicit val unexpectedErrorEncoder: Encoder[UnexpectedError] = new Encoder[UnexpectedError] {
    final def apply(a: UnexpectedError): Json =
      Json.obj(
        ("className", Json.fromString(a.className)),
        ("msg", a.msg.fold(Json.Null)(s => Json.fromString(s)))
      )
  }

  implicit val unexpectedErrorDecoder: Decoder[UnexpectedError] = new Decoder[UnexpectedError] {
    final def apply(c: HCursor): Decoder.Result[UnexpectedError] =
      for {
        className <- c.downField("className").as[String]
        msg       <- c.downField("msg").as[Option[String]]
      } yield UnexpectedError(className, msg)
  }

  implicit def EitherDecoder[A, B](implicit a: Decoder[A], b: Decoder[B]): Decoder[Either[A, B]] =
    a.map(Left.apply) or b.map(Right.apply)

  implicit def EitherEncoder[A, B](implicit ea: Encoder[A], eb: Encoder[B]): Encoder[Either[A, B]] =
    new Encoder[Either[A, B]] {
      final def apply(a: Either[A, B]): Json = a.fold(_.asJson, _.asJson)
    }

  implicit class MessageOps[F[_]](private val message: Message[F]) extends AnyVal {

    def jsonBodyAsStream[A](implicit
        decoder: Decoder[A],
        F: ApplicativeError[F, Throwable]
    ): Stream[F, A] =
      message.body.chunks.unwrapJsonArray.map(_.as[A]).rethrow
  }

  implicit class RequestOps[F[_]](private val request: Request[F]) {

    def asStream[A](implicit decoder: Decoder[A], F: ApplicativeError[F, Throwable]): Stream[F, A] =
      request
        .jsonBodyAsStream[A]
        .adaptError { // mimic behavior of MessageOps.as[T] in handling of parsing errors
          case ex: ParseException =>
            MalformedMessageBodyFailure(ex.getMessage, Some(ex)) // will return 400 instead of 500
        }
  }

  implicit class ResponseOps[F[_]](private val response: Response[F]) {

    def asStream[A](implicit
        decoder: Decoder[A],
        F: ApplicativeError[F, Throwable],
        R: RaiseThrowable[F]
    ): Stream[F, A] =
      if (response.status.code != Ok.code) Stream.raiseError(ResponseError(response.status))
      else response.jsonBodyAsStream[Either[UnexpectedError, A]].rethrow
  }

  implicit class Fs2StreamOps[F[_], A](private val stream: Stream[F, A]) {

    def asJsonEither(implicit encoder: Encoder[A]): Stream[F, Json] =
      stream.attempt.map(_.bimap(_.toUnexpected, identity).asJson)
  }

  implicit class FResponseOps[F[_]](private val response: F[Response[F]]) extends Http4sDsl[F] {

    def adaptErrors(implicit F: ApplicativeThrow[F]): F[Response[F]] =
      response.handleErrorWith {
        case se: StatusException         => errorFromStatus(se.getStatus, se.getMessage)
        case sre: StatusRuntimeException => errorFromStatus(sre.getStatus, sre.getMessage)
        case other: Throwable            => InternalServerError(other.getMessage)
      }

    private def errorFromStatus(status: io.grpc.Status, message: String)(implicit
        F: Applicative[F]
    ): F[Response[F]] =
      status.getCode match {
        case INVALID_ARGUMENT  => BadRequest(message)
        case UNAUTHENTICATED   => Forbidden(message)
        case PERMISSION_DENIED => Forbidden(message)
        case NOT_FOUND         => NotFound(message)
        case UNAVAILABLE       => ServiceUnavailable(message)
        case _                 => InternalServerError(message)
      }
  }

  def handleResponseError[F[_]: RaiseThrowable: fs2.Compiler.Target](
      errorResponse: Response[F]
  ): F[Throwable] =
    errorResponse.bodyText.compile.foldMonoid.map(body =>
      ResponseError(errorResponse.status, Some(body).filter(_.nonEmpty))
    )

  implicit class ThrowableOps(self: Throwable) {
    def toUnexpected: UnexpectedError =
      UnexpectedError(self.getClass.getName, Option(self.getMessage))
  }

}

final case class UnexpectedError(className: String, msg: Option[String])
    extends RuntimeException(className + msg.fold("")(": " + _))
    with NoStackTrace

final case class ResponseError(status: Status, msg: Option[String] = None)
    extends RuntimeException(status.toString + msg.fold("")(": " + _))
    with NoStackTrace
