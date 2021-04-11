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

package higherkindness.mu.rpc.protocol

import cats.effect.{IO, Resource}
import cats.effect.kernel.Ref
import cats.syntax.applicativeError._
import natchez._

import java.net.URI

/*
 * A minimal Natchez tracing implementation that accumulates
 * trace data in a cats-effect Ref.
 */
object Tracing {

  case class TracingData(nextSpanId: Int, log: Vector[String]) {
    def append(entry: String): TracingData =
      copy(log = log :+ entry)

    def incrementNextSpanId: (TracingData, Int) =
      (copy(nextSpanId = nextSpanId + 1), nextSpanId)
  }

  case class TestSpan(id: Int, name: String, parentSpanId: Option[Int], ref: Ref[IO, TracingData])
      extends Span[IO] {

    override def toString: String =
      s"span $id [$name] (${parentSpanId.fold("root")(p => s"child of $p")})"

    def put(fields: (String, TraceValue)*): IO[Unit] =
      IO.unit // not implemented

    def kernel: IO[Kernel] =
      IO.pure(Kernel(Map("span-id" -> id.toString)))

    def span(name: String): Resource[IO, Span[IO]] =
      Resource.make(
        for {
          spanId <- ref.modify(_.incrementNextSpanId)
          span = TestSpan(spanId, name, parentSpanId = Some(id), ref)
          _ <- ref.update(_.append(s"Start $span"))
        } yield span
      )(span => ref.update(_.append(s"End $span")))

    override def traceId: IO[Option[String]] = IO.pure(Some(id.toString))

    override def traceUri: IO[Option[URI]] = IO.pure(None)

    override def spanId: IO[Option[String]] = IO.pure(Some(id.toString))
  }

  def entrypoint(ref: Ref[IO, TracingData]): EntryPoint[IO] =
    new EntryPoint[IO] {

      def root(name: String): Resource[IO, Span[IO]] =
        Resource.make(
          for {
            spanId <- ref.modify(_.incrementNextSpanId)
            span = TestSpan(spanId, name, parentSpanId = None, ref)
            _ <- ref.update(_.append(s"Start $span"))
          } yield span
        )(span => ref.update(_.append(s"End $span")))

      def continue(name: String, kernel: Kernel): Resource[IO, Span[IO]] =
        Resource.make(
          for {
            parentSpanId <- IO {
              kernel.toHeaders
                .get("span-id")
                .map(_.toInt)
                .getOrElse(throw new Exception("Required trace header not found!"))
            }
            spanId <- ref.modify(_.incrementNextSpanId)
            span = TestSpan(spanId, name, parentSpanId = Some(parentSpanId), ref)
            _ <- ref.update(_.append(s"Start $span"))
          } yield span
        )(span => ref.update(_.append(s"End $span")))

      def continueOrElseRoot(name: String, kernel: Kernel): Resource[IO, Span[IO]] =
        continue(name, kernel).recoverWith { case _: Exception =>
          root(name)
        }

    }

}
