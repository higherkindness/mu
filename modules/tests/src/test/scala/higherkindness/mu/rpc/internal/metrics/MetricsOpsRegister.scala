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

package higherkindness.mu.rpc.internal.metrics

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.apply._
import higherkindness.mu.rpc.internal.interceptors.GrpcMethodInfo
import io.grpc.Status

case class MetricsOpsRegister(
    increaseActiveCallsReg: Ref[IO, List[(GrpcMethodInfo, Option[String])]],
    decreaseActiveCallsReg: Ref[IO, List[(GrpcMethodInfo, Option[String])]],
    recordMessageSentReg: Ref[IO, List[(GrpcMethodInfo, Option[String])]],
    recordMessageReceivedReg: Ref[IO, List[(GrpcMethodInfo, Option[String])]],
    recordHeadersTimeReg: Ref[IO, List[(GrpcMethodInfo, Long, Option[String])]],
    recordTotalTimeReg: Ref[IO, List[(GrpcMethodInfo, Status, Long, Option[String])]]
) extends MetricsOps[IO] {

  def increaseActiveCalls(methodInfo: GrpcMethodInfo, classifier: Option[String]): IO[Unit] =
    increaseActiveCallsReg.update(_ :+ ((methodInfo, classifier)))
  def decreaseActiveCalls(methodInfo: GrpcMethodInfo, classifier: Option[String]): IO[Unit] =
    decreaseActiveCallsReg.update(_ :+ ((methodInfo, classifier)))
  def recordMessageSent(methodInfo: GrpcMethodInfo, classifier: Option[String]): IO[Unit] =
    recordMessageSentReg.update(_ :+ ((methodInfo, classifier)))
  def recordMessageReceived(methodInfo: GrpcMethodInfo, classifier: Option[String]): IO[Unit] =
    recordMessageReceivedReg.update(_ :+ ((methodInfo, classifier)))
  def recordHeadersTime(
      methodInfo: GrpcMethodInfo,
      elapsed: Long,
      classifier: Option[String]
  ): IO[Unit] =
    recordHeadersTimeReg.update(_ :+ ((methodInfo, elapsed, classifier)))
  def recordTotalTime(
      methodInfo: GrpcMethodInfo,
      status: Status,
      elapsed: Long,
      classifier: Option[String]
  ): IO[Unit] =
    recordTotalTimeReg.update(_ :+ ((methodInfo, status, elapsed, classifier)))
}

object MetricsOpsRegister {
  def build: IO[MetricsOpsRegister] =
    (
      Ref.of[IO, List[(GrpcMethodInfo, Option[String])]](Nil),
      Ref.of[IO, List[(GrpcMethodInfo, Option[String])]](Nil),
      Ref.of[IO, List[(GrpcMethodInfo, Option[String])]](Nil),
      Ref.of[IO, List[(GrpcMethodInfo, Option[String])]](Nil),
      Ref.of[IO, List[(GrpcMethodInfo, Long, Option[String])]](Nil),
      Ref.of[IO, List[(GrpcMethodInfo, Status, Long, Option[String])]](Nil)
    ).mapN(MetricsOpsRegister.apply)
}
