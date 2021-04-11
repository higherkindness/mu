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

package higherkindness.mu.rpc.common

// import java.util.concurrent.TimeUnit

// import cats.effect.kernel.Ref
// import cats.effect.{Clock, Sync}
// import cats.syntax.flatMap._
// import cats.syntax.functor._

object util {

  // case class FakeClock[F[_]: Sync](step: Long, timeUnit: TimeUnit)(count: Ref[F, Long])
  //     extends Clock[F] {
  //   override def realTime(unit: TimeUnit): F[Long] =
  //     for {
  //       _     <- count.update(_ + step)
  //       value <- count.get
  //     } yield unit.convert(value, timeUnit)

  //   override def monotonic(unit: TimeUnit): F[Long] =
  //     for {
  //       _     <- count.update(_ + step)
  //       value <- count.get
  //     } yield unit.convert(value, timeUnit)
  // }

  // object FakeClock {
  //   def build[F[_]: Sync](
  //       step: Long = 50,
  //       timeUnit: TimeUnit = TimeUnit.NANOSECONDS
  //   ): F[FakeClock[F]] =
  //     Ref.of[F, Long](0).map(FakeClock(step, timeUnit))
  // }

}
