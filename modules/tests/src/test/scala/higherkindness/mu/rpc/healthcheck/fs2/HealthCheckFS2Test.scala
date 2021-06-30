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

package higherkindness.mu.rpc.healthcheck.fs2

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import higherkindness.mu.rpc.healthcheck.fs2.handler.HealthServiceFS2
import higherkindness.mu.rpc.healthcheck.unary.handler.{HealthCheck, HealthStatus, ServerStatus}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import fs2.Stream

class HealthCheckFS2Test extends AsyncWordSpec with Matchers {

  "FS2 health check service" should {

    val handler = HealthServiceFS2.buildInstance[IO]
    val hc      = new HealthCheck("example")

    "work with setStatus and watch" in {
      {
        for {
          hand <- handler
          stream1 = Stream.force(hand.watch(hc))
          v1 <- stream1.take(1).compile.toList.start
          status = HealthStatus(hc, ServerStatus("NOT_SERVING"))
          _       <- hand.setStatus(status)
          outcome <- v1.join
          result  <- outcome.embed(onCancel = IO(fail("Somehow canceled")))
        } yield result shouldBe List(status)
      }.unsafeToFuture()
    }
  }
}
