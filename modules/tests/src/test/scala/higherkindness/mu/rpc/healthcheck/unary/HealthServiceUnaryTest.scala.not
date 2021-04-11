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

package higherkindness.mu.rpc.healthcheck.unary

import cats.effect.IO
import higherkindness.mu.rpc.healthcheck.unary.handler._
import higherkindness.mu.rpc.protocol.Empty
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HealthServiceUnaryTest extends AnyWordSpec with Matchers {

  "Unary health check service" should {

    val handler = HealthService.buildInstance[IO]
    val hc      = new HealthCheck("example")
    val hc1     = new HealthCheck("example1")
    val hc2     = new HealthCheck("example2")

    "work with setStatus and check I" in {
      {
        for {
          hand         <- handler
          firstStatus  <- hand.check(hc)
          _            <- hand.setStatus(HealthStatus(hc, ServerStatus("SERVING")))
          secondStatus <- hand.check(hc)
        } yield (firstStatus, secondStatus)
      }.unsafeRunSync() shouldBe ((ServerStatus("UNKNOWN"), ServerStatus("SERVING")))
    }

    "work with  setStatus and check II" in {
      {
        for {
          hand         <- handler
          firstStatus  <- hand.check(hc)
          _            <- hand.setStatus(HealthStatus(hc, ServerStatus("SERVING")))
          _            <- hand.setStatus(HealthStatus(hc, ServerStatus("NOT_SERVING")))
          secondStatus <- hand.check(hc)
        } yield (firstStatus, secondStatus)
      }.unsafeRunSync() shouldBe ((ServerStatus("UNKNOWN"), ServerStatus("NOT_SERVING")))
    }

    "work with clearStatus and check I" in {
      {
        for {
          hand         <- handler
          _            <- hand.setStatus(HealthStatus(hc, ServerStatus("SERVING")))
          firstStatus  <- hand.check(hc)
          _            <- hand.clearStatus(hc)
          secondStatus <- hand.check(hc)
        } yield (firstStatus, secondStatus)
      }.unsafeRunSync() shouldBe ((ServerStatus("SERVING"), ServerStatus("UNKNOWN")))
    }

    "work with clearStatus and check II" in {
      {
        for {
          hand         <- handler
          firstStatus  <- hand.check(hc)
          _            <- hand.clearStatus(hc)
          _            <- hand.clearStatus(hc)
          secondStatus <- hand.check(hc)
        } yield (firstStatus, secondStatus)
      }.unsafeRunSync() shouldBe ((ServerStatus("UNKNOWN"), ServerStatus("UNKNOWN")))
    }

    "work with checkAll I" in {
      {
        for {
          hand   <- handler
          _      <- hand.setStatus(HealthStatus(hc1, ServerStatus("SERVING")))
          _      <- hand.setStatus(HealthStatus(hc2, ServerStatus("SERVING")))
          status <- hand.checkAll(Empty)
        } yield status
      }.unsafeRunSync() shouldBe AllStatus(
        List(HealthStatus(hc1, ServerStatus("SERVING")), HealthStatus(hc2, ServerStatus("SERVING")))
      )
    }

    "work with cleanAll I" in {
      {
        for {
          hand      <- handler
          _         <- hand.setStatus(HealthStatus(hc1, ServerStatus("SERVING")))
          _         <- hand.setStatus(HealthStatus(hc2, ServerStatus("SERVING")))
          statusIni <- hand.checkAll(Empty)
          _         <- hand.cleanAll(Empty)
          statusEnd <- hand.checkAll(Empty)
        } yield List(statusIni, statusEnd)
      }.unsafeRunSync() shouldBe List(
        AllStatus(
          List(
            HealthStatus(hc1, ServerStatus("SERVING")),
            HealthStatus(hc2, ServerStatus("SERVING"))
          )
        ),
        AllStatus(
          List.empty
        )
      )
    }

  }

}
