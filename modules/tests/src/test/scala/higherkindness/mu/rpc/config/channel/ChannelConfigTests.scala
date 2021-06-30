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

package higherkindness.mu.rpc.config.channel

import cats.effect.IO
import higherkindness.mu.rpc.channel.RpcClientTestSuite
import higherkindness.mu.rpc.common.SC
import higherkindness.mu.rpc.{ChannelForAddress, ChannelForTarget}

class ChannelConfigTests extends RpcClientTestSuite {

  "ChannelConfig" should {

    "for Address [host, port] work as expected" in {
      ConfigForAddress[IO](
        SC.host,
        SC.port.toString
      ).unsafeRunSync() shouldBe ChannelForAddress(
        "localhost",
        50051
      )
    }

    "for Target work as expected" in {

      ConfigForTarget[IO](SC.host).unsafeRunSync() shouldBe ChannelForTarget("target")
    }

  }

}
