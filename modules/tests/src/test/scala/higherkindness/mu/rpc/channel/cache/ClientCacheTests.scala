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

package higherkindness.mu.rpc.channel.cache

import cats.effect.{Ref, _}
import cats.effect.unsafe.implicits._
import cats.implicits._
import fs2.Stream
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class ClientCacheTests extends AnyWordSpec with Matchers {

  private[this] val clockStep: Int = 50

  def compiledStream[H](
      ref1: Ref[IO, Int],
      ref2: Ref[IO, Int],
      keys: List[H],
      cleanUp: Int
  ): IO[Unit] =
    (for {
      keyRef <- Stream.eval(Ref.of[IO, List[H]](keys))
      clientCache <- ClientCache.impl[MyClient, IO, H](
        keyRef.modify(list => (list.tail, list.head)),
        _ =>
          ref1
            .update(_ + 1)
            .map(_ => (new MyClient[IO], ref2.update(_ + 1).void)),
        cleanUp.millis,
        cleanUp.millis
      )
      _ <- Stream.eval((1 to keys.length).toList.traverse_(_ => clientCache.getClient))
    } yield ()).compile.drain

  def test(numClients: Int, cleanUp: Int): (Int, Int) =
    (for {
      ref1         <- Ref.of[IO, Int](0)
      ref2         <- Ref.of[IO, Int](0)
      _            <- compiledStream(ref1, ref2, (1 to numClients).toList.map("node-0000" + _), cleanUp)
      numCreations <- ref1.get
      numCloses    <- ref2.get
    } yield (numCreations, numCloses)).unsafeRunSync()

  "ClientCache.impl" should {

    "create the client and close after one use when the clean up time is lower than the elapsed time" in {
      test(3, clockStep - 10) shouldBe ((3, 3))
    }

    "create the client and close after one use when the clean up time is greater than the elapsed time" in {
      test(5, clockStep + 10) shouldBe ((5, 5))
    }

    "use the provided clients and close them" in {
      test(2, clockStep) shouldBe ((2, 2))
    }

  }

}

class MyClient[F[_]]
