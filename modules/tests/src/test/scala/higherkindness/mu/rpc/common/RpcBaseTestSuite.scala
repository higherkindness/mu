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

import cats.data.Kleisli
import org.scalactic.Prettifier
import org.scalamock.scalatest.MockFactory
import scala.io._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import cats.effect.unsafe

trait RpcBaseTestSuite extends AnyWordSpec with Matchers with MockFactory {
  implicit val ioRuntime: unsafe.IORuntime = unsafe.IORuntime.global

  trait Helpers {

    def runK[F[_], T, U](kleisli: Kleisli[F, T, U], value: T): F[U] =
      kleisli.run(value)

  }

  implicit val prettifier: Prettifier = Prettifier { case x => // includes null
    // initial linebreak makes expected/actual results line up nicely
    System.lineSeparator + Prettifier.default(x)
  }

  def resource(path: String): BufferedSource =
    Source.fromInputStream(getClass.getResourceAsStream(path))
}
