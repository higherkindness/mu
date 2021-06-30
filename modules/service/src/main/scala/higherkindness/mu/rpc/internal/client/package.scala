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

package higherkindness.mu.rpc.internal

import cats.effect.Async
import cats.syntax.all._
import com.google.common.util.concurrent.ListenableFuture
import io.grpc.Metadata
import io.grpc.Metadata.{ASCII_STRING_MARSHALLER, Key}
import natchez.Kernel

import scala.util.Try

package object client {

  private[internal] def listenableFuture2Async[F[_], A](
      lfF: F[ListenableFuture[A]]
  )(implicit F: Async[F]): F[A] =
    F.async { cb =>
      val back = lfF.flatMap { lf =>
        F.executionContext.flatMap { ec =>
          F.delay(
            lf.addListener(
              () => cb(Try(lf.get).toEither),
              // Use the Async's compute EC to avoid needless context shifting across pools.
              (command: Runnable) => ec.execute(command)
            )
          )
        }
      }

      back.as(None)
    }

  private[internal] def tracingKernelToHeaders(kernel: Kernel): Metadata = {
    val headers = new Metadata()
    kernel.toHeaders.foreach { case (k, v) =>
      headers.put(Key.of(k, ASCII_STRING_MARSHALLER), v)
    }
    headers
  }

}
