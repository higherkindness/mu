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

import java.util.concurrent.{Executor => JavaExecutor}

import cats.effect.Effect
import cats.syntax.apply._
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import io.grpc.Metadata
import io.grpc.Metadata.{ASCII_STRING_MARSHALLER, Key}
import natchez.Kernel
import cats.effect.Spawn

package object client {

  private[internal] def listenableFuture2Async[F[_], A](
      fa: => ListenableFuture[A]
  )(implicit E: Effect[F]): F[A] =
    E.async_ { cb =>
      Futures.addCallback(
        fa,
        new FutureCallback[A] {
          override def onSuccess(result: A): Unit = cb(Right(result))

          override def onFailure(t: Throwable): Unit = cb(Left(t))
        },
        new JavaExecutor {
          override def execute(command: Runnable): Unit =
            E.toIO(Spawn[F].cede *> E.delay(command.run())).unsafeRunSync()
        }
      )
    }

  private[internal] def tracingKernelToHeaders(kernel: Kernel): Metadata = {
    val headers = new Metadata()
    kernel.toHeaders.foreach { case (k, v) =>
      headers.put(Key.of(k, ASCII_STRING_MARSHALLER), v)
    }
    headers
  }

}
