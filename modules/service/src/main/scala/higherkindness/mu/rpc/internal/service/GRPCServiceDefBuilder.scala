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

package higherkindness.mu.rpc
package internal.service

import cats.effect.kernel.Sync
import cats.instances.list._
import cats.syntax.foldable._
import cats.syntax.flatMap._
import io.grpc._

object GRPCServiceDefBuilder {

  type MethodCall = (MethodDescriptor[_, _], ServerCallHandler[_, _])

  def build[F[_]: Sync](name: String, calls: MethodCall*): F[ServerServiceDefinition] = {

    def addMethod(
        builder: ServerServiceDefinition.Builder,
        call: MethodCall
    ): F[ServerServiceDefinition.Builder] =
      Sync[F].delay {
        val (descriptor, callHandler) = call
        builder.addMethod(
          ServerMethodDefinition.create(
            descriptor.asInstanceOf[MethodDescriptor[Any, Any]],
            callHandler.asInstanceOf[ServerCallHandler[Any, Any]]
          )
        )
      }

    val builder: ServerServiceDefinition.Builder = io.grpc.ServerServiceDefinition.builder(name)

    calls.toList
      .foldM[F, ServerServiceDefinition.Builder](builder)(addMethod)
      .flatMap(b => Sync[F].delay(b.build()))
  }

}
