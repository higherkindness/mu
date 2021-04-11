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

import higherkindness.mu.rpc.internal.service.makro._
import scala.reflect.macros.blackbox

// $COVERAGE-OFF$
class serviceImpl(val c: blackbox.Context) {
  import c.universe._

  val wartSuppression = new WartSuppression[c.type](c)
  import wartSuppression._

  val rpcServiceModel = new RPCServiceModel[c.type](c)
  import rpcServiceModel._

  def service(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import Flag._

    val classAndMaybeCompanion = annottees.map(_.tree)
    val result: List[Tree] = classAndMaybeCompanion.head match {
      case serviceDef: ClassDef
          if serviceDef.mods.hasFlag(TRAIT) || serviceDef.mods.hasFlag(ABSTRACT) =>
        val service = new RPCService(serviceDef)
        val companion: ModuleDef = classAndMaybeCompanion.lastOption match {
          case Some(obj: ModuleDef) => obj
          case _ =>
            ModuleDef(
              NoMods,
              serviceDef.name.toTermName,
              Template(
                List(TypeTree(typeOf[AnyRef])),
                noSelfType,
                List(
                  DefDef(
                    Modifiers(),
                    termNames.CONSTRUCTOR,
                    List(),
                    List(List()),
                    TypeTree(),
                    Block(List(pendingSuperCall), Literal(Constant(())))
                  )
                )
              )
            )
        }

        val enrichedCompanion = ModuleDef(
          companion.mods.suppressWarts("Any", "NonUnitStatements", "StringPlusAny", "Throw"),
          companion.name,
          Template(
            companion.impl.parents,
            companion.impl.self,
            companion.impl.body ++ service.imports ++ service.encodersImport ++ service.methodDescriptors ++ List(
              service.bindService,
              // service.bindTracingService,
              service.clientClass,
              service.client,
              service.clientFromChannel,
              service.unsafeClientFromChannel
              // service.tracingClientClass,
              // service.tracingClient,
              // service.tracingClientFromChannel,
              // service.unsafeTracingClientFromChannel
            ) ++ service.http
          )
        )

        // println(serviceDef)
        // println(enrichedCompanion)
        // println()
        // println()
        // println()
        // println()

        List(serviceDef, enrichedCompanion)
      case _ => sys.error("@service-annotated definition must be a trait or abstract class")
    }

    c.Expr(Block(result, Literal(Constant(()))))
  }

}

// $COVERAGE-ON$
