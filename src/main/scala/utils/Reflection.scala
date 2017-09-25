package rajkumar.org.utils

import scala.util._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scala.reflect._
import scala.reflect.runtime._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.{universe => ru}


// Singleton class with some misc. reflection helper functions
object Reflection {

  // - Scala Reflection ------------------------------------------------
  // create instance of type T, using ctr with given index and arguments
  def typeInstance[T:TypeTag](args: AnyRef* )( ctrIndex: Int=0):T = {
    val typ = typeTag[T].tpe
    val ctrs = typ.members.filter(m => m.isMethod && m.asMethod.isConstructor)
    val ctor  = ctrs.toSeq( ctrIndex.min( ctrs.size -1 )).asMethod

    val csymbol = typ.typeSymbol.asClass
    val mirror  = currentMirror.reflectClass( csymbol )
    val inst = mirror.reflectConstructor( ctor )( args: _*)
    inst match { case t:T => t }
  }

  // create instance of class-type R, using ctr with given index and arguments
  def classInstance[R:ClassTag](args: AnyRef*)(ctrIndex:Int = 0):R = {
    val tag   = classTag[R]
    val cls   = tag.runtimeClass
    val ctrs  = cls.getConstructors()
    val ctr   = ctrs( ctrIndex )
    val cinst = ctr.newInstance( args: _* )
    cinst match { case r:R => r }
  }
  // create instance of class, given its name, ctr index and arguments
  def namedClassInstance(name: String, args: AnyRef*)(ctrIndex:Int = 0) = {
    val cls   = Class.forName( name )
    val ctrs  = cls.getConstructors()
    val ctr   = ctrs( ctrIndex )
    ctr.newInstance( args: _* )
  }
  def getType[T:ru.TypeTag](obj: T):Type = ru.typeTag[T].tpe
  def getClassTag[T]( obj: T ):ClassTag[T] = ClassTag( obj.getClass )

  // -- Testing -------
  def main( args: Array[String]): Unit = {
  }
}

