package com.google.cloud.spark.bigquery

import org.apache.spark.internal.Logging
import java.util.Optional

object ScalaUtil extends Logging {

  def toOption[T](javaOptional: Optional[T]): Option[T] =
    if (javaOptional.isPresent) Some(javaOptional.get) else None

  def noneIfEmpty(s: String): Option[String] = Option(s).filterNot(_.trim.isEmpty)

}
