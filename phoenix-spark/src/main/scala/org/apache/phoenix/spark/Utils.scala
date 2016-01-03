package org.apache.phoenix.spark

import org.apache.spark.SPARK_VERSION

object Utils {
  def classForName(className: String): Class[_] = {
    val classLoader =
      Option(Thread.currentThread().getContextClassLoader).getOrElse(this.getClass.getClassLoader)
    // scalastyle:off
    Class.forName(className, true, classLoader)
    // scalastyle:on
  }

  def registerDriver(driverClass: String): Unit = {
    // DriverRegistry.register() is one of the few pieces of private Spark functionality which
    // we need to rely on. This class was relocated in Spark 1.5.0, so we need to use reflection
    // in order to support both Spark 1.4.x and 1.5.x.
    if (SPARK_VERSION.startsWith("1.4")) {
      val className = "org.apache.spark.sql.jdbc.package$DriverRegistry$"
      val driverRegistryClass = Utils.classForName(className)
      val registerMethod = driverRegistryClass.getDeclaredMethod("register", classOf[String])
      val companionObject = driverRegistryClass.getDeclaredField("MODULE$").get(null)
      registerMethod.invoke(companionObject, driverClass)
    } else { // Spark 1.5.0+
    val className = "org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry"
      val driverRegistryClass = Utils.classForName(className)
      val registerMethod = driverRegistryClass.getDeclaredMethod("register", classOf[String])
      registerMethod.invoke(null, driverClass)
    }
  }
}
