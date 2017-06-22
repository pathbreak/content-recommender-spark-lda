name := "LDA Prototype"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
    ("org.apache.spark" %% "spark-core" % "2.1.1" % "provided").exclude("javax.servlet", "javax.servlet-api"),
    ("org.apache.spark" %% "spark-mllib" % "2.1.1" % "provided").exclude("javax.servlet", "javax.servlet-api"),
    ("org.apache.spark" %% "spark-sql" % "2.1.1" % "provided").exclude("javax.servlet", "javax.servlet-api")
    //,("com.github.fommil.netlib" % "all" % "1.1.2")
    //,("com.github.fommil" % "jniloader" % "1.1")
    /*,("edu.stanford.nlp" % "stanford-corenlp" % "3.7.0")
        .exclude("javax.servlet", "javax.servlet-api")
        .exclude("org.apache.commons", "commons-lang3")
    */
    
)

/* Spark already provides a stripped down version of netlib-java without the native system interfaces,
 and since it's a dependency of a provided spark-* JAR, it's no use including it in libraryDependencies.
 The way to override it is to add it to dependencyOverrides so that it includes the full netlib-java
 along with native system interfaces.
*/
//dependencyOverrides += "com.github.fommil.netlib" % "all" % "1.1.2"


assemblyJarName in assembly := "lda-prototype.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

