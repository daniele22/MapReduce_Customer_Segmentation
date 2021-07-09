package ScalaSparkDBSCAN.util.commandLine

import org.apache.commons.math3.ml.distance.DistanceMeasure

private [ScalaSparkDBSCAN] class CommonArgsParser [C <: CommonArgs] (val args: C, programName: String)
  extends scopt.OptionParser[Unit] (programName) with Serializable {

  opt[String] ("ds-master")
    .foreach { args.masterUrl = _ }
    .required ()
    .valueName ("<url>")
    .text ("Master URL")

  opt[String] ("ds-jar")
    .foreach { args.jar = _ }
    .required ()
    .valueName ("<jar>")
    .text ("Path to dbscan_prototype.jar which is visible to all nodes in your cluster")

  opt[String] ("ds-input")
    .foreach { args.inputPath = _ }
    .required()
    .valueName("<path>")
    .text("Input path")

  opt[String] ("ds-output")
    .foreach { args.outputPath = _ }
    .required()
    .valueName("<path>").text("Output path")

  opt[String] ("distanceMeasure").foreach {
    x => args.distanceMeasure = Class.forName(x).newInstance().asInstanceOf[DistanceMeasure]
  }

  opt[String] ("ds-debugOutput").foreach { x => args.debugOutputPath = Some(x) }

}

import java.io.File
private [ScalaSparkDBSCAN] case class Config(foo: Int = -1, out: File = new File("."), xyz: Boolean = false,
                  libName: String = "", maxCount: Int = -1, verbose: Boolean = false, debug: Boolean = false,
                  mode: String = "", files: Seq[File] = Seq(), keepalive: Boolean = false,
                  jars: Seq[File] = Seq(), kwargs: Map[String,String] = Map()) extends Serializable