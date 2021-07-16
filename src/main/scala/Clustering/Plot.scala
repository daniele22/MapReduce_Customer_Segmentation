package Clustering

import com.cibo.evilplot._
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.cibo.evilplot.numeric.{Point, Point3d}
import com.cibo.evilplot.colors.RGB
import com.cibo.evilplot.geometry.{Align, Drawable, Extent, Rect, Text}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultFonts, DefaultTheme}
import com.cibo.evilplot.plot.renderers.{BarRenderer, LegendRenderer, PointRenderer, TickRenderer}
import com.cibo.evilplot.colors._
import java.io.File

import com.cibo.evilplot.plot.components.{Legend, Position}
import jdk.nashorn.internal.ir.Labels

import scala.util.Random
import Utils.Const._

//import org.plotly-scala::plotly-render:0.8.1


object Plot {

  def saveBarChart(data: Seq[Double], labels: Seq[String],
                   filepath: String,
                   positive_val_color : (Int, Int, Int) = (241, 121, 6),
                   negative_val_color : (Int, Int, Int) = (226, 56, 140)): Unit = {
//    val percentChange = Seq[Double](-10, 5, 12, 68, -22)
//    val labels = Seq("one", "two", "three", "four", "five")

    val labeledByColor = new BarRenderer {
      val positive = RGB(positive_val_color._1, positive_val_color._2, positive_val_color._3)
      val negative = RGB(negative_val_color._1, negative_val_color._2, negative_val_color._3)
      def render(plot: Plot, extent: Extent, category: Bar): Drawable = {
        val rect = Rect(extent)
        val value = (math floor category.values.head *100)/100 // truncate double numeber
        val color = if (value >= 0) positive else negative
        Align.center(rect filled color, Text(s"$value%", size = 20)
          .filled(RGB(230, 126, 34))
        ).group
      }
    }
    val customCategoricalLegend = Legend(
      Position.Right,
      LegendContext(
        data.map(x => Rect(x)),
        labels.map(x => Text(x))
      ),
      LegendRenderer.vertical(),
      x = 0.0,
      y = 0.3
    )
    val customlabels = (1 to labels.length).map(x => x.toString)
    val mm = data.map(Bar.apply).length
    println("prova lenghtssss: " + customlabels.length+ "   "+mm+"    "+labels.length)
    BarChart.custom(data.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
      .standard(xLabels = labels)//customlabels)
//      .rightLegend(labels = Some(labels))
//      .component(customCategoricalLegend)
//      .continuousAxis(p => p.xbounds, position = Position.Bottom,
//        tickRenderer = Some(TickRenderer.axisTickRenderer(Position.Bottom, rotateText = 45)))
//      .discreteAxis(position = Position.Bottom, labels = labels, values = data , tickRenderer = Some(TickRenderer.axisTickRenderer(Position.Bottom, rotateText = 45)))
//      .xAxis(labels = labels)
//      .xAxis(tickRenderer = Some(TickRenderer.axisTickRenderer(Position.Bottom, rotateText = 45)))
//      .xAxis(labelFormatter = Some())
//      .continuousAxis(plot => plot.xbounds, Position.Bottom,
//        tickRenderer = Some(TickRenderer.axisTickRenderer(Position.Bottom, rotateText = 90)))
//      .xAxis(labels = labels)
//      .background(color = RGB(255, 255, 255))
      .hline(0)
      .render()
      .write(new File(filepath))

    //Rect(40, 40).asBufferedImage
  }


  /**
   *
   * @param data a list of couples (coordinates: Vector, cluster id: Int)
   * @param labels the names of the features that the coordinates represent
   * @param filepath file in which the resulting plot will be saved
   */
  def savePairPlot(data: Array[(Vector[Double], Int)], labels: Seq[String],
                   filepath: String): Unit = {
    //val labels = Vector("a", "b", "c", "d")
//    val dataToPlot = for (i <- 1 to labels.length) yield {
//      (labels(i - 1), data.map(elem => elem._1(i-1)))
//    }

    println("Run save pair plot .........")
    val plt = for (i <- 1 to labels.length) yield {
      for (j <- 1 to labels.length) yield {
        val xdata = data.map(elem => elem._1(i-1))
        val ydata = data.map(elem => elem._1(j-1))
        val clusterIds = data.map(elem => elem._2)
        // Use an instance of the class Point3D to memorize the cluster id and use it to plot the points
        // with different colors based on the points' classes
        val points = (xdata, ydata, clusterIds).zipped.map{ (a, b, c) => Point3d(a, b, c) }
        if (labels(i - 1) == labels(j - 1)) {
//          Histogram(xdata, bins = labels.length)
          ScatterPlot(Array[Point3d[Int]]())  // empty plot
        } else {
          ScatterPlot(points,
            pointRenderer = Some(PointRenderer.colorByCategory(points, {x: Point3d[Int] => x.z}) ))
        }
      }
    }

//    val plots = for ((xlabel, xdata) <- dataToPlot) yield {
//      for ((ylabel, ydata) <- dataToPlot) yield {
//        val points = (xdata, ydata).zipped.map { (a, b) => Point(a, b) }
//        if (ylabel == xlabel) {
//          Histogram(xdata, bins = 4)
//        } else {
//          ScatterPlot(points)
//        }
//      }
//    }

    Facets(plt)
      .standard()
      .title("Pairs Plot")
      .topLabels(labels)
      .rightLabels(labels)
      .rightLegend()
      .render()
      .write(new File(img_pkg_path + "/kmeans_pairplot.png"))

  }

  def main(args: Array[String]): Unit = {

    val labels = Vector("a", "b", "c", "d")
    val data = for (i <- 1 to 4) yield {
      (labels(i - 1), Seq.fill(10) { Random.nextDouble() * 10 })
    }
    println("DATA")
    data.foreach(println)

    val plots = for ((xlabel, xdata) <- data) yield {
      for ((ylabel, ydata) <- data) yield {
        val points = (xdata, ydata).zipped.map { (a, b) => Point(a, b) }
        if (ylabel == xlabel) {
          Histogram(xdata, bins = 4)
        } else {
          ScatterPlot(points)
        }
      }
    }
    Facets(plots)
      .standard()
      .title("Pairs Plot with Histograms")
      .topLabels(data.map { _._1 })
      .rightLabels(data.map { _._1 })
      .render()
      .write(new File("C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/MapReduce_Customer_Segmentation/src/main/scala/img/tmp.png"))


//    val data = Seq.tabulate(100) { i =>
//      Point(i.toDouble, scala.util.Random.nextDouble())
//    }
//    //File file = new File()
//    ScatterPlot(data)
//      .xAxis()
//      .yAxis()
//      .frame()
//      .xLabel("x")
//      .yLabel("y")
//      .render()
//      .write(new File("plot.jpg"))
//
//    val percentChange = Seq[Double](-10, 5, 12, 68, -22)
//    val labels = Seq("one", "two", "three", "four", "five")
//    saveBarChart(percentChange, labels, "C:/Users/hp/Desktop/Uni/magistrale/Scalable_and_cloud_programming/Progetto/MapReduce_Customer_Segmentation/src/main/scala/img/tmp.png")



  }
}
