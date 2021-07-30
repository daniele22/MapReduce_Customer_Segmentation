package Utils

import java.io.File

import Utils.Const.img_pkg_path
import com.cibo.evilplot.plot.renderers.TickRenderer
// import com.cibo.evilplot._
import com.cibo.evilplot
import com.cibo.evilplot.demo.DemoPlots.theme
import com.cibo.evilplot.geometry.{Align, Drawable, Extent, Rect, Text}
import com.cibo.evilplot.numeric.{Point, Point3d}
import com.cibo.evilplot.plot.components.{Legend, Position}
import com.cibo.evilplot.plot.renderers.{BarRenderer, LegendRenderer, PointRenderer}
import com.cibo.evilplot.plot.{Bar, BarChart, Facets, Histogram, LegendContext, LinePlot, Plot, ScatterPlot}
import com.cibo.evilplot.colors.{Color, HSL, RGB}

import scala.util.Random

object Plot {

  /**
   * Create and save a barchart
   *
   * @param data               y axis values
   * @param labels             x axis labels, one per each bar
   * @param filepath           file in which the plot will be saved
   * @param positive_val_color color for positive values in the plot
   * @param negative_val_color color for negative values in the plot
   */
  def saveBarChart(data: Seq[Double], labels: Seq[String],
                   filepath: String,
                   positive_val_color: (Int, Int, Int) = (241, 121, 6),
                   negative_val_color: (Int, Int, Int) = (226, 56, 140)): Unit = {
    println("Plotting barchart.......")
    //    val percentChange = Seq[Double](-10, 5, 12, 68, -22)
    //    val labels = Seq("one", "two", "three", "four", "five")

    val labeledByColor = new BarRenderer {
      val positive = RGB(positive_val_color._1, positive_val_color._2, positive_val_color._3)
      val negative = RGB(negative_val_color._1, negative_val_color._2, negative_val_color._3)

      def render(plot: Plot, extent: Extent, category: Bar): Drawable = {
        val rect = Rect(extent)
        val value = (math floor category.values.head * 100) / 100 // truncate double numeber
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

    // simple barchart
    BarChart(data)
      .standard(xLabels = labels) //customlabels)
      .render()
      .write(new File(filepath))

//    BarChart.custom(data.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
//      .standard(xLabels = labels) //customlabels)
//      //      .rightLegend(labels = Some(labels))
//      //      .component(customCategoricalLegend)
//      //      .continuousAxis(p => p.xbounds, position = Position.Bottom,
//      //        tickRenderer = Some(TickRenderer.axisTickRenderer(Position.Bottom, rotateText = 45)))
//      //      .discreteAxis(position = Position.Bottom, labels = labels, values = data , tickRenderer = Some(TickRenderer.axisTickRenderer(Position.Bottom, rotateText = 45)))
//      //      .xAxis(labels = labels)
//      //      .xAxis(tickRenderer = Some(TickRenderer.axisTickRenderer(Position.Bottom, rotateText = 45)))
//      //      .xAxis(labelFormatter = Some())
//      //      .continuousAxis(plot => plot.xbounds, Position.Bottom,
//      //        tickRenderer = Some(TickRenderer.axisTickRenderer(Position.Bottom, rotateText = 90)))
//      //      .xAxis(labels = labels)
//      //      .background(color = RGB(255, 255, 255))
//      .hline(0)
//      .render()
//      .write(new File(filepath))

  }


  /**
   * Creates a pairplot useful to visualize the clustering results
   *
   * @param data     a list of couples (coordinates: Vector, cluster id: Int)
   * @param labels   the names of the features that the coordinates represent
   * @param filepath file in which the resulting plot will be saved
   */
  def savePairPlot(data: Array[(Int, Vector[Double])], labels: Seq[String],
                   filepath: String): Unit = {
    //val labels = Vector("a", "b", "c", "d")
    //    val dataToPlot = for (i <- 1 to labels.length) yield {
    //      (labels(i - 1), data.map(elem => elem._1(i-1)))
    //    }

    println("Run save pair plot .........")
    val plt = for (i <- 1 to labels.length) yield {
      for (j <- 1 to labels.length) yield {
        val xdata = data.map(elem => elem._2(i - 1))
        val ydata = data.map(elem => elem._2(j - 1))
        val clusterIds = data.map(elem => elem._1)
        // Use an instance of the class Point3D to memorize the cluster id and use it to plot the points
        // with different colors based on the points' classes
        val points = (xdata, ydata, clusterIds).zipped.map { (a, b, c) => Point3d(a, b, c) }
        if (labels(i - 1) == labels(j - 1)) {
          //          Histogram(xdata, bins = labels.length)
          ScatterPlot(Array[Point3d[Int]]()) // empty plot
        } else {
          ScatterPlot(points,
            pointRenderer = Some(PointRenderer.colorByCategory(points, { x: Point3d[Int] => x.z })))
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
      .write(new File(filepath))

  }

  /**
   * Creates a lineplot with the WSSSE obtained for different number of clusters
   * @param wss_list
   * @param clusters_range min - max number of cluster tested
   * @param filename output filename
   */
  def saveElbowLinePlot(wss_list: Seq[Double], clusters_range: Seq[Int], filename: String) = {
    println("Save elbow lineplot.......")
    val data = clusters_range.zip(wss_list).map(pair => Point(pair._1, pair._2))

    LinePlot.series(data, "WSSSE elbow", HSL(210, 100, 56))
      .xAxis().yAxis().frame()
      .xLabel("Number of cluster")
      .yLabel("WSSSE")
      .render()
      .write(new File(img_pkg_path + "/" + filename))
  }

  def main(args: Array[String]): Unit = {

    println("Example of plot......")

    val labels = Vector("a", "b", "c", "d")
    val data = for (i <- 1 to 4) yield {
      (labels(i - 1), Seq.fill(10) {
        Random.nextDouble() * 10
      })
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
      .topLabels(data.map {
        _._1
      })
      .rightLabels(data.map {
        _._1
      })
      .render()
      .write(new File(img_pkg_path+"/temp.png"))

  }
}
