package Clustering

import com.cibo.evilplot._
import com.cibo.evilplot.plot._
import com.cibo.evilplot.plot.aesthetics.DefaultTheme._
import com.cibo.evilplot.numeric.Point
import com.cibo.evilplot.colors.RGB
import com.cibo.evilplot.geometry.{Align, Drawable, Extent, Rect, Text}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultFonts, DefaultTheme}
import com.cibo.evilplot.plot.renderers.BarRenderer
import com.cibo.evilplot.colors._
import java.io.File

import jdk.nashorn.internal.ir.Labels


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

    BarChart.custom(data.map(Bar.apply), spacing = Some(20), barRenderer = Some(labeledByColor))
      .standard(xLabels = labels)
      .hline(0)
      .render()
      .write(new File(filepath))

    Rect(40, 40).asBufferedImage
  }

  def main(args: Array[String]): Unit = {

    val data = Seq.tabulate(100) { i =>
      Point(i.toDouble, scala.util.Random.nextDouble())
    }
    //File file = new File()
    ScatterPlot(data)
      .xAxis()
      .yAxis()
      .frame()
      .xLabel("x")
      .yLabel("y")
      .render()
      .write(new File("plot.jpg"))

    val percentChange = Seq[Double](-10, 5, 12, 68, -22)
    val labels = Seq("one", "two", "three", "four", "five")
    saveBarChart(percentChange, labels, "tmp.png")

  }
}
