package net.mojodna.metricsd.server

import scala.math.{round, max}
import com.codahale.logula.Logging
import org.jboss.netty.channel.{ExceptionEvent, MessageEvent, ChannelHandlerContext, SimpleChannelUpstreamHandler, ChannelLocal}
import com.yammer.metrics.core.MetricName
import java.util.concurrent.TimeUnit
import com.yammer.metrics.Metrics
import util.matching.Regex

/**
 * A service handler for :-delimited metrics strings (à la Etsy's statsd).
 */
object MetricsServiceHandler {
  private val lastFragment = new ChannelLocal[String];
}

class MetricsServiceHandler
  extends SimpleChannelUpstreamHandler with Logging {

  val COUNTER_METRIC_TYPE = "c"
  val GAUGE_METRIC_TYPE = "g"
  val HISTOGRAM_METRIC_TYPE = "h"
  val METER_METRIC_TYPE = "m"
  val TIMER_METRIC_TYPE = "ms"

  val MetricMatcher = new Regex("""([^:]+)(:((-?\d+|delete)?(\|((\w+)(\|@(\d+\.\d+))?)?)?)?)?""")
        
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    var msg = e.getMessage.asInstanceOf[String]    
    var fragment = MetricsServiceHandler.lastFragment.get(ctx.getChannel)
    if (fragment != null) {
      msg = fragment + msg
    }
    fragment = null
    if (!msg.endsWith("\n")) {
        val lastIndexOfNewLine = msg.lastIndexOf('\n')
        fragment = msg.substring(lastIndexOfNewLine + 1)
        if (lastIndexOfNewLine > 0) {
          msg = msg.substring(0, lastIndexOfNewLine)
        }
        else {
          MetricsServiceHandler.lastFragment.set(ctx.getChannel, fragment)
          return
        }
    }
    MetricsServiceHandler.lastFragment.set(ctx.getChannel, fragment)
    log.trace("Received message: %s", msg)

    msg.trim.split("\n").foreach {
      line: String =>
        // parse message
        val MetricMatcher(_metricName, _, _, _value, _, _, _metricType, _, _sampleRate) = line

        // clean up the metric name
        val metricName = _metricName.replaceAll("\\s+", "_").replaceAll("\\/", "-").replaceAll("[^a-zA-Z_\\-0-9\\.]", "")

        val metricType = if ((_value == null || _value.equals("delete")) && _metricType == null) {
          METER_METRIC_TYPE
        } else if (_metricType == null) {
          COUNTER_METRIC_TYPE
        } else {
          _metricType
        }

        val value: Long = if (_value != null && !_value.equals("delete")) {
          _value.toLong
        } else {
          1 // meaningless value
        }

        val deleteMetric = (_value != null && _value.equals("delete"))

        val sampleRate: Double = if (_sampleRate != null) {
          _sampleRate.toDouble
        } else {
          1.0
        }

        if (deleteMetric) {
          val name: MetricName = metricType match {
            case COUNTER_METRIC_TYPE =>
              new MetricName("metrics", "counter", metricName)

            case GAUGE_METRIC_TYPE =>
              new MetricName("metrics", "gauge", metricName)

            case HISTOGRAM_METRIC_TYPE | TIMER_METRIC_TYPE =>
              new MetricName("metrics", "histogram", metricName)

            case METER_METRIC_TYPE =>
              new MetricName("metrics", "meter", metricName)
          }

          log.debug("Deleting metric '%s'", name)
          Metrics.defaultRegistry.removeMetric(name)
        } else {
          metricType match {
            case COUNTER_METRIC_TYPE =>
              log.debug("Incrementing counter '%s' with %d at sample rate %f (%d)", metricName, value, sampleRate, round(value * 1 / sampleRate))
              Metrics.newCounter(new MetricName("metrics", "counter", metricName)).inc(round(value * 1 / sampleRate))

            case GAUGE_METRIC_TYPE =>
              log.debug("Updating gauge '%s' with %d", metricName, value)
              // use a counter to simulate a gauge
              val counter = Metrics.newCounter(new MetricName("metrics", "gauge", metricName))
              counter.clear()
              counter.inc(value)

            case HISTOGRAM_METRIC_TYPE | TIMER_METRIC_TYPE =>
              log.debug("Updating histogram '%s' with %d", metricName, value)
              // note: assumes that values have been normalized to integers
              Metrics.newHistogram(new MetricName("metrics", "histogram", metricName), true).update(value)

            case METER_METRIC_TYPE =>
              log.debug("Marking meter '%s'", metricName)
              Metrics.newMeter(new MetricName("metrics", "meter", metricName), "samples", TimeUnit.SECONDS).mark(value)

            case x: String =>
              log.error("Unknown metric type: %s", x)
          }

          Metrics.newMeter(new MetricName("metricsd", "meter", "samples"), "samples", TimeUnit.SECONDS).mark()
        }
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    log.error(e.getCause, "Exception in MetricsServiceHandler", e)
  }
}
