package net.mojodna.metricsd.server

import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory
import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.{ConnectionlessBootstrap, ServerBootstrap}
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.string.{StringDecoder, StringEncoder}
import org.jboss.netty.channel.{FixedReceiveBufferSizePredictorFactory, Channels, ChannelPipeline, ChannelPipelineFactory}
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory
import com.codahale.logula.Logging
import java.net.InetSocketAddress

class MetricsServer(port: Int) extends Logging {
  def listen = {
    val f = new NioDatagramChannelFactory(Executors.newCachedThreadPool)
    val b = new ConnectionlessBootstrap(f)
    var h = new MetricsServiceHandler

    // Configure the pipeline factory.
    b.setPipelineFactory(new ChannelPipelineFactory {
      def getPipeline: ChannelPipeline = {
        Channels.pipeline(
          new StringEncoder(CharsetUtil.UTF_8),
          new StringDecoder(CharsetUtil.UTF_8),
          h
        )
      }
    })
    b.setOption("broadcast", "false")
    b.setOption("receiveBufferSizePredictorFactory", new FixedReceiveBufferSizePredictorFactory(1024))
    log.info("Listening on UDP port %d.", port)
    b.bind(new InetSocketAddress(port))
    val tf = new NioServerSocketChannelFactory(Executors.newCachedThreadPool, 
                          Executors.newCachedThreadPool)
    val tb = new ServerBootstrap(tf)
    tb.setPipelineFactory(new ChannelPipelineFactory {
        def getPipeline: ChannelPipeline = {
          Channels.pipeline(
            new StringEncoder(CharsetUtil.UTF_8),
            new StringDecoder(CharsetUtil.UTF_8),
            h
          )
        }
      })
    tb.setOption("child.tcpNoDelay", true)
    tb.setOption("child.keepAlive", true)
    log.info("Listening on TCP port %d.", port)
    tb.bind(new InetSocketAddress(port))
  }
}

object MetricsServer {
  val DEFAULT_PORT = 8125
}