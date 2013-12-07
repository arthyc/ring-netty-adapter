(ns ring.adapter.netty
  (:use ring.adapter.plumbing)
  (:import (java.net InetSocketAddress)
           (java.util.concurrent Executors)
           (java.io ByteArrayInputStream)
           (io.netty.bootstrap ServerBootstrap)
           (io.netty.channel ChannelFutureListener ChannelPipeline ChannelInitializer ChannelOption 
                             SimpleChannelInboundHandler)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.socket.nio NioServerSocketChannel)
           (io.netty.handler.stream ChunkedWriteHandler)
           (io.netty.handler.codec.http HttpContentCompressor HttpRequestDecoder 
                                        HttpResponseEncoder HttpObjectAggregator FullHttpRequest)
    ))
	   
(defn- make-handler [handler zerocopy]
  (proxy [SimpleChannelInboundHandler] [FullHttpRequest]
    ;(channelReadComplete [ctx]
    ;  (.flush ctx))
    (channelRead0 [ctx msg]
      (cond (instance? FullHttpRequest msg)
        (let [request-map (build-request-map ctx ^FullHttpRequest msg)
              ring-response (handler request-map)]
          (.println System/out request-map)
          (when ring-response
            (write-response ctx zerocopy (request-map :keep-alive) ring-response)))
        (instance? WebSocketFrame msg)
        (println "websocket frame")
      ))
    (exceptionCaught [ctx cause]
      ;(print "----" (type cause))
      (-> cause .printStackTrace)
      ;(print "----" (-> cause .printStackTrace))
      ;(.printStackTrace cause)
      (-> ctx  .close))))

(defn- make-pipeline [options handler ch]
  (let [pipeline (.pipeline ch)]
    (doto pipeline
		  (.addLast "decoder" (HttpRequestDecoder.))
		  (.addLast "aggregator" (HttpObjectAggregator. 65636))
		  (.addLast "encoder" (HttpResponseEncoder.))
      (.addLast "chunkedWriter" (ChunkedWriteHandler.))
		  ;(.addLast "deflater" (HttpContentCompressor.))
		  (.addLast "handler" (make-handler handler (or (:zerocopy options) false))))
    pipeline))

(defn- init-channel [options handler]
  (proxy [ChannelInitializer] []
    (initChannel [ch] (make-pipeline options handler ch))))

(defn- create-server [options handler] 
  (let [bootstrap (ServerBootstrap.)]
    (doto bootstrap
      (.channel NioServerSocketChannel)
      (.childHandler (init-channel options handler))
      (.option ChannelOption/SO_BACKLOG (int 100))
      (.childOption ChannelOption/TCP_NODELAY  true)
      (.childOption ChannelOption/SO_KEEPALIVE true))
    bootstrap))
	
(defn- bind [bs port]
  (.bind bs (InetSocketAddress. port)))

(defn run-netty [handler options]
  (let [boss-group (NioEventLoopGroup.)
        work-group (NioEventLoopGroup.)
        bootstrap (create-server options handler)port (options :port 80)]
    (try
      (.group bootstrap boss-group work-group)
      (println "Running server on port:" port)
      (-> bootstrap (bind port) .sync 
        .channel .closeFuture .sync)
      (catch Exception e (print (.getMessage e)))
      (finally (.shutdownGracefully boss-group)
               (.shutdownGracefully work-group)))))

		    