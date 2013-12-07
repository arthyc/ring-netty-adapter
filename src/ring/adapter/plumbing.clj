(ns ring.adapter.plumbing
  (:use ring.util.mime-type)
  (:import (java.io InputStream File RandomAccessFile FileInputStream)
           (java.net URLConnection)
           (io.netty.channel ChannelFutureListener DefaultFileRegion ChannelProgressiveFutureListener)
           (io.netty.util CharsetUtil )
           (io.netty.buffer ByteBufInputStream Unpooled)
           (io.netty.handler.stream ChunkedStream ChunkedFile)
           (io.netty.handler.codec.http HttpHeaders HttpHeaders$Names HttpHeaders$Values HttpVersion HttpMethod 
					       HttpResponseStatus DefaultHttpResponse LastHttpContent DefaultHttpContent)))
	   
(defn- remote-address [ctx]
  (-> ctx .channel .remoteAddress .toString (.split ":") first (subs 1)))

(defn- get-meth [req]
  (-> req .getMethod .name .toLowerCase keyword))

(defn- get-body [req]
  (->> req .content ByteBufInputStream. ))

(defn- get-headers [req] 
  (let [headers (.headers req)
        names (.names headers)] 
    (reduce #(assoc %1 (.toLowerCase %2) (.get headers %2))
            {} names)))

(defn- content-type [headers]
  (if-let [ct (headers "content-type")]
    (-> ct (.split ";") first .trim .toLowerCase)))

(defn- uri-query [req]
  (let [uri (.getUri req)
	idx (.indexOf uri "?")]
    (if (= idx -1)
      [uri nil]
      [(subs uri 0 idx) (subs uri (inc idx))])))

(defn keep-alive? [req]
  (HttpHeaders/isKeepAlive req))

(defn build-request-map
  "Converts a netty request into a ring request map"
  [ctx netty-request]
  (let [headers (get-headers netty-request)
	socket-address (-> ctx .channel .localAddress)
	[uri query-string] (uri-query netty-request)]
    {:server-port        (.getPort socket-address)
     :server-name        (.getHostName socket-address)
     :remote-addr        (remote-address ctx)
     :uri                uri
     :query-string       query-string
     :scheme             (keyword (headers "x-scheme" "http"))
     :request-method     (get-meth netty-request)
     :headers            headers
     :content-type       (content-type headers)
     :content-length     (headers "content-length")
     :character-encoding (headers "content-encoding")
     :body               (get-body netty-request)
     :keep-alive         (keep-alive? netty-request)}))

(defn- set-headers [response headers]
  (doseq [[key val-or-vals]  headers]
    (if (string? val-or-vals)
      (.set (.headers response) key val-or-vals)
      (doseq [val val-or-vals]
	(.add (.headers response) key val)))))
		 
(defn- set-content-length [headers length]
  (.set headers HttpHeaders$Names/CONTENT_LENGTH length))

(defn- set-content-type [headers content-type]
  (.set headers HttpHeaders$Names/CONTENT_TYPE content-type))

(defn- write-content [ctx response content keep-alive]
  (let [buffer (Unpooled/copiedBuffer content CharsetUtil/UTF_8)
        http-content (DefaultHttpContent. buffer)
        len (.readableBytes buffer)]
    ;(set-content-type (.headers response) "text/plain; charset=UTF-8")
    (set-content-length (.headers response) len)
    (if keep-alive (.set (.headers response) HttpHeaders$Names/CONNECTION HttpHeaders$Values/KEEP_ALIVE))
    (.write ctx response)
    (-> ctx (.writeAndFlush http-content)(.addListener ChannelFutureListener/CLOSE))
    ))

(defn- write-file [ctx response file keep-alive zero-copy]
  
  (let [raf (RandomAccessFile. file "r")
        len (.length raf)
        region (if zero-copy (DefaultFileRegion. (.getChannel raf) 0 len) (ChunkedFile. raf 0 len 8192))]
    ;(println (type region))
    (set-content-type (.headers response) (ext-mime-type (.getName file)))
    (set-content-length (.headers response) len)
    (if keep-alive (.set (.headers response) HttpHeaders$Names/CONNECTION HttpHeaders$Values/KEEP_ALIVE))
    ;Write the initial line and the header.
    (.write ctx response);@1
    ;Write the content.
    (let [send-file-future (->> ctx .newProgressivePromise (.write ctx region))];@2
      ;if zero-copy?
      (.addListener (.write ctx region) (proxy [ChannelProgressiveFutureListener][]
                                        (operationProgressed [future progress total]
                                          ((.println System/err "============")
                                          (if (< total 0) 
                                            (.println System/err  "Transfer progress: " progress) 
                                            (.println System/err  "Transfer progress: " progress " / " total))
                                          )
                                          )
                                        (operationComplete [future]
                                          (.println System/err "Transfer complete.")))))
    ;Write the end marker
    (let [last-content-future (.writeAndFlush ctx LastHttpContent/EMPTY_LAST_CONTENT)]
      ;Decide whether to close the connection or not. ;Close the connection when the whole content is written out.
      (if-not keep-alive (.addListener last-content-future ChannelFutureListener/CLOSE)))))

(defn write-response [ctx zerocopy keep-alive {:keys [status headers body]}]
  (let [netty-response (DefaultHttpResponse. HttpVersion/HTTP_1_1 (HttpResponseStatus/valueOf status))]
    (set-headers netty-response headers)
    (cond (string? body)
      (write-content ctx netty-response body keep-alive)
      (seq? body)
      (write-content ctx netty-response (apply str body) keep-alive)
      (instance? InputStream body)
      (do
        (.write ctx netty-response)
        (-> ctx (.writeAndFlush (ChunkedStream. body))
          (.addListener (ChannelFutureListener/CLOSE))))
      (instance? File body)
      (write-file ctx netty-response body keep-alive zerocopy)
      (nil? body)
      nil
      :else
      (throw ( "Unrecognized body: %s" body)))))
