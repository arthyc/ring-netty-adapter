(defproject ring-netty-adapter "0.0.3"
 ; :repositories [["JBoss" "http://repository.jboss.org/nexus/content/groups/public/"]]
  :description "Ring Netty adapter"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [ring/ring-core "1.2.1"]
                 [javax.servlet/servlet-api "2.5"]
                 [io.netty/netty-codec-http "4.0.13.Final"]]
  ;:dev-dependencies [[swank-clojure "1.2.1"]]
  :namespaces [ring.adapter.netty ring.adapter.plumbing])
