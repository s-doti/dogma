(defproject dogma "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure]
                 [org.clojure/core.async]
                 [com.taoensso/timbre]
                 [midje/midje]
                 [http-kit]
                 [com.cycognito/domain-name-utils-clj]
                 [com.cycognito/ip-utils-clj]]
  :repl-options {:init-ns dogma.core}
  #_#_:managed-dependencies [[org.clojure/clojure "1.10.1"]
                         [org.clojure/core.async "1.3.618"]
                         [com.taoensso/timbre "5.1.2"]
                         [http-kit "2.5.3"]
                         [midje/midje "1.10.4"]]
  :parent-project {:coords  [com.cycognito/cyco-parent-clj "0.0.275"]
                   :inherit [:managed-dependencies
                             :dependencies
                             :plugins
                             :bom]}
  :plugins [[lein-parent "0.3.8"]]
  :repositories [["snapshots" {:url      "https://maven.cyco.fun/snapshots"
                               :username :env/cyco_maven_username
                               :password :env/cyco_maven_password}]
                 ["releases" {:url      "https://maven.cyco.fun/releases"
                              :username :env/cyco_maven_username
                              :password :env/cyco_maven_password}]])
