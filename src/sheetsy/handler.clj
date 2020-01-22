(ns sheetsy.handler
  (:require [compojure.core :refer :all]
            [clojure.core.async :refer [>! <! <!! >!! chan go-loop go timeout alts!]]
            [compojure.route :as route]
            [environ.core :refer [env]]
            [ring.util.response :as r]
            [ring.middleware.cors :refer [wrap-cors]]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]))

(defn deny []
  (-> (r/response "Access Denied")
      (r/status 403)))

(defn wrap-auth [handler]
  (fn [request]
    (if-let [pass (get-in request [:headers "authorization"])]
      (if (= pass (env :sheetsy-secret))
        (handler request)
        (deny))
      (deny))))

(defn debounce
  ([in ms] (debounce in (chan) ms))
  ([in out ms]
   (go-loop [val (<! in)]
     (let [timer (timeout ms)
           [new-val ch] (alts! [in timer])]
       (condp = ch
         timer (do (>! out val) (recur (<! in)))
         in (if new-val (recur new-val)))))
   out))

(defn handle [ch headers]
  (go 
    (>! ch "Restart")))

(defn make-routes [out]
  (let [in (chan)]
    (do
      (debounce in out 5000)
      (defroutes app-routes
        (GET "/" [:as {headers :headers}] (handle in headers))
        (route/not-found "Not Found")))))

(def app
  ;; run the twitter bit...
  (let [out (chan)]
    (do
      (start-publisher out)
      (-> (make-routes (chan)) 
          (wrap-cors :access-control-allow-origin [#".*"]
                     :access-control-allow-methods [:get :put :post :delete])
          (wrap-auth)
          (wrap-defaults api-defaults)))))
