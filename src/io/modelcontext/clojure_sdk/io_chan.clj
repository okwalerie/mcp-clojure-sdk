(ns io.modelcontext.clojure-sdk.io-chan
  (:require [babashka.json :as json]
            [camel-snake-kebab.extras :as cske]
            [clojure.core.async :as async]
            [clojure.java.io :as io]
            [me.vedang.logger.interface :as log])
  (:import [java.io ByteArrayOutputStream InputStream]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets CodingErrorAction]))

(set! *warn-on-reflection* true)

;;;; IO <-> chan

;; Follow the MCP spec for reading and writing JSON-RPC messages. Convert the
;; messages to and from Clojure hashmaps and shuttle them to core.async
;; channels.

;; https://modelcontextprotocol.io/specification

(defn ^:private kw->camelCaseString
  "Convert keywords to camelCase strings, but preserve capitalization of things
  that are already strings."
  [k]
  (if (and (keyword? k) (namespace k))
    (str (namespace k) "/" (name k))
    (if (keyword? k) (name k) k)))

(defn message->json-str
  "Serialize an MCP message map to a JSON string, converting keyword keys
  to camelCase strings as required by the wire format."
  [msg]
  (json/write-str (cske/transform-keys kw->camelCaseString msg)))

(defn json-str->message
  "Parse a JSON string into an MCP message map with keyword keys. Returns
  `:parse-error` if the string is nil or cannot be parsed.

  The nil check matters: some JSON providers (e.g. cheshire under
  Babashka) return nil for nil input instead of throwing, and nil must
  never be put on a core.async channel."
  [s]
  (if (nil? s)
    :parse-error
    (try (json/read-str s)
         (catch Exception ex
           (log/error :fn :json-str->message :ex ex)
           :parse-error))))

#_{:clj-kondo/ignore [:unused-private-var]}
(defn ^:private read-message
  [^java.io.BufferedReader input]
  (try (let [content (.readLine input)]
         (log/trace :fn :read-message :line content)
         (json-str->message content))
       (catch Exception ex (log/error :fn :read-message :ex ex) :parse-error)))

(def ^:private write-lock (Object.))

(defn ^:private write-message
  [^java.io.BufferedWriter output msg]
  (let [content (message->json-str msg)]
    (when (> (alength (.getBytes ^String content StandardCharsets/UTF_8))
             (* 4 1024 1024))
      (throw (ex-info "MCP output frame exceeds byte limit" {})))
    (locking write-lock
      (doto output (.write ^String content) (.newLine) (.flush)))))

(def max-frame-bytes (* 512 1024))
(def max-output-bytes (* 4 1024 1024))

(defn read-frame
  "Read one bounded UTF-8 line. EOF and malformed input remain distinguishable."
  [^InputStream input]
  (let [buffer (ByteArrayOutputStream.)]
    (loop []
      (let [b (.read input)]
        (cond (and (= -1 b) (zero? (.size buffer))) ::eof
              (or (= -1 b) (= 10 b))
                (try (let [decoder (doto (.newDecoder StandardCharsets/UTF_8)
                                     (.onMalformedInput
                                       CodingErrorAction/REPORT)
                                     (.onUnmappableCharacter
                                       CodingErrorAction/REPORT))]
                       (str (.decode decoder
                                     (ByteBuffer/wrap (.toByteArray buffer)))))
                     (catch java.nio.charset.CharacterCodingException _
                       ::bad-encoding))
              (>= (.size buffer) max-frame-bytes)
                (throw (ex-info "MCP input frame exceeds byte limit"
                                {:limit max-frame-bytes}))
              :else (do (.write buffer b) (recur)))))))

(defn input-stream->input-chan
  "Returns a channel which will yield parsed messages that have been read off
  the `input`. When the input is closed, closes the channel. By default when the
  channel closes, will close the input, but can be determined by `close?`.

  Reads in a thread to avoid blocking a go block thread."
  [input]
  (log/trace :fn :input-stream->input-chan :msg "Creating new input-chan")
  (let [messages (async/chan 1)]
    ;; close output when channel closes
    (async/thread
      (try (with-open [stream (io/input-stream input)]
             (loop []
               (let [frame (read-frame stream)]
                 (when-not (= ::eof frame)
                   (let [msg (if (= ::bad-encoding frame)
                               :parse-error
                               (json-str->message frame))]
                     (log/trace :fn :input-stream->input-chan :msg msg)
                     (when (async/>!! messages msg) (recur)))))))
           (catch Exception ex (log/error :fn :input-stream->input-chan :ex ex))
           (finally (async/close! messages))))
    messages))

(defn output-stream->output-chan
  "Returns a channel which expects to have messages put on it. nil values are
  not allowed. Serializes and writes the messages to the output. When the
  channel is closed, closes the output.

  Writes in a thread to avoid blocking a go block thread."
  [output]
  (let [messages (async/chan 1)]
    ;; close output when channel closes
    (async/thread (with-open [writer (io/writer (io/output-stream output))]
                    (loop []
                      (when-let [msg (async/<!! messages)]
                        (log/trace :fn :output-stream->output-chan :msg msg)
                        (try
                          (write-message writer msg)
                          (catch Throwable e (async/close! messages) (throw e)))
                        (recur)))))
    messages))
