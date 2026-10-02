;;;;;;;;;;;;;;;;;;;
;;; delta messenger
(in-package :delta-messenger)

;;(add-delta-logger)
(add-delta-messenger "http://delta-notifier/")

;;;;;;;;;;;;;;;;;
;;; configuration
(in-package :client)
(setf *log-sparql-query-roundtrip* nil)
(setf *backend* "http://triplestore:8890/sparql")


(in-package :server)
(setf *log-incoming-requests-p* nil)

;;;;;;;;;;;;;;;;;
;;; access rights
(in-package :acl)

(defparameter *access-specifications* nil
  "All known ACCESS specifications.")

(defparameter *graphs* nil
  "All known GRAPH-SPECIFICATION instances.")

(defparameter *rights* nil
  "All known GRANT instances connecting ACCESS-SPECIFICATION to GRAPH.")

(define-graph public ("http://mu.semte.ch/graphs/public")
  (_ -> _))

(define-graph harvester0 ("http://mu.semte.ch/graphs/harvester-0")
  (_ -> _))

(define-graph harvester1 ("http://mu.semte.ch/graphs/harvester-1")
  (_ -> _))

(define-graph harvester2 ("http://mu.semte.ch/graphs/harvester-2")
  (_ -> _))

(define-graph harvester3 ("http://mu.semte.ch/graphs/harvester-3")
  (_ -> _))

(define-graph mandaten ("http://mu.semte.ch/graphs/mandaten")
  (_ -> _))

(define-graph organisations ("http://mu.semte.ch/graphs/organisations")
  (_ -> _))

;; app-decide's database keeps the governing body labels here
(define-graph organizations ("http://mu.semte.ch/graphs/organizations")
  (_ -> _))



(supply-allowed-group "public")

(grant (read)
  :to-graph (public)
  :for-allowed-group "public")

(grant (read)
  :to-graph (harvester0)
  :for-allowed-group "public")

(grant (read)
  :to-graph (harvester1)
  :for-allowed-group "public")

(grant (read)
  :to-graph (harvester2)
  :for-allowed-group "public")

(grant (read)
  :to-graph (harvester3)
  :for-allowed-group "public")

(grant (read)
  :to-graph (mandaten)
  :for-allowed-group "public")

(grant (read)
  :to-graph (organisations)
  :for-allowed-group "public")

(grant (read)
  :to-graph (organizations)
  :for-allowed-group "public")

;; increase the default read timeout. this allows waiting heavier queries
(setf dexador.util:*default-read-timeout* 300)

