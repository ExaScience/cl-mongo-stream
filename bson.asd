;;; Copyright (c) 2013, 2014 by Pascal Costanza, Intel Corporation.
;;; Intel Confidential

(asdf:defsystem #:bson
  :components
  ((:file "bson-package")
   (:file "bson-byte-vector" :depends-on ("bson-package"))
   (:file "bson-types" :depends-on ("bson-byte-vector"))
   (:file "bson-buffer" :depends-on ("bson-package"))
   (:file "bson-input" :depends-on ("bson-types" "bson-buffer"))
   (:file "bson-output" :depends-on ("bson-types" "bson-buffer")))
  :depends-on ("named-readtables"))
