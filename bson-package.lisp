(in-package :cl-user)

;; implements the BSON format. see http://bsonspec.org/spec.html

(defpackage #:bson
  (:use #:common-lisp #:named-readtables #:stream)
  (:import-from #:lispworks #:if-let #:sbchar #:string-append)
  (:shadow #:byte #:write-string #:undefined #:null #:min #:max)
  (:export
   ;; types

   #:byte #:byte-vector #:make-byte-vector
   #:byte-vector-reader-macro

   #:+utc-offset+

   #:document

   #:*document-type* #:*array-type*

   #:double-float #:simple-base-string

   #:embedded-document #:make-embedded-document #:embedded-document-p
   #:embedded-document-buffer
   #:embedded-array #:make-embedded-array #:embedded-array-p
   #:embedded-array-buffer

   #:binary #:make-binary #:binary-p
   #:binary-subtype
   #:binary-bytes

   #:undefined

   #:objectid #:make-objectid #:objectid-p
   #:objectid-bytes

   #:false #:true

   #:datetime #:make-datetime #:datetime-p
   #:datetime-seconds #:datetime-milliseconds

   #:null

   #:regular-expression #:make-regular-expression #:regular-expression-p
   #:regular-expression-pattern
   #:regular-expression-options

   #:dbpointer #:make-dbpointer #:dbpointer-p
   #:dbpointer-string
   #:dbpointer-bytes
   
   #:javascript #:make-javascript #:javascript-p
   #:javascript-string

   #:symbol

   #:javascript-with-scope #:make-javascript-with-scope #:javascript-with-scope-p
   #:javascript-with-scope-string
   #:javascript-with-scope-scope

   #:int32

   #:timestamp #:make-timestamp
   #:timestamp-value

   #:int64

   #:min #:max

   ;; buffer

   #:buffer #:make-buffer #:buffer-p
   #:buffer-pos
   #:buffer-str
   #:buffer-bytes
   #:buffer-stream
   #:reinitialize-buffer

   #:buffer-push #:buffer-push-char
   #:buffer-extend #:buffer-extend-string #:buffer-extend-bytes
   #:flush-buffer #:buffer-to-byte-vector

   ;; output

   #:%write-byte #:%write-byte-vector #:%write-byte-sequence

   #:key

   #:write-with-preceding-length #:with-preceding-length
   #:call-with-document-output #:with-document-output

   #:%write-cstring
   #:%write-int32
   #:%write-int64
   #:%write-float
   #:%write-string
   #:%write-list-document
   #:%write-embedded-list-document
   #:%write-sequence-array

   #:write-key

   #:write-float
   #:write-string
   #:write-embedded-document #:with-embedded-document-output
   #:write-embedded-array #:with-embedded-array-output
   #:write-binary
   #:write-undefined
   #:write-objectid
   #:write-false
   #:write-true
   #:write-datetime
   #:write-null
   #:write-regular-expression
   #:write-dbpointer
   #:write-javascript
   #:write-symbol
   #:write-javascript-with-scope
   #:write-int32
   #:write-timestamp
   #:write-int64
   #:write-min
   #:write-max

   #:write-boolean #:write-lisp-boolean
   #:write-number
   #:write-element
   #:write-document

   #:document-reader-macro
   #:bson-syntax #:in-bson-syntax

   ;; input

   #:*key-package*

   #:scan-sequence
   #:scan-bytes
   #:skip-bytes
   #:scan-cstring
   #:scan-string
   #:scan-key
   #:scan-keyword
   #:scan-integer-key
   
   #:%read-byte
   #:%read-document
   #:%read-array
   #:%read-float
   #:%read-int32
   #:%read-int64
   #:%read-nothing
   #:%read-error

   #:read-float
   #:read-string
   #:read-embedded-document
   #:read-embedded-array
   #:read-binary
   #:read-undefined
   #:read-objectid
   #:read-false
   #:read-true
   #:read-datetime
   #:read-null
   #:read-regular-expression
   #:read-dbpointer
   #:read-javascript
   #:read-symbol
   #:read-javascript-with-scope
   #:read-int32
   #:read-timestamp
   #:read-int64
   #:read-min
   #:read-max

   #:read-boolean
   #:read-element #:with-element-input
   #:read-document #:with-document-input
   #:read-array #:with-array-input

   #:read-binary-document #:read-binary-string-document

   #:force-value #:skip-value

   #:skip-document

   #:read-document-batch))
