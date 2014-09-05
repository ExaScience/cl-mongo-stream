(in-package :bson)
(in-readtable bson-byte-vector-syntax)

(defconstant +utc-offset+ (encode-universal-time 0 0 0 1 1 1970 0)
  "offset between BSON UTC datetime and Common Lisp universal time")

(deftype document ()
  "a BSON document is represented either as a binary byte vector,
   as a property list or association list representing a sequence of BSON key/value pairs,
   or as a designator for a function creating a binary BSON document on the fly"
  '(or byte-vector list function symbol))

(defvar *document-type* :plist
  "for BSON documents represented as sequences of key/value pairs, this special variable
   determines whether they are represented as Lisp property lists (:plist) or association lists (:alist)")

(defvar *array-type* 'vector
  "for BSON embedded arrays, this special variable determines
   whether they are represented as Lisp 'vector or 'list")

;;; BSON floating point numbers are represented as Lisp 'double-float
;;; BSON UTF-8 strings are represented as Lisp 'simple-base-string

(defstruct embedded-document
  "BSON embedded documents are not necessarily immediately parsed from a bson:buffer,
   but may first be wrapped by an instance of this struct.
   embedded-document-buffer can be used to continue parsing."
  buffer)

(defstruct embedded-array
  "BSON embedded arrays are not necessarily immediately parsed from a bson:buffer,
   but may first be wrapped by an instance of this struct.
   embedded-document-buffer can be used to continue parsing."
  buffer)

(defstruct binary
  "a struct representing BSON binary data"
  (subtype #x00 :type byte)
  (bytes #[] :type byte-vector))

(defconstant undefined 'undefined
  "a constant representing BSON 'undefined'")

(defstruct objectid
  "a struct representing a BSON ObjectId"
  (bytes #[] :type byte-vector))

(defconstant false 'false
  "a constant representing BSON Boolean 'false'")

(defconstant true 'true
  "a constant representing BSON Boolean 'true'")

(defstruct datetime
  "a struct representing a BSON UTC datetime"
  (seconds 0 :type integer)
  (milliseconds 0 :type fixnum))

(defconstant null 'null
  "a constant representing the BSON null value")

(defstruct regular-expression
  "a struct representing a BSON regular expression"
  (pattern "" :type string)
  (options "" :type string))

(defstruct dbpointer
  "a struct representing a BSON DBPointer"
  (string "" :type string)
  (bytes #[] :type byte-vector))

(defstruct javascript
  "a struct representing BSON JavaScript code"
  (string "" :type string))

;;; BSON symbols are represented as Lisp 'symbol

(defstruct javascript-with-scope
  "a struct representing BSON JavaScript code w/ scope"
  (string "" :type string)
  (scope '() :type list))

(deftype int32 ()
  "BSON int32 type"
  '(signed-byte 32))

(defstruct timestamp
  "a struct representing a BSON timestamp"
  (value 0 :type integer))

(deftype int64 ()
  "BSON int64 type"
  '(signed-byte 64))

(defconstant min 'min
 "a constant representing the BSON min value")

(defconstant max 'max
  "a constant representing the BSON max value")
