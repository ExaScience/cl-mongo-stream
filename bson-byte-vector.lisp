(in-package :bson)

(deftype byte ()
  "8-bit byte type"
  '(unsigned-byte 8))

(deftype byte-vector ()
  "vector of 8-bit bytes type"
  '(vector byte))
  
(declaim (inline make-byte-vector))
  
(defun make-byte-vector (length &rest args &key
                                initial-element initial-contents
                                adjustable fill-pointer
                                displaced-to displaced-index-offset
                                allocation single-thread)
  "create a vector of 8-bit bytes, arguments like for make-array"
  (declare (dynamic-extent args)
           (ignore initial-element initial-contents
                   adjustable fill-pointer
                   displaced-to displaced-index-offset
                   allocation single-thread))
  (apply 'make-array length :element-type 'byte args))

(defvar *empty-byte-vector* (make-byte-vector 0)
  "a byte-vector with 0 elements")
  
(defun byte-vector-reader-macro (stream char p)
  "syntax for vectors of 8-bit bytes"
  (declare (ignore char))
  (let ((l (read-delimited-list #\] stream t)))
    (cond (p (if (zerop p)
               *empty-byte-vector*
               (loop with vector = (make-byte-vector p)
                     for index below p
                     for element in l
                     do (setf (aref vector index) element)
                     finally
                     (when (< index p)
                       (fill vector element :start index))
                     (return vector))))
          (l (make-byte-vector (length l) :initial-contents l))
          (t *empty-byte-vector*))))
  
(defreadtable bson-byte-vector-syntax
  (:merge :standard)
  (:dispatch-macro-char #\# #\[ 'byte-vector-reader-macro)
  (:macro-char #\] (get-macro-character #\)) nil))
