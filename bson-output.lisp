(in-package :bson)

(declaim (inline %write-byte))

(defun %write-byte (element buffer)
  "write a byte to buffer"
  (declare (byte element) (buffer buffer) #.*optimization*)
  (buffer-push element buffer))

(declaim (notinline %write-byte-vector))

(defun %write-byte-vector (byte-vector buffer)
  "write a byte-vector to buffer"
  (declare (byte-vector byte-vector) (buffer buffer) #.*optimization*)
  (buffer-extend byte-vector buffer))

(defun %write-byte-sequence (byte-sequence buffer)
  "write a byte sequence to buffer; can be byte-vector, vector, or list"
  (declare (buffer buffer) #.*optimization*)
  (typecase byte-sequence
    (byte-vector (%write-byte-vector byte-sequence buffer))
    (vector      (loop for byte across byte-sequence do (%write-byte byte buffer)))
    (list        (loop for byte in byte-sequence do (%write-byte byte buffer)))))

(defun slow-key (key)
  "convert an arbitrary BSON key to string, slow path"
  (flet ((positive-integer-to-string (i)
           (declare (fixnum i) #.*fixnum-optimization*)
           (when (= i 0) (return-from positive-integer-to-string "0"))
           (assert (> i 0))
           (let ((j i) (k 0))
             (declare (fixnum j k))
             (loop until (zerop j) do 
                   (setq j (floor j 10))
                   (incf k))
             (let ((string (make-array k :element-type 'base-char)))
               (loop until (zerop i) do
                     (setf (values i j) (floor i 10))
                     (decf k)
                     (setf (sbchar string k) (code-char (+ j #.(char-code #\0)))))
               string))))
    (etypecase key
      (character (make-string 1 :initial-element key :element-type 'base-char))
      (integer   (positive-integer-to-string key)))))

(declaim (inline key))

(defun key (key)
  "convert an arbitrary BSON key to string; can be symbol, string, character, or integer"
  (declare #.*optimization*)
  (typecase key
    (symbol (symbol-name key))
    (string key)
    (t      (slow-key key))))

(declaim (inline write-with-preceding-length))

(defun write-with-preceding-length (buffer fun)
  "remembers the current position of the buffer, increments it by the size of an int32,
   and then calls fun with that buffer. when fun returns, the remembered position is patched
   with the distance between the remembered position and the position after return of the fun."
  (declare (buffer buffer) #.*fixnum-optimization*)
  (let ((patch-position (buffer-pos buffer)))
    (declare (fixnum patch-position))
    (setf (buffer-pos buffer) 
          (+ patch-position 4))
    (multiple-value-prog1 (funcall fun buffer)
      (let ((cont-position (buffer-pos buffer)))
        (declare (fixnum cont-position))
        (setf (buffer-pos buffer) patch-position)
        (%write-int32 (- cont-position patch-position) buffer)
        (setf (buffer-pos buffer) cont-position)))))

(defmacro with-preceding-length ((buffer) &body body)
  "macro version of write-with-preceding-length"
  `(write-with-preceding-length ,buffer (lambda (,buffer) ,@body)))

(declaim (inline call-with-document-output))

(defun call-with-document-output (buffer fun)
  "calls fun with buffer to produce a BSON document on the fly.
   the preceding length and the final null byte are implicitly added."
  (declare (buffer buffer) #.*optimization*)
  (with-preceding-length (buffer)
    (multiple-value-prog1 (funcall fun buffer)
      (%write-byte #x00 buffer))))

(defmacro with-document-output ((var) &body body)
  "macro version of call-with-document-output"
  `(call-with-document-output ,var (lambda (,var) ,@body)))

(defun %write-cstring (string buffer)
  "write a BSON cstring"
  (declare (simple-base-string string) (buffer buffer) #.*optimization*)
  (when (> (length string) 0)
    (buffer-extend-string string buffer))
  (%write-byte #x00 buffer))

(defun %write-int32 (value buffer)
  "write a BSON int32"
  (declare (integer value) (buffer buffer) #.*optimization*)
  (let ((bytes (buffer-bytes buffer)))
    (declare (vector bytes))
    (setf (sys:typed-aref '(signed-byte 32) bytes 0) value)
    (buffer-extend-bytes bytes 4 buffer)))

(defun %write-int64 (value buffer)
  "write a BSON int64"
  (declare (integer value) (buffer buffer) #.*optimization*)
  (let ((bytes (buffer-bytes buffer)))
    (declare (vector bytes))
    (multiple-value-bind
        (div mod)
        (floor value #.(expt 2 32))
      (setf (sys:typed-aref '(unsigned-byte 32) bytes 0) mod)
      (setf (sys:typed-aref '(signed-byte 32)   bytes 4) div)
      (buffer-extend-bytes bytes 8 buffer))))

(defun %write-float (value buffer)
  "write a BSON float"
  (declare (double-float value) (buffer buffer) #.*optimization*)
  (let ((bytes (buffer-bytes buffer)))
    (declare (vector bytes))
    (setf (sys:typed-aref 'double-float bytes 0) value)
    (buffer-extend-bytes bytes 8 buffer)))

(declaim (inline %write-string))

(defun %write-string (string buffer)
  "write a BSON string"
  (declare (simple-base-string string) (buffer buffer) #.*fixnum-optimization*)
  (%write-int32 (1+ (length string)) buffer)
  (%write-cstring string buffer))

(declaim (inline write-key))

(defun write-key (key buffer)
  "write a BSON key; can be symbol, string, character, or integer"
  (declare (buffer buffer) #.*optimization*)
  (%write-cstring (key key) buffer))

(declaim (inline write-float))

(defun write-float (key value buffer)
  "write a BSON float element"
  (declare (number value) (buffer buffer) #.*optimization*)
  (%write-byte #x01 buffer)
  (write-key key buffer)
  (%write-float (coerce value 'double-float) buffer))

(declaim (inline write-string))

(defun write-string (key value buffer)
  "write a BSON string element"
  (declare (string value) (buffer buffer) #.*optimization*)
  (%write-byte #x02 buffer)
  (write-key key buffer)
  (%write-string value buffer))

(defun %write-list-document (document buffer)
  "write a BSON document represented as a Lisp property list or association list"
  (declare (list document) (buffer buffer) #.*optimization*)
  (with-document-output (buffer)
    (let ((type *document-type*))
      (ecase type
        (:plist (loop for (key value) on document by 'cddr
                      do (write-element key value buffer)))
        (:alist (loop for (key . value) in document
                      do (write-element key value buffer)))))))

(declaim (inline write-document))

(defun write-document (document buffer)
  "write a BSON document represented as a byte-vector, a Lisp property list or association list,
   or a designator for a function that creates a BSON document on the fly."
  (declare (buffer buffer) #.*optimization*)
  (typecase document
    (byte-vector (%write-byte-vector document buffer))
    (list        (%write-list-document document buffer))
    (t           (call-with-document-output buffer document))))

(declaim (inline %write-embedded-list-document))

(defun %write-embedded-list-document (key document buffer)
  "write a BSON embedded document represented as a Lisp property list or association list"
  (declare (buffer buffer) (list document) #.*optimization*)
  (%write-byte #x03 buffer)
  (write-key key buffer)
  (%write-list-document document buffer))

(declaim (inline write-embedded-document))

(defun write-embedded-document (key document buffer)
  "write a BSON embedded document represented as a byte-vector, a Lisp property list or association list,
   or a designator for a function that creates a BSON document on the fly."
  (declare (buffer buffer) #.*optimization*)
  (%write-byte #x03 buffer)
  (write-key key buffer)
  (write-document document buffer))

(defmacro with-embedded-document-output ((key buffer) &body body)
  "macro version of write-embedded-document for creating BSON documents on the fly"
  `(write-embedded-document ,key (lambda (,buffer) ,@body) ,buffer))

(defun %write-sequence-array (array buffer)
  "write a BSON embedded array represented as a Lisp 'vector or 'list"
  (declare (sequence array) (buffer buffer) #.*fixnum-optimization*)
  (flet ((inc-string-integer (string)
           (declare (simple-base-string string) #.*fixnum-optimization*)
           (loop for pos of-type fixnum from (1- (length string)) downto 0
                 for char of-type base-char = (sbchar string pos) do
                 (cond ((char= char #\9)
                        (setf (sbchar string pos) #\0))
                       (t (setf (sbchar string pos) (code-char (1+ (char-code char))))
                          (return string)))
                 finally
                 (let ((new-string (make-array (1+ (length string)) :element-type 'base-char)))
                   (setf (sbchar new-string 0) #\1)
                   (loop for i of-type fixnum below (length string)
                         do (setf (sbchar new-string (1+ i)) (sbchar string i)))
                   (return new-string)))))
    (etypecase array
      (vector (with-document-output (buffer)
                (loop for index of-type fixnum below (length (the vector array))
                      for i of-type simple-base-string = (make-string 1 :initial-element #\0 :element-type 'base-char)
                      then (inc-string-integer i) do
                      (write-element i (aref array index) buffer))))
      (list   (with-document-output (buffer)
                (loop for element in (the list array)
                      for i of-type simple-base-string = (make-string 1 :initial-element #\0 :element-type 'base-char)
                      then (inc-string-integer i) do
                      (write-element i element buffer)))))))

(declaim (inline write-embedded-array))

(defun write-embedded-array (key value buffer)
  "write a BSON embedded array represented as a byte-vector, a Lisp 'vector or 'list,
   or a designator for a function that creates a BSON array on the fly."
  (declare (buffer buffer) #.*optimization*)
  (%write-byte #x04 buffer)
  (write-key key buffer)
  (typecase value
    (byte-vector (%write-byte-vector value buffer))
    (sequence    (%write-sequence-array value buffer))
    (t           (call-with-document-output buffer value))))

(defmacro with-embedded-array-output ((key buffer) &body body)
  "macro version of write-embedded-array for creating BSON arrays on the fly"
  `(write-embedded-array ,key (lambda (,buffer) ,@body) ,buffer))

(defun write-binary (subtype key value buffer)
  "write a BSON binary element"
  (declare (byte subtype) (sequence value) (buffer buffer) #.*optimization*)
  (%write-byte #x05 buffer)
  (write-key key buffer)
  (%write-int32 (length value) buffer)
  (%write-byte subtype buffer)
  (%write-byte-sequence value buffer))

(defun write-undefined (key buffer)
  "write a BSON undefined element"
  (declare (buffer buffer) #.*optimization*)
  (warn "Writing deprecated BSON undefined value.")
  (%write-byte #x06 buffer)
  (write-key key buffer))

(defun write-objectid (key value buffer)
  "write a BSON ObjectId element"
  (declare (sequence value) (buffer buffer) #.*optimization*)
  (assert (= (length value) 12))
  (%write-byte #x07 buffer)
  (write-key key buffer)
  (%write-byte-sequence value buffer))

(defun write-false (key buffer)
  "write a BSON Boolean false element"
  (declare (buffer buffer) #.*optimization*)
  (%write-byte #x08 buffer)
  (write-key key buffer)
  (%write-byte #x00 buffer))

(defun write-true (key buffer)
  "write a BSON Boolean true element"
  (declare (buffer buffer) #.*optimization*)
  (%write-byte #x08 buffer)
  (write-key key buffer)
  (%write-byte #x01 buffer))

(defun write-boolean (key value buffer)
  "write a BSON Boolean element, where value must be either 'false or 'true"
  (declare (buffer buffer) #.*optimization*)
  (%write-byte #x08 buffer)
  (write-key key buffer)
  (%write-byte (ecase value (false #x00) (true #x01)) buffer))

(defun write-lisp-boolean (key value buffer)
  "write a BSON Boolean element, where value is either Lisp nil or not Lisp nil"
  (declare (buffer buffer) #.*optimization*)
  (%write-byte #x08 buffer)
  (write-key key buffer)
  (%write-byte (if value #x01 #x00) buffer))

(defun write-datetime (key seconds milliseconds buffer)
  "write a BSON datetime element"
  (declare (integer seconds) (fixnum milliseconds) (buffer buffer) #.*optimization*)
  (%write-byte #x09 buffer)
  (write-key key buffer)
  (%write-int64 (+ (* 1000 (+ seconds +utc-offset+) milliseconds)) buffer))

(declaim (inline write-null))

(defun write-null (key buffer)
  "write a BSON null element"
  (declare (buffer buffer) #.*optimization*)
  (%write-byte #x0A buffer)
  (write-key key buffer))

(defun write-regular-expression (key pattern options buffer)
  "write a BSON regular expression element"
  (declare (simple-base-string pattern options) (buffer buffer) #.*optimization*)
  (%write-byte #x0B buffer)
  (write-key key buffer)
  (%write-cstring pattern buffer)
  (%write-cstring options buffer))

(defun write-dbpointer (key string bytes buffer)
  "write a BSON dbpointer element"
  (declare (simple-base-string string) (sequence bytes) (buffer buffer) #.*optimization*)
  (warn "Writing deprecated BSON DBPointer value.")
  (assert (= (length bytes) 12))
  (%write-byte #x0C buffer)
  (write-key key buffer)
  (%write-string string buffer)
  (%write-byte-sequence bytes buffer))

(defun write-javascript (key string buffer)
  "write a BSON JavaScript element"
  (declare (simple-base-string string) (buffer buffer) #.*optimization*)
  (%write-byte #x0D buffer)
  (write-key key buffer)
  (%write-string string buffer))
  
(defun write-symbol (key value buffer)
  "write a BSON symbol element"
  (declare (buffer buffer) #.*optimization*)
  (warn "Writing deprecated BSON symbol value.")
  (%write-byte #x0E buffer)
  (write-key key buffer)
  (%write-string (string value) buffer))

(defun write-javascript-with-scope (key string document buffer)
  "write a BSON JavaScript w/ scope element"
  (declare (simple-base-string string) (buffer buffer) #.*optimization*)
  (%write-byte #x0F buffer)
  (write-key key buffer)
  (with-preceding-length (buffer)
    (%write-string string buffer)
    (write-document document buffer)))

(declaim (inline write-int32))

(defun write-int32 (key value buffer)
  "write a BSON int32 element"
  (declare (integer value) (buffer buffer) #.*optimization*)
  (%write-byte #x10 buffer)
  (write-key key buffer)
  (%write-int32 value buffer))

(declaim (inline write-timestamp))

(defun write-timestamp (key value buffer)
  "write a BSON timestamp element"
  (declare (integer value) (buffer buffer) #.*optimization*)
  (%write-byte #x11 buffer)
  (write-key key buffer)
  (%write-int64 value buffer))

(declaim (inline write-int64))

(defun write-int64 (key value buffer)
  "write a BSON int64 element"
  (declare (integer value) (buffer buffer) #.*optimization*)
  (%write-byte #x12 buffer)
  (write-key key buffer)
  (%write-int64 value buffer))

(declaim (inline write-min))

(defun write-min (key buffer)
  "write a BSON min element"
  (declare (buffer buffer) #.*optimization*)
  (%write-byte #xFF buffer)
  (write-key key buffer))

(declaim (inline write-max))

(defun write-max (key buffer)
  "write a BSON max element"
  (declare (buffer buffer) #.*optimization*)
  (%write-byte #x7F buffer)
  (write-key key buffer))

(defun write-number (key value buffer)
  "write a BSON number.
   integers in the appropriate ranges are represented as BSON int32 or int64.
   all other numbers are represented as double floats"
  (typecase value
    (int32 (write-int32 key value buffer))
    (int64 (write-int64 key value buffer))
    (t     (write-float key value buffer))))

(defgeneric write-element (key value buffer)
  (:documentation
   "write a BSON element, where the element type is decuded from the Lisp type of value.
    corner cases:
    - when value is a 'list, it is always interpreted as a property list or association list,
      representing a BSON embedded document, never as a sequence representing a BSON embedded array
    - only when value is a 'vector, it is interpreted as a sequence representing a BSON embedded array
    - Lisp 'nil or not 'nil are never interpreted as Booleans, use 'true or 'false instead")
  (:method (key (value float) (buffer buffer))
   (write-float key value buffer))
  (:method (key (value string) (buffer buffer))
   (write-string key value buffer))
  (:method (key (document list) (buffer buffer))
   (%write-embedded-list-document key document buffer))
  (:method (key (value vector) (buffer buffer))
   (write-embedded-array key value buffer))
  (:method (key (value binary) (buffer buffer))
   (write-binary (binary-subtype value) 
                 key 
                 (binary-bytes value) 
                 buffer))
  (:method (key (value objectid) (buffer buffer))
   (write-objectid key (objectid-bytes value) buffer))
  (:method (key (value (eql 'false)) (buffer buffer))
   (write-false key buffer))
  (:method (key (value (eql 'true)) (buffer buffer))
   (write-true key buffer))
  (:method (key (value datetime) (buffer buffer))
   (write-datetime key (datetime-seconds value) (datetime-milliseconds value) buffer))
  (:method (key (value (eql 'null)) (buffer buffer))
   (write-null key buffer))
  (:method (key (value regular-expression) (buffer buffer))
   (write-regular-expression key 
                             (regular-expression-pattern value)
                             (regular-expression-options value) 
                             buffer))
  (:method (key (value javascript) (buffer buffer))
   (write-javascript key (javascript-string value) buffer))
  (:method (key (value javascript-with-scope) (buffer buffer))
   (write-javascript-with-scope key 
                                (javascript-with-scope-string value)
                                (javascript-with-scope-scope value) 
                                buffer))
  (:method (key (value integer) (buffer buffer))
   (write-number key value buffer))
  (:method (key (value timestamp) (buffer buffer))
   (write-timestamp key (timestamp-value value) buffer))
  (:method (key (value (eql 'min)) (buffer buffer))
   (write-min key buffer))
  (:method (key (value (eql 'max)) (buffer buffer))
   (write-max key buffer)))

(defun document (document)
  "creates a byte-vector representing the BSON document, typically from a property list or association list"
  (declare #.*optimization*)
  (let ((buffer (make-buffer)))
    (declare (buffer buffer))
    (write-document document buffer)
    (buffer-to-byte-vector buffer)))

(defun document-reader-macro (stream char p)
  "syntax for creating binary BSON documents, typically from property lists or association lists"
  (declare (ignore char))
  (when p (warn "Unused infix parameter ~S in bson syntax." p))
  (document (read stream t nil t)))

(defreadtable bson-syntax
  (:merge bson-byte-vector-syntax)
  (:dispatch-macro-char #\# #\D 'document-reader-macro))

(defmacro in-bson-syntax ()
  "make syntax for byte-vector and BSON documents available"
  '(in-readtable bson-syntax))
