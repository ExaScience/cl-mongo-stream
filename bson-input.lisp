(in-package :bson)

(defun scan-sequence (buffer target &key (start 0) (end (length target)))
  "scan a number of bytes into a byte-vector"
  (declare (buffer buffer) (byte-vector target) (fixnum start end) #.*fixnum-optimization*)
  (when (> end start)
    (let ((stream (buffer-stream buffer)))
      (declare (buffered-stream stream))
      (let ((tindex start)) (declare (fixnum tindex))
        (loop (with-stream-input-buffer (source sindex limit) stream
                (declare (simple-base-string source) (fixnum sindex limit))
                (loop while (< sindex limit) do
                      (setf (aref target tindex)
                            (char-code (sbchar source sindex)))
                      (incf sindex)
                      (incf tindex)
                      (when (= tindex end)
                        (return-from scan-sequence))))
              (unless (stream-fill-buffer stream)
                (error "Stream overflow in bson:scan-sequence.")))))))

(defun scan-bytes (buffer length)
  "scan length bytes into buffer-bytes"
  (declare (buffer buffer) (fixnum length) #.*fixnum-optimization*)
  (let ((stream (buffer-stream buffer))
        (bytes  (buffer-bytes buffer))
        (tindex  0)) 
    (declare (buffered-stream stream) (vector bytes) (fixnum tindex))
    (loop (with-stream-input-buffer (source sindex limit) stream
            (declare (simple-base-string source) (fixnum sindex limit))
            (loop while (< sindex limit) do
                  (setf (sys:typed-aref '(unsigned-byte 8) bytes tindex)
                        (char-code (sbchar source sindex)))
                  (incf sindex)
                  (incf tindex)
                  (when (= tindex length)
                    (return-from scan-bytes))))
          (unless (stream-fill-buffer stream)
            (error "Stream overflow in bson:scan-bytes.")))))

(defun skip-bytes (buffer length)
  "skip length bytes"
  (declare (buffer buffer) (fixnum length) #.*fixnum-optimization*)
  (let ((stream (buffer-stream buffer)))
    (declare (buffered-stream stream))
    (loop (with-stream-input-buffer (source index limit) stream
            (declare (ignore source) (fixnum index limit))
            (let ((new-index (+ index length)))
              (cond ((<= new-index limit)
                     (setq index new-index)
                     (return-from skip-bytes))
                    (t (setq index limit)
                       (setq length (- new-index limit))))))
          (unless (stream-fill-buffer stream)
            (error "Stream overflow in bson:skip-bytes.")))))

(defun scan-cstring (buffer)
  "scan a BSON cstring"
  (declare (buffer buffer) #.*fixnum-optimization*)
  (let ((stream (buffer-stream buffer)) (strings '()))
    (declare (buffered-stream stream) (list strings))
    (loop (with-stream-input-buffer (source index limit) stream
            (declare (simple-base-string source) (fixnum index limit))
            (when (< index limit)
              (loop for end of-type fixnum from index below limit
                    for flag of-type boolean = (char= (sbchar source end) #\Null)
                    until flag finally
                    (let ((string (make-array (- end index) :element-type 'base-char)))
                      (declare (simple-base-string string))
                      (loop for j of-type fixnum from index below end
                            for i of-type fixnum from 0
                            do (setf (sbchar string i) (sbchar source j)))
                      (cond (flag (setq index (1+ end))
                                  (cond (strings (push string strings)
                                                 (return-from scan-cstring (apply 'string-append (nreverse strings))))
                                        (t       (return-from scan-cstring string))))
                            (t    (setq index end)
                                  (push string strings)))))))
          (unless (stream-fill-buffer stream)
            (if strings
              (if (cdr strings)
                (return-from scan-cstring (apply 'string-append (nreverse strings)))
                (return-from scan-cstring (car strings)))
              (return-from scan-cstring ""))))))

(defun %read-byte (buffer)
  "read a single byte"
  (declare (buffer buffer) #.*fixnum-optimization*)
  (let ((stream (buffer-stream buffer)))
    (declare (buffered-stream stream))
    (loop (with-stream-input-buffer (buffer index limit) stream
            (when (< index limit)
              (return-from %read-byte (prog1 (char-code (sbchar buffer index))
                                        (incf index)))))
          (unless (stream-fill-buffer stream)
            (error "Stream overflow in bson:%read-byte.")))))

(declaim (inline %read-int32))

(defun %read-int32 (buffer)
  "read a BSON int32"
  (declare (buffer buffer) #.*optimization*)
  (scan-bytes buffer 4)
  (sys:typed-aref '(signed-byte 32) (buffer-bytes buffer) 0))

(defun %read-int64 (buffer)
  "read a BSON int64"
  (declare (buffer buffer) #.*optimization*)
  (scan-bytes buffer 8)
  (let ((bytes (buffer-bytes buffer)))
    (declare (vector bytes))
    (+ (sys:typed-aref '(unsigned-byte 32) bytes 0)
       (ash (sys:typed-aref '(signed-byte 32) bytes 4) 32))))

(declaim (inline %read-float))

(defun %read-float (buffer)
  "read a BSON float"
  (declare (buffer buffer) #.*optimization*)
  (scan-bytes buffer 8)
  (sys:typed-aref 'double-float (buffer-bytes buffer) 0))

(defun %scan-string (buffer target &key (start 0) (end (length target)))
  "scan a number of 8-bit characters into a simple-base-string"
  (declare (buffer buffer) (simple-base-string target) (fixnum start end) #.*fixnum-optimization*)
  (when (> end start)
    (let ((stream (buffer-stream buffer)))
      (declare (buffered-stream stream))
      (let ((tindex start)) (declare (fixnum tindex))
        (loop (with-stream-input-buffer (source sindex limit) stream
                (declare (simple-base-string source) (fixnum sindex limit))
                (loop while (< sindex limit) do
                      (setf (sbchar target tindex)
                            (sbchar source sindex))
                      (incf sindex)
                      (incf tindex)
                      (when (= tindex end)
                        (return-from %scan-string))))
              (unless (stream-fill-buffer stream)
                (error "Stream overflow in bson:%scan-string.")))))))

(declaim (inline scan-string))

(defun scan-string (buffer)
  "scan a BSON string"
  (declare (buffer buffer) #.*optimization*)
  (let* ((length (%read-int32 buffer))
         (result (make-array (1- length) :element-type 'base-char)))
    (declare (int32 length) (simple-base-string result))
    (%scan-string buffer result)
    (assert (= (%read-byte buffer) #x00))
    result))

(declaim (inline scan-key))

(defun scan-key (buffer)
  "scan a BSON key as a string"
  (declare (buffer buffer) #.*optimization*)
  (scan-cstring buffer))

(defvar *key-package* (find-package :keyword)
  "determines the Lisp package in which BSON keys are interned, default is :keyword.
   if nil, do not intern but keep BSON keys as strings.")

(declaim (inline scan-keyword))

(defun scan-keyword (buffer)
  "scan a BSON key, potentially interning it"
  (declare (buffer buffer) #.*optimization*)
  (let ((key (scan-key buffer)))
    (if-let (pkg *key-package*)
        (intern key pkg)
      key)))

(declaim (inline scan-integer-key))

(defun scan-integer-key (buffer)
  "scan a BSON key as an integer"
  (declare (buffer buffer) #.*optimization*)
  (parse-integer (scan-key buffer)))

(declaim (inline read-float))

(defun read-float (buffer)
  "read a BSON float element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (%read-float buffer)))

(declaim (inline read-string))

(defun read-string (buffer)
  "read a BSON string element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (scan-string buffer)))

(declaim (inline read-embedded-document))

(defun read-embedded-document (buffer)
  "read a BSON embedded document element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer) 
          (make-embedded-document :buffer buffer)))

(declaim (inline read-embedded-array))

(defun read-embedded-array (buffer)
  "read a BSON embedded array element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (make-embedded-array :buffer buffer)))

(defun read-binary (buffer)
  "read a BSON binary element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (let* ((length (%read-int32 buffer))
                 (bytes (make-byte-vector length))
                 (subtype (%read-byte buffer)))
            (scan-sequence buffer bytes)
            (make-binary :subtype subtype :bytes bytes))))

(defun read-undefined (buffer)
  "read a BSON undefined element"
  (declare (buffer buffer) #.*optimization*)
  (warn "Reading deprecated BSON undefined value.")
  (values (scan-keyword buffer) 'undefined))

(declaim (inline read-objectid))

(defun read-objectid (buffer)
  "read a BSON ObjectId element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (let ((bytes (make-byte-vector 12)))
            (scan-sequence buffer bytes)
            (make-objectid :bytes bytes))))

(declaim (inline read-false))

(defun read-false (buffer)
  "read a BSON Boolean false element"
  (declare (buffer buffer) #.*fixnum-optimization*)
  (values (scan-keyword buffer)
          (progn (assert (= (%read-byte buffer) #x00)) 'false)))

(declaim (inline read-true))

(defun read-true (buffer)
  "read a BSON Boolean true element"
  (declare (buffer buffer) #.*fixnum-optimization*)
  (values (scan-keyword buffer)
          (progn (assert (= (%read-byte buffer) #x01)) 'true)))

(declaim (inline read-boolean))

(defun read-boolean (buffer)
  "read a BSON Boolean value element"
  (declare (buffer buffer) #.*fixnum-optimization*)
  (values (scan-keyword buffer)
          (if (= (%read-byte buffer) #x00) 'false 'true)))

(declaim (inline read-datetime))

(defun read-datetime (buffer)
  "read a BSON datetime element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (multiple-value-bind
              (seconds milliseconds)
              (floor (%read-int64 buffer) 1000)
            (make-datetime :seconds (- seconds +utc-offset+)
                           :milliseconds milliseconds))))

(declaim (inline read-null))

(defun read-null (buffer)
  "read a BSON null element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer) 'null))

(declaim (inline read-regular-expression))

(defun read-regular-expression (buffer)
  "read a BSON regular expression element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (make-regular-expression
           :pattern (scan-cstring buffer)
           :options (scan-cstring buffer))))

(defun read-dbpointer (buffer)
  "read a BSON dbpointer element"
  (declare (buffer buffer) #.*optimization*)
  (warn "Reading deprecated BSON DBPointer value.")
  (values (scan-keyword buffer)
          (let ((string (scan-string buffer))
                (bytes (make-byte-vector 12)))
            (scan-sequence buffer bytes)
            (make-dbpointer :string string :bytes bytes))))

(defun read-javascript (buffer)
  "read a BSON JavaScript element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (make-javascript :string (scan-string buffer))))

(defun read-symbol (buffer &optional (package :keyword))
  "read a BSON symbol element"
  (declare (buffer buffer) #.*optimization*)
  (warn "Reading deprecated BSON symbol value.")
  (values (scan-keyword buffer)
          (intern (scan-string buffer) package)))

(declaim (inline read-javascript-with-scope))

(defun read-javascript-with-scope (buffer)
  "read a BSON JavaScript w/ scope element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (progn
            (skip-bytes buffer 4)
            (make-javascript-with-scope
             :string (scan-string buffer)
             :scope (read-document buffer)))))

(declaim (inline read-int32))

(defun read-int32 (buffer)
  "read a BSON int32 element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (%read-int32 buffer)))

(declaim (inline read-timestamp))

(defun read-timestamp (buffer)
  "read a BSON timestamp element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (make-timestamp :value (%read-int64 buffer))))

(declaim (inline read-int64))

(defun read-int64 (buffer)
  "read a BSON int64 element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer)
          (%read-int64 buffer)))

(declaim (inline read-min))

(defun read-min (buffer)
  "read a BSON min element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer) 'min))

(declaim (inline read-max))

(defun read-max (buffer)
  "read a BSON max element"
  (declare (buffer buffer) #.*optimization*)
  (values (scan-keyword buffer) 'max))

(defun %read-nothing (buffer)
  "read nothing from a BSON buffer"
  (declare (ignore buffer) #.*optimization*)
  (values nil nil))

(defun %read-error (buffer)
  "indicate a read error when attempting to read an element from a BSON buffer"
  (declare (ignore buffer) #.*optimization*)
  (error "Invalid BSON element type tag in bson:%read-error."))

(declaim (notinline read-element))

(defun read-element (buffer)
  "read a BSON element, where the type is determined by the next byte read from the buffer"
  (declare (buffer buffer) #.*fixnum-optimization*)
  (funcall (the function (svref (load-time-value
                                 (let ((table (make-array #x100 
                                                          :initial-element #'%read-error
                                                          :allocation :long-lived 
                                                          :single-thread t)))
                                   (flet ((s (index value) (setf (aref table index) value)))
                                     (s #x00 #'%read-nothing)
                                     (s #x01 #'read-float)
                                     (s #x02 #'read-string)
                                     (s #x03 #'read-embedded-document)
                                     (s #x04 #'read-embedded-array)
                                     (s #x05 #'read-binary)
                                     (s #x06 #'read-undefined)
                                     (s #x07 #'read-objectid)
                                     (s #x08 #'read-boolean)
                                     (s #x09 #'read-datetime)
                                     (s #x0A #'read-null)
                                     (s #x0B #'read-regular-expression)
                                     (s #x0C #'read-dbpointer)
                                     (s #x0D #'read-javascript)
                                     (s #x0E #'read-symbol)
                                     (s #x0F #'read-javascript-with-scope)
                                     (s #x10 #'read-int32)
                                     (s #x11 #'read-timestamp)
                                     (s #x12 #'read-int64)
                                     (s #xFF #'read-min)
                                     (s #x7F #'read-max))
                                   table)
                                 t)
                                (the byte (%read-byte buffer))))
           buffer))

(defmacro with-element-input ((buffer key value) &body body)
  "macro version of read-element"
  `(multiple-value-call (lambda (,key ,value) ,@body) (read-element ,buffer)))

(defun %read-document (buffer function)
  "read a BSON document.
   if function is nil, create a property list or association list;
   otherwise, call the function repeatedly with key/value pairs."
  (declare (buffer buffer) #.*optimization*)
  (skip-bytes buffer 4)
  (multiple-value-bind (key value) (read-element buffer)
    (if function
      (loop while key do
            (funcall function key value)
            (setf (values key value) (read-element buffer)))
      (let ((type *document-type*))
        (ecase type
          (:plist (loop while key
                        collect key
                        collect (force-value value)
                        do (setf (values key value) (read-element buffer))))
          (:alist (loop while key
                        collect (cons key (force-value value))
                        do (setf (values key value) (read-element buffer)))))))))

(defun %read-array (buffer function)
  "read a BSON array.
   if function is nil, create a vector or list;
   otherwise, call the function repeatedly with key/value pairs."
  (declare (buffer buffer) #.*optimization*)
  (skip-bytes buffer 4)
  (multiple-value-bind (key value) (read-element buffer)
    (if function
      (loop while key do
            (funcall function value)
            (setf (values key value) (read-element buffer)))
      (let ((type *array-type*))
        (ecase type
          (vector (loop with result = (make-array 8 :adjustable t :fill-pointer 0 :single-thread t)
                        while key do
                        (vector-push-extend (force-value value) result)
                        (setf (values key value) (read-element buffer))
                        finally (return result)))
          (list   (loop while key
                        collect (force-value value)
                        do (setf (values key value) (read-element buffer)))))))))

(declaim (inline force-value))

(defun force-value (value)
  "ensure the value is completely resolved, especially for the case of embedded-document and embedded-array"
  (declare #.*optimization*)
  (typecase value
    (embedded-document (%read-document (embedded-document-buffer value) nil))
    (embedded-array    (%read-array    (embedded-array-buffer value)    nil))
    (t                 value)))

(defgeneric read-document (buffer &optional function)
  (:documentation
   "read a BSON document.
    if function is nil, create a property list or association list;
    otherwise, call the function repeatedly with key/value pairs.")
  (:method ((buffer buffer) &optional function)
   (%read-document buffer function))
  (:method ((document embedded-document) &optional function)
   (%read-document (embedded-document-buffer document) function))
  (:method ((document embedded-array) &optional function)
   (%read-document (embedded-array-buffer document) function)))

(defmacro with-document-input ((buffer key value) &body body)
  "macro version of read-document, implicitly creating a function"
  `(read-document ,buffer (lambda (,key ,value) ,@body)))

(defgeneric read-array (buffer &optional function)
  (:documentation
   "read a BSON array.
    if function is nil, create a property list or association list;
    otherwise, call the function repeatedly with key/value pairs.")
  (:method ((buffer buffer) &optional function)
   (%read-array buffer function))
  (:method ((document embedded-array) &optional function)
   (%read-array (embedded-array-buffer document) function)))

(defmacro with-array-input ((buffer value) &body body)
  "macro version of read-array, implicitly creating a function"
  `(read-array ,buffer (lambda (,value) ,@body)))

(defun read-binary-document (buffer)
  "read a BSON document as a byte-vector"
  (declare (buffer buffer) #.*optimization*)
  (let* ((length (%read-int32 buffer))
         (byte-vector (make-byte-vector length)))
    (setf (aref byte-vector 0) (sys:typed-aref '(unsigned-byte 8) (buffer-bytes buffer) 0)
          (aref byte-vector 1) (sys:typed-aref '(unsigned-byte 8) (buffer-bytes buffer) 1)
          (aref byte-vector 2) (sys:typed-aref '(unsigned-byte 8) (buffer-bytes buffer) 2)
          (aref byte-vector 3) (sys:typed-aref '(unsigned-byte 8) (buffer-bytes buffer) 3))
    (scan-sequence buffer byte-vector :start 4)
    byte-vector))

(defun read-binary-string-document (buffer)
  "read a BSON document as a binary simple-base-string"
  (declare (buffer buffer) #.*optimization*)
  (let* ((length (%read-int32 buffer))
         (string (make-array length :element-type 'base-char)))
    (setf (sbchar string 0) (code-char (sys:typed-aref '(unsigned-byte 8) (buffer-bytes buffer) 0))
          (sbchar string 1) (code-char (sys:typed-aref '(unsigned-byte 8) (buffer-bytes buffer) 1))
          (sbchar string 2) (code-char (sys:typed-aref '(unsigned-byte 8) (buffer-bytes buffer) 2))
          (sbchar string 3) (code-char (sys:typed-aref '(unsigned-byte 8) (buffer-bytes buffer) 3)))
    (%scan-string buffer string :start 4)
    string))

(declaim (inline skip-document))

(defun skip-document (buffer)
  "skip the next BSON document"
  (declare (buffer buffer) #.*optimization*)
  (skip-bytes buffer (- (the int32 (%read-int32 buffer)) 4)))

(defun skip-value (value)
  "skip the value, especially for the case of embedded-document and embedded-array"
  (declare #.*optimization*)
  (typecase value
    (embedded-document (skip-document (embedded-document-buffer value)))
    (embedded-array    (skip-document (embedded-array-buffer value))))
  (values))

(declaim (inline %read-document-batch read-document-batch))

(defun %read-document-batch (buffer n)
  "call read-document n times, and collect the resulting documents in a list"
  (loop repeat n collect (read-document buffer)))

(defun read-document-batch (&optional (read 'read-document))
  "call the given read function n times, and collect the resulting documents in a list"
  (if (or (eq read 'read-document)
          (eq read #'read-document))
    '%read-document-batch
    (lambda (buffer n)
      (loop repeat n collect (funcall read buffer)))))

(define-compiler-macro read-document-batch (&whole form &optional read)
  (if (or (eq read 'read-document)
          (eq read #'read-document))
    ''%read-document-batch
    form))
