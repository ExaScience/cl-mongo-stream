(in-package :bson)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defvar *optimization*
    '(optimize (speed 3) (space 0) (debug 1) (safety 0)
               (compilation-speed 0)))

  (defvar *fixnum-optimization*
    '(optimize (speed 3) (space 0) (debug 1) (safety 0)
               (compilation-speed 0) (hcl:fixnum-safety 0))))

(defconstant +chunk-size+ (expt 2 13)
  "size of BSON buffer chunks")

(declaim (inline make-buffer buffer-p))

(defstruct (buffer (:constructor make-buffer (&key (stream nil))))
  "a BSON buffer consists of a vector of chunks, each of which is a simple-base-string,
   which in LispWorks are used to represent arrays of 8-bit bytes internally.
   pos is an absolute position into the chunks.
   bytes is a typed-aref-vector used for low-level datatype conversion.
   stream is an input and/or output stream."
  (pos 0 :type fixnum)
  (str #() :type simple-vector)
  (bytes (sys:make-typed-aref-vector 8) :type vector)
  (stream nil :type (or cl:null buffered-stream)))

(defun reinitialize-buffer (buffer &key (stream nil))
  "reset the position and output stream"
  (setf (buffer-pos buffer) 0
        (buffer-stream buffer) stream)
  buffer)

(defun ensure-str (buf old-str n)
  "ensure the buffer holds n chunks"
  (declare (buffer buf) (simple-vector old-str) (fixnum n) #.*fixnum-optimization*)
  (let ((new-str (make-array n)))
    (declare (simple-vector new-str))
    (loop for i of-type fixnum below (length old-str)
          do (setf (svref new-str i) (svref old-str i)))
    (loop for i of-type fixnum from (length old-str) below (length new-str)
          do (setf (svref new-str i) (make-array +chunk-size+ :element-type 'base-char)))
    (setf (buffer-str buf) new-str)))

(declaim (inline ensure-chunk))

(defun ensure-chunk (buf hi)
  "ensure the buffer holds enough chunks so it can be indexed with hi as a chunk index"
  (declare (buffer buf) (fixnum hi) #.*fixnum-optimization*)
  (let ((str (buffer-str buf)))
    (declare (simple-vector str))
    (svref (if (< hi (length str)) str
             (ensure-str buf str (1+ hi))) hi)))

(defmethod print-object ((buffer buffer) stream)
  "pretty printing of buffers"
  (print-unreadable-object (buffer stream :type t :identity t)
    (format stream ":POS ~S :STR ~S :STREAM ~S"
            (buffer-pos buffer)
            (unless (zerop (length (buffer-str buffer))) "...")
            (buffer-stream buffer))))

(defun buffer-push (element buf)
  "push a byte to a buffer"
  (declare (byte element) (buffer buf) #.*fixnum-optimization*)
  (let ((pos (buffer-pos buf)))
    (declare (fixnum pos))
    (multiple-value-bind (hi lo) (floor pos +chunk-size+)
      (declare (fixnum hi lo))
      (let ((chunk (ensure-chunk buf hi)))
        (declare (simple-base-string chunk))
        (setf (sbchar chunk lo) (code-char element))
        (setf (buffer-pos buf) (1+ pos))))))

(defun buffer-push-char (char buf)
  "push an 8-bit character to a buffer"
  (declare (base-char char) (buffer buf) #.*fixnum-optimization*)
  (let ((pos (buffer-pos buf)))
    (declare (fixnum pos))
    (multiple-value-bind (hi lo) (floor pos +chunk-size+)
      (declare (fixnum hi lo))
      (let ((chunk (ensure-chunk buf hi)))
        (declare (simple-base-string chunk))
        (setf (sbchar chunk lo) char)
        (setf (buffer-pos buf) (1+ pos))))))

(defun slow-buffer-extend (byte-vector buf pos hi lo chunk)
  "extend buffer by a byte-vector, slow path"
  (declare (byte-vector byte-vector) (buffer buf)
           (fixnum pos hi lo) (simple-base-string chunk)
           #.*fixnum-optimization*)
  (loop with source of-type fixnum = 0 do
        (loop for target of-type fixnum from lo below +chunk-size+ do
              (setf (sbchar chunk target)
                    (code-char (aref byte-vector source)))
              (when (= (incf source) (length byte-vector))
                (setf (buffer-pos buf) (+ pos (length byte-vector)))
                (return-from slow-buffer-extend)))
        (incf hi) (setq lo 0)
        (setq chunk (ensure-chunk buf hi))))

(declaim (inline buffer-extend))

(defun buffer-extend (byte-vector buf)
  "extend buffer by a byte-vector"
  (declare (byte-vector byte-vector) (buffer buf) #.*fixnum-optimization*)
  (let ((pos (buffer-pos buf)))
    (declare (fixnum pos))
    (multiple-value-bind (hi lo) (floor pos +chunk-size+)
      (declare (fixnum hi lo))
      (let ((chunk (ensure-chunk buf hi)))
        (declare (simple-base-string chunk))
        (if (<= (+ lo (length byte-vector)) +chunk-size+)
          (loop for i of-type fixnum below (length byte-vector)
                for j of-type fixnum from lo do
                (setf (aref chunk j)
                      (code-char (aref byte-vector i)))
                finally 
                (setf (buffer-pos buf) (+ pos (length byte-vector)))
                (return (values)))
          (slow-buffer-extend byte-vector buf pos hi lo chunk))))))

(defun slow-buffer-extend-string (string buf pos hi lo chunk)
  "extend buffer by an 8-bit character string, slow path"
  (declare (simple-base-string string) (buffer buf)
           (fixnum pos hi lo) (simple-base-string chunk)
           #.*fixnum-optimization*)
  (loop with source of-type fixnum = 0 do
        (loop for target of-type fixnum from lo below +chunk-size+ do
              (setf (sbchar chunk target)
                    (sbchar string source))
              (when (= (incf source) (length string))
                (setf (buffer-pos buf) (+ pos (length string)))
                (return-from slow-buffer-extend-string)))
        (incf hi) (setq lo 0)
        (setq chunk (ensure-chunk buf hi))))

(declaim (inline buffer-extend-string))

(defun buffer-extend-string (string buf)
  "extend buffer by an 8-bit character string"
  (declare (simple-base-string string) (buffer buf) #.*fixnum-optimization*)
  (let ((pos (buffer-pos buf)))
    (declare (fixnum pos))
    (multiple-value-bind (hi lo) (floor pos +chunk-size+)
      (declare (fixnum hi lo))
      (let ((chunk (ensure-chunk buf hi)))
        (declare (simple-base-string chunk))
        (if (<= (+ lo (length string)) +chunk-size+)
          (loop for i of-type fixnum below (length string)
                for j of-type fixnum from lo do
                (setf (aref chunk j)
                      (sbchar string i))
                finally 
                (setf (buffer-pos buf) (+ pos (length string)))
                (return (values)))
          (slow-buffer-extend string buf pos hi lo chunk))))))

(defun slow-buffer-extend-bytes (bytes n buf pos hi lo chunk)
  "extend buffer by n bytes of a typed-aref-vector, slow path"
  (declare (vector bytes) (fixnum n) (buffer buf)
           (fixnum pos hi lo) (simple-base-string chunk)
           #.*fixnum-optimization*)
  (loop with source of-type fixnum = 0 do
        (loop for target of-type fixnum from lo below +chunk-size+ do
              (setf (aref chunk target)
                    (code-char (sys:typed-aref '(unsigned-byte 8) bytes source)))
              (when (= (incf source) n)
                (setf (buffer-pos buf) (+ pos n))
                (return-from slow-buffer-extend-bytes)))
        (incf hi) (setq lo 0)
        (setq chunk (ensure-chunk buf hi))))

(declaim (inline buffer-extend-bytes))

(defun buffer-extend-bytes (bytes n buf)
  "extend buffer by n bytes of a typed-aref-vector"
  (declare (vector bytes) (fixnum n) (buffer buf) #.*fixnum-optimization*)
  (let ((pos (buffer-pos buf)))
    (declare (fixnum pos))
    (multiple-value-bind (hi lo) (floor pos +chunk-size+)
      (declare (fixnum hi lo))
      (let ((chunk (ensure-chunk buf hi)))
        (declare (simple-base-string chunk))
        (if (<= (+ lo n) +chunk-size+)
          (loop for i of-type fixnum below n
                for j of-type fixnum from lo do
                (setf (aref chunk j)
                      (code-char (sys:typed-aref '(unsigned-byte 8) bytes i)))
                finally 
                (setf (buffer-pos buf) (+ pos n))
                (return (values)))
          (slow-buffer-extend-bytes bytes n buf pos hi lo chunk))))))

(defun flush-buffer (buf)
  "flush current contents of the buffer to its output stream; reset the buffer position"
  (declare (buffer buf) #.*fixnum-optimization*)
  (let ((pos (buffer-pos buf))
        (str (buffer-str buf))
        (stream (buffer-stream buf)))
    (declare (fixnum pos) (simple-vector str) (buffered-stream stream))
    (multiple-value-bind (hi lo) (floor pos +chunk-size+)
      (declare (fixnum hi lo))
      (loop for i of-type fixnum below hi
            do (stream-write-buffer stream (svref str i) 0 +chunk-size+))
      (when (> lo 0) (stream-write-buffer stream (svref str hi) 0 lo))))
  (setf (buffer-pos buf) 0))

(defun buffer-to-byte-vector (buf)
  "copy currenty contents of the buffer to a newly created byte-vector"
  (declare (buffer buf) #.*fixnum-optimization*)
  (let ((pos (buffer-pos buf))
        (str (buffer-str buf)))
    (declare (fixnum pos) (simple-vector str))
    (multiple-value-bind (hi lo) (floor pos +chunk-size+)
      (declare (fixnum hi lo))
      (let ((result (make-byte-vector pos))
            (target -1))
        (declare (byte-vector result) (fixnum target))
        (loop for i of-type fixnum below hi
              for chunk of-type simple-base-string = (svref str i) do
              (loop for j of-type fixnum below +chunk-size+ do
                    (setf (aref result (incf target))
                          (char-code (sbchar chunk j)))))
        (when (> lo 0)
          (loop with chunk of-type simple-base-string = (svref str hi)
                for j of-type fixnum below lo do
                (setf (aref result (incf target))
                      (char-code (sbchar chunk j)))))
        (setf (buffer-pos buf) 0)
        result))))
