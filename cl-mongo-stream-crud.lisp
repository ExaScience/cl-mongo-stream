(in-package :cl-mongo-stream)
(bson:in-bson-syntax)

(declaim (inline make-query))

(defun make-query (&key (query '()) (sort nil) (hint nil) (explain nil))
  "create a BSON document representing a MongoDB query"
  `("$query" ,query
    ,@(when sort `("$orderby" ,sort))
    ,@(when hint `("$hint" ,hint))
    ,@(when explain '("$explain" 1))))

(defstruct cursor
  "a cursor that enables fetching chunks of documents for a single MongoDB query"
  (documents             '() :type list)
  (buffer (bson:make-buffer) :type bson:buffer)
  (exhaust               nil :type boolean)
  (database               "" :type simple-base-string)
  (collection             "" :type simple-base-string)
  (id                      0 :type bson:int64)
  (return                  0 :type bson:int32)
  (read  'bson:read-document :type (or symbol function)))

(defun get-more-cursor (cursor)
  "get the next chunk of documents"
  (let ((buffer (cursor-buffer cursor)))
    (unless (cursor-exhaust cursor)
      (get-more buffer 
                (cursor-database cursor) 
                (cursor-collection cursor) 
                (cursor-id cursor) 
                :return (cursor-return cursor)))
    (let ((reply (read-reply-header buffer)))
      (declare (dynamic-extent reply))
      (assert (= (reply-opcode reply) +op-reply+))
      (assert (zerop (logand (reply-flags reply) +query-failure+)))
      (setf (cursor-id cursor) (reply-cursor reply)
            (cursor-documents cursor) (loop repeat (reply-returned reply)
                                            collect (funcall (cursor-read cursor) buffer))))))

(declaim (inline has-next))

(defun has-next (cursor)
  "are there more chunks to be fetched?"
  (loop (cond ((cursor-documents cursor)  (return-from has-next t))
              ((zerop (cursor-id cursor)) (return-from has-next nil))
              (t (get-more-cursor cursor)))))

(declaim (inline next))

(defun next (cursor)
  "get the next document from the cursor"
  (loop (cond ((cursor-documents cursor)
               (return-from next (pop (cursor-documents cursor))))
              ((zerop (cursor-id cursor))
               (error "MongoDB cursor for ~S.~S has no more next documenst."))
              (t (get-more-cursor cursor)))))

(defun find (buffer database collection &rest args
                    &key (flags 0) (skip 0) (return 0) (query #D()) (fields nil) (read 'bson:read-document read-p))
  "query many documents"
  (declare (dynamic-extent args) (ignore skip query fields))
  (apply 'query buffer database collection (without args read-p :read))
  (let ((reply (read-reply-header buffer)))
    (declare (dynamic-extent reply))
    (assert (= (reply-opcode reply) +op-reply+))
    (assert (zerop (logand (reply-flags reply) +query-failure+)))
    (make-cursor :buffer buffer
                 :exhaust (not (zerop (logand flags +exhaust+)))
                 :database database
                 :collection collection
                 :id (reply-cursor reply)
                 :return return
                 :read read
                 :documents (loop repeat (reply-returned reply)
                                  collect (funcall read buffer)))))

(defun find-one (buffer database collection &rest args
                        &key (flags 0) (skip 0) (query #D()) (fields nil) (read 'bson:read-document read-p))
  "query a single document"
  (declare (dynamic-extent args) (ignore flags skip query fields))
  (apply 'query buffer database collection :return 1 (without args read-p :read))
  (let ((reply (read-reply-header buffer)))
    (declare (dynamic-extent reply))
    (assert (= (reply-opcode reply) +op-reply+))
    (assert (zerop (logand (reply-flags reply) +query-failure+)))
    (assert (= (reply-returned reply) 1))
    (funcall read buffer)))

(defun bulk-insert (buffer database collection documents &key
                           (write-concern #D("w" 1) write-concern-p) (ordered nil))
  "bulk insert many documents"
  (find-one buffer database "$cmd"
            :query (lambda (buffer)
                     (bson:write-string "insert" collection buffer)
                     (bson:write-embedded-array "documents" documents buffer)
                     (when write-concern-p
                       (bson:write-embedded-document "writeConcern" write-concern buffer))
                     (when ordered
                       (bson:write-lisp-boolean "ordered" ordered buffer)))))

(defun create (buffer database collection &key
                      (capped nil capped-p)
                      (auto-index-id t auto-index-id-p)
                      size max
                      ;; TokuMX options
                      page-size read-page-size compression)
  "create a collection"
  (find-one buffer database "$cmd"
            :query (lambda (buffer)
                     (bson:write-string "create" collection buffer)
                     (when capped-p        (bson:write-lisp-boolean "capped" capped buffer))
                     (when auto-index-id-p (bson:write-lisp-boolean "autoIndexId" auto-index-id buffer))
                     (when size            (bson:write-number "size" size buffer))
                     (when max             (bson:write-number "max" max buffer))
                     (when page-size       (bson:write-number "pageSize" page-size buffer))
                     (when read-page-size  (bson:write-number "readPageSize" read-page-size buffer))
                     (when compression     (bson:write-string "compression" compression buffer)))
            :read 'bson:skip-document))

(defun ensure-index (buffer database collection keys &key
                            background unique name drop-dups sparse 
                            expire-after-seconds v 
                            weights default-language language-override
                            ;; TokuMX options
                            page-size read-page-size compression
                            (clustering nil clustering-p))
  "create an index on a collection"
  (insert buffer database "system.indexes"
          (lambda (buffer)
            (bson:with-document-output (buffer)
              (bson:write-embedded-document "key" keys buffer)
              (bson:write-string "ns" (string-append database #\. collection) buffer)
              (when background (bson:write-true "background" buffer))
              (when unique     (bson:write-true "unique" buffer))
              (when name       (bson:write-string "name" name buffer))
              (when drop-dups  (bson:write-true "dropDups" buffer))
              (when sparse     (bson:write-true "sparse" buffer))
              (when expire-after-seconds (bson:write-number "expireAfterSeconds" expire-after-seconds buffer))
              (when v          
                (warn "Specifying index version ~S for index ~S on ~S.~S with keys ~S.~%"
                      name database collection keys)
                (bson:write-number "v" v buffer))
              (when weights    (bson:write-embedded-document "weights" weights buffer))
              (when default-language (bson:write-string "default_language" default-language buffer))
              (when language-override (bson:write-string "language_override" language-override buffer))
              (when page-size      (bson:write-number "pageSize" page-size buffer))
              (when read-page-size (bson:write-number "readPageSize" read-page-size buffer))
              (when compression    (bson:write-string "compression" compression buffer))
              (when clustering-p   (bson:write-lisp-boolean "clustering" clustering buffer))))))

(defun drop-indexes (buffer database collection index)
  "drop indexes for a collection"
  (find-one buffer database "$cmd"
            :query (lambda (buffer)
                     (bson:write-string "dropIndexes" collection buffer)
                     (bson:write-string "index" index buffer))
            :read 'bson:skip-document))

(defun drop-database (buffer database)
  "drop a database"
  (find-one buffer database "$cmd" :query #D("dropDatabase" 1)))

(defun get-last-error (buffer database)
  "get the last error message"
  (find-one buffer database "$cmd" :query #D("getLastError" 1)))

(defun coll-mod (buffer database collection &key (use-power-of-2-sizes nil upo2s-p) index)
  "coll mod"
  (find-one buffer database "$cmd"
            :query (lambda (buffer)
                     (bson:write-string "collMod" collection buffer)
                     (when upo2s-p
                       (bson:write-lisp-boolean "usePowerOf2Sizes" use-power-of-2-sizes buffer))
                     (when index
                       (bson:write-embedded-document "index" index buffer)))
            :read 'bson:skip-document))

(defun count (buffer database collection &key query limit skip)
  "count the number of documents for a query"
  (find-one buffer database "$cmd"
            :query (lambda (buffer)
                     (bson:write-string "count" collection buffer)
                     (when query (bson:write-embedded-document "query" query buffer))
                     (when limit (bson:write-number "limit" limit buffer))
                     (when skip  (bson:write-number "skip" skip buffer)))
            :read (lambda (buffer)
                    (let (result)
                      (bson:with-document-input (buffer key value)
                        (string-case (key :default (bson:skip-value value))
                          ("n" (setq result (floor value)))))
                      result))))

(defun distinct (buffer database collection key &key query)
  "return distinct results"
  (find-one buffer database "$cmd"
            :query (lambda (buffer)
                     (bson:write-string "distinct" collection buffer)
                  (bson:write-string "key" key buffer)
                  (when query (bson:write-embedded-document "query" query buffer)))
            :read (lambda (buffer)
                    (let (result)
                      (bson:with-document-input (buffer key value)
                        (string-case (key :default (bson:skip-value value))
                          ("values" (setq result (bson:read-array value)))))
                      result))))

(defun copydb (buffer fromdb todb &key fromhost)
  "copy a database"
  (find-one buffer "admin" "$cmd"
            :query (lambda (buffer)
                     (bson:write-int32 "copydb" 1 buffer)
                     (when fromhost
                       (bson:write-string "fromhost" fromhost buffer))
                     (bson:write-string "fromdb" fromdb buffer)
                     (bson:write-string "todb" todb buffer))))

(defun map-document-stream (function buffer database collection &rest args &key 
                                     (flags 0) (skip 0) (return 0) (query #D()) (fields nil))
  "perform a MongoDB query and call the function repeatedly with a buffer a number of documents that can be read from the buffer"
  (declare (dynamic-extent args) (ignore skip query fields))
  (apply 'query buffer database collection args)
  (let ((exhaust (not (zerop (logand flags +exhaust+)))))
    (loop (let ((reply (read-reply-header buffer)))
            (declare (dynamic-extent reply))
            (assert (= (reply-opcode reply) +op-reply+))
            (assert (zerop (logand (reply-flags reply) +query-failure+)))
            (funcall function buffer (reply-returned reply))
            (when (zerop (reply-cursor reply))
              (return-from map-document-stream))
            (unless exhaust
              (get-more buffer database collection (reply-cursor reply) :return return))))))

(defmacro with-document-stream ((in n)
                                (buffer database collection &rest args &key flags skip return query fields)
                                &body body)
  "macro version of map-document-stream"
  (declare (ignore flags skip return query fields))
  `(map-document-stream (lambda (,in ,n) ,@body) ,buffer ,database ,collection ,@args))

(declaim (inline map-document-batches))

(defun map-document-batches (function buffer database collection &rest args &key 
                                      (flags 0) (skip 0) (return 0) (query #D()) (fields nil)
                                      (read (bson:read-document-batch) read-p))
  "perform a MongoDB query and call the function repeatedly with a list of documents for each call"
  (declare (dynamic-extent args) (ignore flags skip return query fields))
  (apply 'map-document-stream
         (lambda (in n) (funcall function (funcall read in n)))
         buffer database collection
         (without args read-p :read)))

(defmacro with-document-batches ((documents)
                                 (buffer database collection &rest args &key flags skip return query fields read)
                                 &body body)
  "macro version of map-document-batches"
  (declare (ignore flags skip return query fields read))
  `(map-document-batches (lambda (,documents) ,@body) ,buffer ,database ,collection ,@args))

(declaim (inline map-documents))

(defun map-documents (function buffer database collection &rest args
                               &key (flags 0) (skip 0) (return 0) (query #D()) (fields nil) (read 'bson:read-document read-p))
  "perform a MongoDB query and all the function repeatedly with one document for each call"
  (declare (dynamic-extent args) (ignore flags skip return query fields))
  (apply 'map-document-stream
         (lambda (in n) (loop repeat n do (funcall function (funcall read in))))
         buffer database collection
         (without args read-p :read)))

(defmacro with-documents ((document) 
                          (buffer database collection &rest args &key flags skip return query fields read)
                          &body body)
  "macro version of map-documents"
  (declare (ignore flags skip return query fields read))
  `(map-documents (lambda (,document) ,@body) ,buffer ,database ,collection ,@args))
