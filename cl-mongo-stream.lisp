(in-package :cl-mongo-stream)
(bson:in-bson-syntax)

(defun %without (plist &rest keys)
  (declare (dynamic-extent keys))
  (loop for (key value) on plist by 'cddr
        unless (member key keys :test 'eq)
        nconc (list key value)))

(declaim (inline without))

(defun without (plist flag &rest keys)
  "if flag is true, return a copy of the property list except for entries for the given keys"
  (declare (dynamic-extent keys))
  (if flag (apply '%without plist keys) plist))

;;; implements the MongoDB Wire Protocol
;;; see http://docs.mongodb.org/meta-driver/latest/legacy/mongodb-wire-protocol/

(declaim (inline write-message-header))

(defun write-message-header (buffer request response opcode)
  "write the standard message header, except for the preceding length"
  (bson:%write-int32 request buffer)
  (bson:%write-int32 response buffer)
  (bson:%write-int32 opcode buffer))

(defconstant +op-reply+           1 "request opcode")
(defconstant +op-message+      1000 "request opcode")
(defconstant +op-update+       2001 "request opcode")
(defconstant +op-insert+       2002 "request opcode")
(defconstant +reserved+        2003 "request opcode")
(defconstant +op-query+        2004 "request opcode")
(defconstant +op-get-more+     2005 "request opcode")
(defconstant +op-delete+       2006 "request opcode")
(defconstant +op-kill-cursors+ 2007 "request opcode")

(defconstant +upsert+       #b01 "op-update flag")
(defconstant +multi-update+ #b10 "op-update flag")

(defun update (buffer database collection selector update 
                      &key (request 0) (response 0) (flags 0))
  (bson:with-preceding-length (buffer)
    (write-message-header buffer request response +op-update+)
    (bson:%write-int32 #x00 buffer)
    (bson:buffer-extend-string database buffer)
    (bson:buffer-push-char #\. buffer)
    (bson:%write-cstring collection buffer)
    (bson:%write-int32 flags buffer)
    (bson:write-document selector buffer)
    (bson:write-document update buffer))
  (bson:flush-buffer buffer))

(defconstant +continue-on-error+ #b1 "op-insert flag")

(defun insert (buffer database collection documents 
                      &key (request 0) (response 0) (flags 0))
  (bson:with-preceding-length (buffer)
    (write-message-header buffer request response +op-insert+)
    (bson:%write-int32 flags buffer)
    (bson:buffer-extend-string database buffer)
    (bson:buffer-push-char #\. buffer)
    (bson:%write-cstring collection buffer)
    (typecase documents
      (bson:byte-vector (bson:%write-byte-vector documents buffer))
      (list             (loop for document in documents
                              do (bson:write-document document buffer)))
      (t                (funcall documents buffer))))
  (bson:flush-buffer buffer))

(defconstant +tailable-cursor    #b00000010 "op-query flag")
(defconstant +slave-ok+          #b00000100 "op-query flag")
(defconstant +oplog-replay+      #b00001000 "op-query flag")
(defconstant +no-cursor-timeout+ #b00010000 "op-query flag")
(defconstant +await-data+        #b00100000 "op-query flag")
(defconstant +exhaust+           #b01000000 "op-query flag")
(defconstant +partial+           #b10000000 "op-query flag")

(defun query (buffer database collection
                     &key (request 0) (response 0) (flags 0)
                     (skip 0) (return 0) 
                     (query #D()) (fields nil))
  (bson:with-preceding-length (buffer)
    (write-message-header buffer request response +op-query+)
    (bson:%write-int32 flags buffer)
    (bson:buffer-extend-string database buffer)
    (bson:buffer-push-char #\. buffer)
    (bson:%write-cstring collection buffer)
    (bson:%write-int32 skip buffer)
    (bson:%write-int32 return buffer)
    (bson:write-document query buffer)
    (when fields (bson:write-document fields buffer)))
  (bson:flush-buffer buffer))

(declaim (inline get-more))

(defun get-more (buffer database collection cursor 
                        &key (request 0) (response 0) (return 0))
  (bson:with-preceding-length (buffer)
    (write-message-header buffer request response +op-get-more+)
    (bson:%write-int32 #x00 buffer)
    (bson:buffer-extend-string database buffer)
    (bson:buffer-push-char #\. buffer)
    (bson:%write-cstring collection buffer)
    (bson:%write-int32 return buffer)
    (bson:%write-int64 cursor buffer))
  (bson:flush-buffer buffer))

(defconstant +single-remove+ #b1 "op-delete flag")

(defun delete (buffer database collection 
                      &key (request 0) (response 0) (flags 0)
                      (selector #D()))
  (bson:with-preceding-length (buffer)
    (write-message-header buffer request response +op-delete+)
    (bson:%write-int32 #x00 buffer)
    (bson:buffer-extend-string database buffer)
    (bson:buffer-push-char #\. buffer)
    (bson:%write-cstring collection buffer)
    (bson:%write-int32 flags buffer)
    (bson:write-document selector buffer))
  (bson:flush-buffer buffer))

(defun kill-cursors (buffer cursors &key (request 0) (response 0))
  (bson:with-preceding-length (buffer)
    (write-message-header buffer request response +op-kill-cursors+)
    (bson:%write-int32 #x00 buffer)
    (cond ((integerp cursors)
           (bson:%write-int32 1 buffer)
           (bson:%write-int64 cursors buffer))
          (t (bson:%write-int32 (length cursors) buffer)
             (etypecase cursors
               (list   (loop for cursor in cursors
                             do (bson:%write-int64 cursor buffer)))
               (vector (loop for cursor across cursors
                             do (bson:%write-int64 cursor buffer)))))))
  (bson:flush-buffer buffer))

(defun message (buffer message &key (request 0) (response 0))
  (warn "Writing deprecated MongoDB message.")
  (bson:with-preceding-length (buffer)
    (write-message-header buffer request response +op-message+)
    (bson:%write-cstring message buffer))
  (bson:flush-buffer buffer))

(declaim (inline make-reply read-reply-header read-reply))

(defconstant +reply-header-length+ (+ 4 4 4 4 4 8 4 4))

(defconstant +cursor-not-found+   #b0001 "op-reply flag")
(defconstant +query-failure+      #b0010 "op-reply flag")
(defconstant +shard-config-stale+ #b0100 "op-reply flag")
(defconstant +await-capable+      #b1000 "op-reply flag")

(defstruct (reply (:constructor make-reply
                   (&key (request 0)
                         (response 0)
                         (opcode +op-reply+)
                         (flags 0)
                         (cursor 0)
                         (start 0)
                         (returned 0)
                         (documents '())
                         (length (+ +reply-header-length+ (reduce '+ documents :key 'length)))))
                  (:constructor read-reply-header
                   (buffer &aux
                           (length   (bson:%read-int32 buffer))
                           (request  (bson:%read-int32 buffer))
                           (response (bson:%read-int32 buffer))
                           (opcode   (bson:%read-int32 buffer))
                           (flags    (bson:%read-int32 buffer))
                           (cursor   (bson:%read-int64 buffer))
                           (start    (bson:%read-int32 buffer))
                           (returned (bson:%read-int32 buffer))))
                  (:constructor read-reply
                   (buffer &aux
                           (length    (bson:%read-int32 buffer))
                           (request   (bson:%read-int32 buffer))
                           (response  (bson:%read-int32 buffer))
                           (opcode    (bson:%read-int32 buffer))
                           (flags     (bson:%read-int32 buffer))
                           (cursor    (bson:%read-int64 buffer))
                           (start     (bson:%read-int32 buffer))
                           (returned  (bson:%read-int32 buffer))
                           (documents (loop repeat returned collect (bson:read-document buffer))))))
  "result of an op-reply"
  (length      0 :type bson:int32)
  (request     0 :type bson:int32)
  (response    0 :type bson:int32)
  (opcode      0 :type bson:int32)
  (flags       0 :type bson:int32)
  (cursor      0 :type bson:int64)
  (start       0 :type bson:int32)
  (returned    0 :type bson:int32)
  (documents '() :type list))

(defun fetch-reply-header (reply buffer)
  "fetch only header for an op-reply"
  (setf (reply-length reply)   (bson:%read-int32 buffer)
        (reply-request reply)  (bson:%read-int32 buffer)
        (reply-response reply) (bson:%read-int32 buffer)
        (reply-opcode reply)   (bson:%read-int32 buffer)
        (reply-flags reply)    (bson:%read-int32 buffer)
        (reply-cursor reply)   (bson:%read-int64 buffer)
        (reply-start reply)    (bson:%read-int32 buffer)
        (reply-returned reply) (bson:%read-int32 buffer))
  reply)

(defun fetch-reply (reply buffer)
  "fetch full reply for an op-reply"
  (fetch-reply-header reply buffer)
  (setf (reply-documents reply) 
        (loop repeat (reply-returned reply)
              collect (bson:read-document buffer)))
  reply)

(declaim (inline mongo-connect mongo-close))

(defun mongo-connect (&rest args &key (host "localhost" host-p) (port 27017 port-p) &allow-other-keys)
  "connect to a MongoDB instance"
  (declare (dynamic-extent args))
  (bson:make-buffer :stream (apply 'comm:open-tcp-stream host port (without args (or host-p port-p) :host :port))))

(defun mongo-close (buffer)
  "close a MongoDB instance"
  (close (bson:buffer-stream buffer))
  (setf (bson:buffer-pos buffer) 0
        (bson:buffer-str buffer) #()
        (bson:buffer-stream buffer) nil))
