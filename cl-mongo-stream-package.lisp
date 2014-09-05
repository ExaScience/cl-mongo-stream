(in-package :cl-user)

(defpackage #:cl-mongo-stream
  (:use #:common-lisp #:named-readtables #:stream #:string-case)
  (:shadow #:count #:delete #:find)
  (:import-from #:lispworks #:string-append)
  (:nicknames #:ms)
  (:export 
   #:without
   
   ;; wire protocol

   #:write-message-header

   #:+op-reply+
   #:+op-message+
   #:+op-update+
   #:+op-insert+
   #:+reserved+
   #:+op-query+
   #:+op-get-more+
   #:+op-delete+
   #:+op-kill-cursors+

   #:update #:+upsert+ #:+multi-update+
   #:insert #:+continue-on-error+

   #:query
   #:+tailable-cursor+
   #:+slave-ok+
   #:+oplog-replay+
   #:+no-cursor-timeout+
   #:+await-data+
   #:+exhaust+
   #:+partial+

   #:get-more
   #:delete #:+single-remove+
   #:kill-cursors
   #:message

   #:reply #:make-reply #:reply-p
   #:reply-length
   #:reply-request
   #:reply-response
   #:reply-opcode
   #:reply-flags
   #:reply-cursor
   #:reply-start
   #:reply-returned
   #:reply-documents
   #:+cursor-not-found+
   #:+query-failure+
   #:+shard-config-stale+
   #:+await-capable+

   #:read-reply-header #:read-reply
   #:fetch-reply-header #:fetch-reply

   #:mongo-connect #:mongo-close

   ;; CRUD and other commands

   #:make-query
   #:cursor #:make-cursor #:cursor-p
   #:cursor-documents
   #:cursor-buffer
   #:cursor-exhaust
   #:cursor-database
   #:cursor-collection
   #:cursor-id
   #:cursor-return
   #:cursor-read
   #:has-next #:next #:get-more-cursor

   #:find #:find-one
   #:bulk-insert
   #:create #:ensure-index #:drop-indexes #:drop-database #:get-last-error #:coll-mod #:count #:distinct #:copydb

   #:map-document-stream #:map-document-batches #:map-documents
   #:with-document-stream #:with-document-batches #:with-documents))
