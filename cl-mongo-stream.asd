(asdf:defsystem #:cl-mongo-stream
  :version "1.0"
  :author "Pascal Costanza (Intel Corporation)"
  :license
  "Copyright (c) 2014, Intel Corporation. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
 notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright
 notice, this list of conditions and the following disclaimer in the
 documentation and/or other materials provided with the distribution.
* Neither the name of Imec or Intel Corporation nor the names of its
 contributors may be used to endorse or promote products derived from
 this software without specific prior written permission."
  :components
  ((:file "cl-mongo-stream-package")
   (:file "cl-mongo-stream" :depends-on ("cl-mongo-stream-package"))
   (:file "cl-mongo-stream-crud" :depends-on ("cl-mongo-stream")))
  :depends-on ("bson" "named-readtables" "string-case"))
