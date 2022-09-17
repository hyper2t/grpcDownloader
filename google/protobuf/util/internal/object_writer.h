// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#ifndef GOOGLE_PROTOBUF_UTIL_INTERNAL_OBJECT_WRITER_H__
#define GOOGLE_PROTOBUF_UTIL_INTERNAL_OBJECT_WRITER_H__

#include <cstdint>

#include "google/protobuf/stubs/common.h"
#include "google/protobuf/port.h"
#include "absl/strings/string_view.h"

// Must be included last.
#include "google/protobuf/port_def.inc"

namespace google {
namespace protobuf {
namespace util {
namespace converter {


class DataPiece;

// An ObjectWriter is an interface for writing a stream of events
// representing objects and collections. Implementation of this
// interface can be used to write an object stream to an in-memory
// structure, protobufs, JSON, XML, or any other output format
// desired. The ObjectSource interface is typically used as the
// source of an object stream.
//
// See JsonObjectWriter for a sample implementation of ObjectWriter
// and its use.
//
// Derived classes could be thread-unsafe.
//
// TODO(xinb): seems like a prime candidate to apply the RAII paradigm
// and get rid the need to call EndXXX().
class PROTOBUF_EXPORT ObjectWriter {
 public:
  ObjectWriter(const ObjectWriter&) = delete;
  ObjectWriter& operator=(const ObjectWriter&) = delete;
  virtual ~ObjectWriter() {}

  // Starts an object. If the name is empty, the object will not be named.
  virtual ObjectWriter* StartObject(absl::string_view name) = 0;

  // Ends an object.
  virtual ObjectWriter* EndObject() = 0;

  // Starts a list. If the name is empty, the list will not be named.
  virtual ObjectWriter* StartList(absl::string_view name) = 0;

  // Ends a list.
  virtual ObjectWriter* EndList() = 0;

  // Renders a boolean value.
  virtual ObjectWriter* RenderBool(absl::string_view name, bool value) = 0;

  // Renders an 32-bit integer value.
  virtual ObjectWriter* RenderInt32(absl::string_view name, int32_t value) = 0;

  // Renders an 32-bit unsigned integer value.
  virtual ObjectWriter* RenderUint32(absl::string_view name,
                                     uint32_t value) = 0;

  // Renders a 64-bit integer value.
  virtual ObjectWriter* RenderInt64(absl::string_view name, int64_t value) = 0;

  // Renders an 64-bit unsigned integer value.
  virtual ObjectWriter* RenderUint64(absl::string_view name,
                                     uint64_t value) = 0;


  // Renders a double value.
  virtual ObjectWriter* RenderDouble(absl::string_view name, double value) = 0;
  // Renders a float value.
  virtual ObjectWriter* RenderFloat(absl::string_view name, float value) = 0;

  // Renders a string value.
  virtual ObjectWriter* RenderString(absl::string_view name,
                                     absl::string_view value) = 0;

  // Renders a bytes value.
  virtual ObjectWriter* RenderBytes(absl::string_view name,
                                    absl::string_view value) = 0;

  // Renders a Null value.
  virtual ObjectWriter* RenderNull(absl::string_view name) = 0;


  // Renders a DataPiece object to a ObjectWriter.
  static void RenderDataPieceTo(const DataPiece& data, absl::string_view name,
                                ObjectWriter* ow);


  // Indicates whether this ObjectWriter has completed writing the root message,
  // usually this means writing of one complete object. Subclasses must override
  // this behavior appropriately.
  virtual bool done() { return false; }

  void set_use_strict_base64_decoding(bool value) {
    use_strict_base64_decoding_ = value;
  }

  bool use_strict_base64_decoding() const {
    return use_strict_base64_decoding_;
  }

 protected:
  ObjectWriter() : use_strict_base64_decoding_(true) {}

 private:
  // If set to true, we use the stricter version of base64 decoding for byte
  // fields by making sure decoded version encodes back to the original string.
  bool use_strict_base64_decoding_;
};

}  // namespace converter
}  // namespace util
}  // namespace protobuf
}  // namespace google

#include "google/protobuf/port_undef.inc"

#endif  // GOOGLE_PROTOBUF_UTIL_INTERNAL_OBJECT_WRITER_H__