/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <string>

#include "velox/vector/ComplexVector.h"

namespace gluten::trino {

/// Reads and writes Trino's raw page layout: an int32 block count followed by
/// each block as written by Trino's InternalBlockEncodingSerde (length-prefixed
/// encoding name, then the encoding body). This is the layout between the page
/// header and the end of a serialized page, without compression, encryption or
/// framing, which stay on the Java side.
///
/// Supported Velox types and the Trino encodings they map to:
///   BOOLEAN, TINYINT          BYTE_ARRAY
///   SMALLINT                  SHORT_ARRAY
///   INTEGER, DATE, REAL       INT_ARRAY
///   BIGINT, DOUBLE,
///   short DECIMAL             LONG_ARRAY
///   long DECIMAL              INT128_ARRAY
///   VARCHAR, VARBINARY        VARIABLE_WIDTH
///   TIMESTAMP                 LONG_ARRAY (epoch microseconds)
class TrinoPageSerde {
 public:
  static void serialize(const facebook::velox::RowVectorPtr& rows, std::string& out);

  static facebook::velox::RowVectorPtr deserialize(
      const facebook::velox::RowTypePtr& type,
      int32_t positionCount,
      const char* data,
      size_t size,
      facebook::velox::memory::MemoryPool* pool);
};

} // namespace gluten::trino
