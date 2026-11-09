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

#include "trino/TrinoPageSerde.h"

#include <bit>
#include <cstring>

#include "utils/Exception.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;

namespace gluten::trino {
namespace {

constexpr std::string_view kByteArray = "BYTE_ARRAY";
constexpr std::string_view kShortArray = "SHORT_ARRAY";
constexpr std::string_view kIntArray = "INT_ARRAY";
constexpr std::string_view kLongArray = "LONG_ARRAY";
constexpr std::string_view kInt128Array = "INT128_ARRAY";
constexpr std::string_view kVariableWidth = "VARIABLE_WIDTH";
constexpr std::string_view kRle = "RLE";
constexpr std::string_view kDictionary = "DICTIONARY";

// Trino writes all multi-byte values little-endian (airlift Slice).
static_assert(folly::kIsLittleEndian);

template <typename T>
void put(std::string& out, T value) {
  out.append(reinterpret_cast<const char*>(&value), sizeof(T));
}

void putName(std::string& out, std::string_view name) {
  put<int32_t>(out, name.size());
  out.append(name);
}

// Writes Trino's validity header: a boolean, then if true the validity bitmap
// as LSB-first 64-bit words with 1 meaning valid. Velox null bitmaps use the
// same layout, but positions come from a DecodedVector so they are rebuilt.
// Returns the number of valid positions.
vector_size_t putValidity(std::string& out, const DecodedVector& decoded, vector_size_t size) {
  if (!decoded.mayHaveNulls()) {
    put<bool>(out, false);
    return size;
  }
  std::vector<uint64_t> words(bits::nwords(size), 0);
  vector_size_t valid = 0;
  for (vector_size_t i = 0; i < size; ++i) {
    if (!decoded.isNullAt(i)) {
      bits::setBit(words.data(), i);
      ++valid;
    }
  }
  if (valid == size) {
    put<bool>(out, false);
    return size;
  }
  put<bool>(out, true);
  out.append(reinterpret_cast<const char*>(words.data()), words.size() * sizeof(uint64_t));
  return valid;
}

// Fixed-width encodings write the values of valid positions only, preceded by
// their count when the block has nulls.
template <typename From, typename To, typename Convert>
void putFixed(std::string& out, std::string_view name, const DecodedVector& decoded, vector_size_t size, Convert convert) {
  putName(out, name);
  put<int32_t>(out, size);
  auto valid = putValidity(out, decoded, size);
  bool hasNulls = valid != size;
  if (hasNulls) {
    put<int32_t>(out, valid);
  }
  out.reserve(out.size() + valid * sizeof(To));
  for (vector_size_t i = 0; i < size; ++i) {
    if (hasNulls && decoded.isNullAt(i)) {
      continue;
    }
    convert(out, decoded.valueAt<From>(i));
  }
}

template <typename From, typename To>
void putFixed(std::string& out, std::string_view name, const DecodedVector& decoded, vector_size_t size) {
  putFixed<From, To>(out, name, decoded, size, [](std::string& o, From v) { put<To>(o, static_cast<To>(v)); });
}

void putVariableWidth(std::string& out, const DecodedVector& decoded, vector_size_t size) {
  putName(out, kVariableWidth);
  put<int32_t>(out, size);
  auto valid = putValidity(out, decoded, size);
  bool hasNulls = valid != size;
  // End offsets of valid positions; Trino recovers null positions from the
  // validity bitmap.
  put<int32_t>(out, valid);
  int32_t end = 0;
  for (vector_size_t i = 0; i < size; ++i) {
    if (hasNulls && decoded.isNullAt(i)) {
      continue;
    }
    end += decoded.valueAt<StringView>(i).size();
    put<int32_t>(out, end);
  }
  out.reserve(out.size() + end);
  for (vector_size_t i = 0; i < size; ++i) {
    if (hasNulls && decoded.isNullAt(i)) {
      continue;
    }
    auto value = decoded.valueAt<StringView>(i);
    out.append(value.data(), value.size());
  }
}

void putBlock(std::string& out, const VectorPtr& vector) {
  auto size = vector->size();
  DecodedVector decoded(*vector);
  const auto& type = vector->type();
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      putFixed<bool, int8_t>(out, kByteArray, decoded, size);
      return;
    case TypeKind::TINYINT:
      putFixed<int8_t, int8_t>(out, kByteArray, decoded, size);
      return;
    case TypeKind::SMALLINT:
      putFixed<int16_t, int16_t>(out, kShortArray, decoded, size);
      return;
    case TypeKind::INTEGER:
      putFixed<int32_t, int32_t>(out, kIntArray, decoded, size);
      return;
    case TypeKind::REAL:
      putFixed<float, int32_t>(
          out, kIntArray, decoded, size, [](std::string& o, float v) { put<int32_t>(o, std::bit_cast<int32_t>(v)); });
      return;
    case TypeKind::BIGINT:
      putFixed<int64_t, int64_t>(out, kLongArray, decoded, size);
      return;
    case TypeKind::DOUBLE:
      putFixed<double, int64_t>(
          out, kLongArray, decoded, size, [](std::string& o, double v) { put<int64_t>(o, std::bit_cast<int64_t>(v)); });
      return;
    case TypeKind::HUGEINT:
      // Trino stores each value as (high, low).
      putFixed<int128_t, int128_t>(out, kInt128Array, decoded, size, [](std::string& o, int128_t v) {
        put<int64_t>(o, static_cast<int64_t>(v >> 64));
        put<int64_t>(o, static_cast<int64_t>(static_cast<uint64_t>(v)));
      });
      return;
    case TypeKind::TIMESTAMP:
      putFixed<Timestamp, int64_t>(
          out, kLongArray, decoded, size, [](std::string& o, Timestamp v) { put<int64_t>(o, v.toMicros()); });
      return;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      putVariableWidth(out, decoded, size);
      return;
    default:
      throw GlutenException("Trino page serde does not support type " + type->toString());
  }
}

class Reader {
 public:
  Reader(const char* data, size_t size) : data_(data), end_(data + size) {}

  template <typename T>
  T get() {
    check(sizeof(T));
    T value;
    std::memcpy(&value, data_, sizeof(T));
    data_ += sizeof(T);
    return value;
  }

  const char* take(size_t bytes) {
    check(bytes);
    auto start = data_;
    data_ += bytes;
    return start;
  }

  std::string_view name() {
    auto length = get<int32_t>();
    return {take(length), static_cast<size_t>(length)};
  }

  bool atEnd() const {
    return data_ == end_;
  }

 private:
  void check(size_t bytes) const {
    if (static_cast<size_t>(end_ - data_) < bytes) {
      throw GlutenException("Truncated Trino page");
    }
  }

  const char* data_;
  const char* end_;
};

// Reads the validity header into the vector's null buffer. Returns the number
// of valid positions.
vector_size_t readValidity(Reader& reader, BaseVector& vector, vector_size_t size) {
  if (!reader.get<bool>()) {
    return size;
  }
  auto words = bits::nwords(size);
  auto* nulls = vector.mutableRawNulls();
  std::memcpy(nulls, reader.take(words * sizeof(uint64_t)), words * sizeof(uint64_t));
  return bits::countNonNulls(nulls, 0, size);
}

template <typename T, typename Stored, typename Convert>
VectorPtr readFixed(Reader& reader, const TypePtr& type, memory::MemoryPool* pool, Convert convert) {
  auto size = reader.get<int32_t>();
  auto vector = BaseVector::create<FlatVector<T>>(type, size, pool);
  auto valid = readValidity(reader, *vector, size);
  bool hasNulls = valid != size;
  if (hasNulls && reader.get<int32_t>() != valid) {
    throw GlutenException("Trino page non-null count does not match validity bitmap");
  }
  auto* values = vector->mutableRawValues();
  auto* nulls = hasNulls ? vector->rawNulls() : nullptr;
  for (vector_size_t i = 0; i < size; ++i) {
    if (nulls && bits::isBitNull(nulls, i)) {
      values[i] = T{};
      continue;
    }
    values[i] = convert(reader.get<Stored>());
  }
  return vector;
}

template <typename T, typename Stored>
VectorPtr readFixed(Reader& reader, const TypePtr& type, memory::MemoryPool* pool) {
  return readFixed<T, Stored>(reader, type, pool, [](Stored v) { return static_cast<T>(v); });
}

VectorPtr readVariableWidth(Reader& reader, const TypePtr& type, memory::MemoryPool* pool) {
  auto size = reader.get<int32_t>();
  auto vector = BaseVector::create<FlatVector<StringView>>(type, size, pool);
  auto valid = readValidity(reader, *vector, size);
  if (reader.get<int32_t>() != valid) {
    throw GlutenException("Trino page offset count does not match validity bitmap");
  }
  auto* ends = reinterpret_cast<const int32_t*>(reader.take(valid * sizeof(int32_t)));
  int32_t total = valid == 0 ? 0 : ends[valid - 1];
  auto buffer = AlignedBuffer::allocate<char>(total, pool);
  std::memcpy(buffer->asMutable<char>(), reader.take(total), total);
  auto* chars = buffer->as<char>();
  auto* nulls = valid != size ? vector->rawNulls() : nullptr;
  int32_t start = 0;
  vector_size_t next = 0;
  for (vector_size_t i = 0; i < size; ++i) {
    if (nulls && bits::isBitNull(nulls, i)) {
      continue;
    }
    vector->setNoCopy(i, StringView(chars + start, ends[next] - start));
    start = ends[next++];
  }
  vector->setStringBuffers({std::move(buffer)});
  return vector;
}

VectorPtr readBlock(Reader& reader, const TypePtr& type, memory::MemoryPool* pool);

VectorPtr readEncodedBody(Reader& reader, std::string_view name, const TypePtr& type, memory::MemoryPool* pool) {
  if (name == kRle) {
    auto size = reader.get<int32_t>();
    auto value = readBlock(reader, type, pool);
    return BaseVector::wrapInConstant(size, 0, value);
  }
  if (name == kDictionary) {
    auto size = reader.get<int32_t>();
    auto dictionary = readBlock(reader, type, pool);
    auto indices = allocateIndices(size, pool);
    std::memcpy(indices->asMutable<vector_size_t>(), reader.take(size * sizeof(int32_t)), size * sizeof(int32_t));
    return BaseVector::wrapInDictionary(nullptr, std::move(indices), size, std::move(dictionary));
  }
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return readFixed<bool, int8_t>(reader, type, pool, [](int8_t v) { return v != 0; });
    case TypeKind::TINYINT:
      return readFixed<int8_t, int8_t>(reader, type, pool);
    case TypeKind::SMALLINT:
      return readFixed<int16_t, int16_t>(reader, type, pool);
    case TypeKind::INTEGER:
      return readFixed<int32_t, int32_t>(reader, type, pool);
    case TypeKind::REAL:
      return readFixed<float, int32_t>(reader, type, pool, [](int32_t v) { return std::bit_cast<float>(v); });
    case TypeKind::BIGINT:
      return readFixed<int64_t, int64_t>(reader, type, pool);
    case TypeKind::DOUBLE:
      return readFixed<double, int64_t>(reader, type, pool, [](int64_t v) { return std::bit_cast<double>(v); });
    case TypeKind::TIMESTAMP:
      return readFixed<Timestamp, int64_t>(reader, type, pool, [](int64_t v) { return Timestamp::fromMicros(v); });
    case TypeKind::HUGEINT: {
      struct HighLow {
        int64_t high;
        uint64_t low;
      };
      return readFixed<int128_t, HighLow>(
          reader, type, pool, [](HighLow v) { return (static_cast<int128_t>(v.high) << 64) | v.low; });
    }
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return readVariableWidth(reader, type, pool);
    default:
      throw GlutenException("Trino page serde does not support type " + type->toString());
  }
}

std::string_view expectedEncoding(const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
    case TypeKind::TINYINT:
      return kByteArray;
    case TypeKind::SMALLINT:
      return kShortArray;
    case TypeKind::INTEGER:
    case TypeKind::REAL:
      return kIntArray;
    case TypeKind::BIGINT:
    case TypeKind::DOUBLE:
    case TypeKind::TIMESTAMP:
      return kLongArray;
    case TypeKind::HUGEINT:
      return kInt128Array;
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY:
      return kVariableWidth;
    default:
      throw GlutenException("Trino page serde does not support type " + type->toString());
  }
}

VectorPtr readBlock(Reader& reader, const TypePtr& type, memory::MemoryPool* pool) {
  auto name = reader.name();
  if (name != kRle && name != kDictionary && name != expectedEncoding(type)) {
    throw GlutenException(
        "Trino block encoding " + std::string(name) + " does not match type " + type->toString());
  }
  return readEncodedBody(reader, name, type, pool);
}

} // namespace

void TrinoPageSerde::serialize(const RowVectorPtr& rows, std::string& out) {
  put<int32_t>(out, rows->childrenSize());
  for (const auto& child : rows->children()) {
    putBlock(out, child);
  }
}

RowVectorPtr TrinoPageSerde::deserialize(
    const RowTypePtr& type,
    int32_t positionCount,
    const char* data,
    size_t size,
    memory::MemoryPool* pool) {
  Reader reader(data, size);
  auto blockCount = reader.get<int32_t>();
  if (blockCount != static_cast<int32_t>(type->size())) {
    throw GlutenException(
        "Trino page has " + std::to_string(blockCount) + " blocks, expected " + std::to_string(type->size()));
  }
  std::vector<VectorPtr> children;
  children.reserve(blockCount);
  for (int32_t i = 0; i < blockCount; ++i) {
    children.push_back(readBlock(reader, type->childAt(i), pool));
    if (children.back()->size() != positionCount) {
      throw GlutenException("Trino block size does not match page position count");
    }
  }
  if (!reader.atEnd()) {
    throw GlutenException("Trailing bytes after Trino page");
  }
  return std::make_shared<RowVector>(pool, type, nullptr, positionCount, std::move(children));
}

} // namespace gluten::trino
