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

// JNI entry points for org.apache.gluten.trino.jni.TrinoNative. This library
// is loaded by the Trino worker instead of libvelox.so, whose JNI_OnLoad
// requires Spark classes. Pages cross the boundary in Trino's raw page layout
// (see TrinoPageSerde.h), prefixed by an int32 position count.

#include <jni.h>

#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "compute/ResultIterator.h"
#include "compute/Runtime.h"
#include "compute/VeloxBackend.h"
#include "compute/WholeStageResultIterator.h"
#include "config/GlutenConfig.h"
#include "config/VeloxConfig.h"
#include "memory/AllocationListener.h"
#include "memory/MemoryManager.h"
#include "memory/VeloxColumnarBatch.h"
#include "memory/VeloxMemoryManager.h"
#include "substrait/SubstraitParser.h"
#include "substrait/SubstraitToVeloxPlan.h"
#include "substrait/type.pb.h"
#include "threads/ThreadInitializer.h"
#include "threads/ThreadManager.h"
#include "trino/TrinoPageSerde.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#ifdef GLUTEN_ENABLE_GPU
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/PrestoAggregateFunctions.h"
#include "velox/experimental/cudf/expression/ExpressionEvaluator.h"
#include "velox/experimental/cudf/expression/PrestoFunctions.h"
#include "velox/experimental/ucx-exchange/Communicator.h"
#endif

using namespace facebook::velox;

namespace gluten::trino {
namespace {

// Gluten registers Spark functions without a prefix. Trino plans call the
// Presto functions, whose semantics match Trino's, under this prefix.
const std::string kPrestoPrefix = "presto.";

// The UCX exchange listens on this port; peers derive it from the worker's HTTP port plus 3.
const std::string kUcxPort = "spark.gluten.trino.ucx.port";
constexpr uint16_t kUcxPortDefault = 8084;

JavaVM* gVm = nullptr;
jclass gRuntimeExceptionClass = nullptr;
jmethodID gPageSourceNext = nullptr;

class AttachedEnv {
 public:
  AttachedEnv() {
    if (gVm->GetEnv(reinterpret_cast<void**>(&env_), JNI_VERSION_1_8) == JNI_EDETACHED) {
      if (gVm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&env_), nullptr) != JNI_OK) {
        throw GlutenException("Unable to attach thread to JVM");
      }
    }
  }

  JNIEnv* get() const {
    return env_;
  }

 private:
  JNIEnv* env_{nullptr};
};

std::string toString(JNIEnv* env, jstring value) {
  const char* chars = env->GetStringUTFChars(value, nullptr);
  std::string result(chars);
  env->ReleaseStringUTFChars(value, chars);
  return result;
}

std::vector<char> toBytes(JNIEnv* env, jbyteArray array) {
  std::vector<char> bytes(env->GetArrayLength(array));
  env->GetByteArrayRegion(array, 0, bytes.size(), reinterpret_cast<jbyte*>(bytes.data()));
  return bytes;
}

std::unordered_map<std::string, std::string> toConf(JNIEnv* env, jobjectArray keys, jobjectArray values) {
  std::unordered_map<std::string, std::string> conf;
  jsize size = env->GetArrayLength(keys);
  for (jsize i = 0; i < size; ++i) {
    auto key = static_cast<jstring>(env->GetObjectArrayElement(keys, i));
    auto value = static_cast<jstring>(env->GetObjectArrayElement(values, i));
    conf[toString(env, key)] = toString(env, value);
    env->DeleteLocalRef(key);
    env->DeleteLocalRef(value);
  }
  return conf;
}

RowTypePtr toRowType(const std::vector<char>& namedStructBytes) {
  ::substrait::NamedStruct namedStruct;
  if (!namedStruct.ParseFromArray(namedStructBytes.data(), namedStructBytes.size())) {
    throw GlutenException("Unable to parse input schema");
  }
  auto types = SubstraitParser::parseNamedStruct(namedStruct);
  std::vector<std::string> names(namedStruct.names().begin(), namedStruct.names().end());
  return ROW(std::move(names), std::move(types));
}

// Pulls pages from a Java org.apache.gluten.trino.jni.NativePageSource, which
// returns a position-count-prefixed raw page, or null at end of stream.
class JavaPageSourceIterator : public ColumnarBatchIterator {
 public:
  JavaPageSourceIterator(JNIEnv* env, jobject source, RowTypePtr type, memory::MemoryPool* pool)
      : source_(env->NewGlobalRef(source)), type_(std::move(type)), pool_(pool) {}

  ~JavaPageSourceIterator() override {
    AttachedEnv env;
    env.get()->DeleteGlobalRef(source_);
  }

  std::shared_ptr<ColumnarBatch> next() override {
    AttachedEnv attached;
    JNIEnv* env = attached.get();
    auto page = static_cast<jbyteArray>(env->CallObjectMethod(source_, gPageSourceNext));
    if (env->ExceptionCheck()) {
      jthrowable error = env->ExceptionOccurred();
      env->ExceptionClear();
      jclass throwable = env->FindClass("java/lang/Throwable");
      jmethodID toStringMethod = env->GetMethodID(throwable, "toString", "()Ljava/lang/String;");
      auto message = static_cast<jstring>(env->CallObjectMethod(error, toStringMethod));
      throw GlutenException("Trino page source failed: " + toString(env, message));
    }
    if (page == nullptr) {
      return nullptr;
    }
    auto bytes = toBytes(env, page);
    env->DeleteLocalRef(page);
    int32_t positionCount;
    std::memcpy(&positionCount, bytes.data(), sizeof(int32_t));
    auto rows = TrinoPageSerde::deserialize(
        type_, positionCount, bytes.data() + sizeof(int32_t), bytes.size() - sizeof(int32_t), pool_);
    return std::make_shared<VeloxColumnarBatch>(std::move(rows));
  }

 private:
  jobject source_;
  RowTypePtr type_;
  memory::MemoryPool* pool_;
};

struct TrinoTask {
  MemoryManager* memoryManager{nullptr};
  ThreadManager* threadManager{nullptr};
  Runtime* runtime{nullptr};
  std::shared_ptr<ResultIterator> iterator;
  std::string page;

  ~TrinoTask() {
    // The iterator holds Velox pools owned by the memory manager.
    iterator.reset();
    if (runtime) {
      Runtime::release(runtime);
    }
    if (threadManager) {
      ThreadManager::release(threadManager);
    }
    if (memoryManager) {
      MemoryManager::release(memoryManager);
    }
  }
};

TrinoTask* toTask(jlong handle) {
  if (handle == 0) {
    throw GlutenException("Trino task handle is closed");
  }
  return reinterpret_cast<TrinoTask*>(handle);
}

WholeStageResultIterator* wholeStageIterator(TrinoTask* task) {
  auto* iterator = dynamic_cast<WholeStageResultIterator*>(task->iterator->getInputIter());
  if (iterator == nullptr) {
    throw GlutenException("Native task is not a whole stage task");
  }
  return iterator;
}

void throwJava(JNIEnv* env, const std::string& message) {
  env->ThrowNew(gRuntimeExceptionClass, message.c_str());
}

#define TRINO_JNI_START try {
#define TRINO_JNI_END(fallback)       \
  }                                   \
  catch (const std::exception& e) {   \
    throwJava(env, e.what());         \
    return fallback;                  \
  }

} // namespace
} // namespace gluten::trino

using namespace gluten;
using namespace gluten::trino;

extern "C" {

jint JNI_OnLoad(JavaVM* vm, void*) {
  JNIEnv* env;
  if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
    return JNI_ERR;
  }
  gVm = vm;
  auto runtimeException = env->FindClass("java/lang/RuntimeException");
  gRuntimeExceptionClass = static_cast<jclass>(env->NewGlobalRef(runtimeException));
  auto pageSource = env->FindClass("org/apache/gluten/trino/jni/NativePageSource");
  if (pageSource == nullptr) {
    return JNI_ERR;
  }
  gPageSourceNext = env->GetMethodID(pageSource, "next", "()[B");
  if (gPageSourceNext == nullptr) {
    return JNI_ERR;
  }
  return JNI_VERSION_1_8;
}

JNIEXPORT void JNICALL Java_org_apache_gluten_trino_jni_TrinoNative_initialize( // NOLINT
    JNIEnv* env,
    jclass,
    jobjectArray confKeys,
    jobjectArray confValues) {
  TRINO_JNI_START
  VeloxBackend::create(AllocationListener::noop(), toConf(env, confKeys, confValues));
  functions::prestosql::registerAllScalarFunctions(kPrestoPrefix);
  aggregate::prestosql::registerAllAggregateFunctions(kPrestoPrefix);
#ifdef GLUTEN_ENABLE_GPU
  if (VeloxBackend::get()->getBackendConf()->get<bool>(kCudfEnabled, kCudfEnabledDefault)) {
    cudf_velox::CudfConfig::getInstance().functionNamePrefix = kPrestoPrefix;
    // registerCudf() ran inside VeloxBackend::create() with an empty prefix, so the built-in
    // arithmetic, comparison and cast functions exist only under unprefixed names.
    cudf_velox::registerBuiltinFunctions(kPrestoPrefix);
    cudf_velox::registerPrestoFunctions(kPrestoPrefix);
    cudf_velox::registerPrestoAggregateFunctions(kPrestoPrefix);
  }
  if (cudf_velox::CudfConfig::getInstance().exchange) {
    const auto port = VeloxBackend::get()->getBackendConf()->get<uint16_t>(kUcxPort, kUcxPortDefault);
    ContinueFuture started;
    auto communicator = ucx_exchange::Communicator::initAndGet(port, "", &started);
    if (communicator == nullptr) {
      throw GlutenException("Unable to start the UCX communicator on port " + std::to_string(port));
    }
    std::thread(&ucx_exchange::Communicator::run, communicator.get()).detach();
    started.wait();
    LOG(INFO) << "UCX communicator listening on port " << port;
  }
#endif
  TRINO_JNI_END()
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_trino_jni_TrinoNative_createTask( // NOLINT
    JNIEnv* env,
    jclass,
    jbyteArray plan,
    jobjectArray splitInfos,
    jobjectArray inputSources,
    jobjectArray inputSchemas,
    jobjectArray confKeys,
    jobjectArray confValues,
    jstring spillDir) {
  TRINO_JNI_START
  auto task = std::make_unique<TrinoTask>();
  task->memoryManager = MemoryManager::create(kVeloxBackendKind, AllocationListener::noop());
  task->threadManager = ThreadManager::create(kVeloxBackendKind, ThreadInitializer::noop());
  auto conf = toConf(env, confKeys, confValues);
  conf[kSubstraitDialect] = "presto";
  task->runtime = Runtime::create(kVeloxBackendKind, task->memoryManager, task->threadManager, conf);

  auto planBytes = toBytes(env, plan);
  task->runtime->parsePlan(reinterpret_cast<const uint8_t*>(planBytes.data()), planBytes.size());

  for (jsize i = 0, size = env->GetArrayLength(splitInfos); i < size; ++i) {
    auto splitInfo = static_cast<jbyteArray>(env->GetObjectArrayElement(splitInfos, i));
    auto bytes = toBytes(env, splitInfo);
    env->DeleteLocalRef(splitInfo);
    task->runtime->parseSplitInfo(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size(), i);
  }

  auto* pool = dynamic_cast<VeloxMemoryManager*>(task->memoryManager)->getLeafMemoryPool().get();
  std::vector<std::shared_ptr<ResultIterator>> inputs;
  for (jsize i = 0, size = env->GetArrayLength(inputSources); i < size; ++i) {
    auto source = env->GetObjectArrayElement(inputSources, i);
    auto schema = static_cast<jbyteArray>(env->GetObjectArrayElement(inputSchemas, i));
    auto type = toRowType(toBytes(env, schema));
    inputs.push_back(
        std::make_shared<ResultIterator>(std::make_unique<JavaPageSourceIterator>(env, source, type, pool)));
    env->DeleteLocalRef(source);
    env->DeleteLocalRef(schema);
  }

  task->iterator = task->runtime->createResultIterator(toString(env, spillDir), inputs);
  task->runtime->noMoreSplits(task->iterator.get());
  return reinterpret_cast<jlong>(task.release());
  TRINO_JNI_END(0)
}

JNIEXPORT jstring JNICALL Java_org_apache_gluten_trino_jni_TrinoNative_planString( // NOLINT
    JNIEnv* env,
    jclass,
    jlong handle) {
  TRINO_JNI_START
  auto* task = toTask(handle);
  return env->NewStringUTF(task->runtime->planString(true, task->runtime->getConfMap()).c_str());
  TRINO_JNI_END(nullptr)
}

// Returns the next page, or null when the task has no more output.
JNIEXPORT jbyteArray JNICALL Java_org_apache_gluten_trino_jni_TrinoNative_next( // NOLINT
    JNIEnv* env,
    jclass,
    jlong handle) {
  TRINO_JNI_START
  auto* task = toTask(handle);
  if (!task->iterator->hasNext()) {
    return nullptr;
  }
  auto batch = std::dynamic_pointer_cast<VeloxColumnarBatch>(task->iterator->next());
  if (batch == nullptr) {
    throw GlutenException("Expected a Velox batch from the native task");
  }
  auto rows = batch->getRowVector();
  task->page.clear();
  int32_t positionCount = rows->size();
  task->page.append(reinterpret_cast<const char*>(&positionCount), sizeof(int32_t));
  TrinoPageSerde::serialize(rows, task->page);
  auto result = env->NewByteArray(task->page.size());
  env->SetByteArrayRegion(result, 0, task->page.size(), reinterpret_cast<const jbyte*>(task->page.data()));
  return result;
  TRINO_JNI_END(nullptr)
}

// Adds a remote task's result location to the UCX exchange that reads plan input 'inputIndex'.
JNIEXPORT void JNICALL Java_org_apache_gluten_trino_jni_TrinoNative_addRemoteSplit( // NOLINT
    JNIEnv* env,
    jclass,
    jlong handle,
    jint inputIndex,
    jstring location) {
  TRINO_JNI_START
  wholeStageIterator(toTask(handle))->addRemoteSplit(ucxExchangeNodeId(inputIndex), toString(env, location));
  TRINO_JNI_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_trino_jni_TrinoNative_noMoreRemoteSplits( // NOLINT
    JNIEnv* env,
    jclass,
    jlong handle,
    jint inputIndex) {
  TRINO_JNI_START
  wholeStageIterator(toTask(handle))->noMoreRemoteSplits(ucxExchangeNodeId(inputIndex));
  TRINO_JNI_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_trino_jni_TrinoNative_noMoreOutputBuffers( // NOLINT
    JNIEnv* env,
    jclass,
    jlong handle,
    jint numBuffers) {
  TRINO_JNI_START
  wholeStageIterator(toTask(handle))->updateOutputBuffers(numBuffers, true);
  TRINO_JNI_END()
}

JNIEXPORT void JNICALL Java_org_apache_gluten_trino_jni_TrinoNative_close( // NOLINT
    JNIEnv* env,
    jclass,
    jlong handle) {
  TRINO_JNI_START
  delete toTask(handle);
  TRINO_JNI_END()
}

} // extern "C"
