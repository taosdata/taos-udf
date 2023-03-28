#include <taospyudf.h>

#include <pybind11/embed.h>
#include <pybind11/pybind11.h>

#include <plog/Initializers/RollingFileInitializer.h>
#include <plog/Log.h>

#include <dlfcn.h>

#include <fstream>
#include <iostream>

#include <regex>

#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace py = pybind11;

struct UdfDataBlock {
  UdfDataBlock(SUdfDataBlock *block) : _dataBlock(block) {}

  py::tuple shape() {
    py::tuple t = py::make_tuple(_dataBlock->numOfRows, _dataBlock->numOfCols);
    return t;
  }

  py::object data(int i, int j) {
    if (i < 0 || i > _dataBlock->numOfRows || j < 0 || j > _dataBlock->numOfCols) {
      throw py::index_error("out of range for data call");
    }
    SUdfColumn *col = _dataBlock->udfCols[j];
    if (udfColDataIsNull(col, i)) {
      return py::none();
    }
    char *data = udfColDataGetData(col, i);

    switch (col->colMeta.type) {
      case TSDB_DATA_TYPE_TIMESTAMP:
        return py::int_(*(int64_t *)data);

      case TSDB_DATA_TYPE_TINYINT:
        return py::int_(*(int8_t *)data);
      case TSDB_DATA_TYPE_UTINYINT:
        return py::int_(*(uint8_t *)data);
      case TSDB_DATA_TYPE_SMALLINT:
        return py::int_(*(int16_t *)data);
      case TSDB_DATA_TYPE_USMALLINT:
        return py::int_(*(uint16_t *)data);
      case TSDB_DATA_TYPE_INT:
        return py::int_(*(int32_t *)data);
      case TSDB_DATA_TYPE_UINT:
        return py::int_(*(uint32_t *)data);
      case TSDB_DATA_TYPE_BIGINT:
        return py::int_(*(int64_t *)data);
      case TSDB_DATA_TYPE_UBIGINT:
        return py::int_(*(uint64_t *)data);

      case TSDB_DATA_TYPE_BOOL:
        return py::bool_(*(int8_t *)data);

      case TSDB_DATA_TYPE_FLOAT:
        return py::float_(*(float *)data);
      case TSDB_DATA_TYPE_DOUBLE:
        return py::float_(*(double *)data);

      case TSDB_DATA_TYPE_NCHAR:
      case TSDB_DATA_TYPE_BINARY:
        return py::bytes(varDataVal(data), varDataLen(data));

      case TSDB_DATA_TYPE_JSON:
      default:
        throw py::type_error("unsupported python udf type");
    }
  }

  static void copyPyTuple2UdfColumn(SUdfColumn *col, py::tuple t) {
    for (int i = 0; i < t.size(); ++i) {
      py::object obj = t[i];
      if (obj.is_none()) {
        udfColDataSetNull(col, i);
      } else {
        switch (col->colMeta.type) {
          case TSDB_DATA_TYPE_TIMESTAMP: {
            int64_t c = obj.cast<int64_t>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }
          case TSDB_DATA_TYPE_TINYINT: {
            int8_t c = obj.cast<int8_t>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }
          case TSDB_DATA_TYPE_UTINYINT: {
            uint8_t c = obj.cast<uint8_t>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }
          case TSDB_DATA_TYPE_SMALLINT: {
            int16_t c = obj.cast<int16_t>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }
          case TSDB_DATA_TYPE_USMALLINT: {
            uint16_t c = obj.cast<uint16_t>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }
          case TSDB_DATA_TYPE_INT: {
            int32_t c = obj.cast<int32_t>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }
          case TSDB_DATA_TYPE_UINT: {
            uint32_t c = obj.cast<uint32_t>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }
          case TSDB_DATA_TYPE_BIGINT: {
            int64_t c = obj.cast<int64_t>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }
          case TSDB_DATA_TYPE_UBIGINT: {
            uint64_t c = obj.cast<uint64_t>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }
          case TSDB_DATA_TYPE_FLOAT: {
            float c = obj.cast<float>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }
          case TSDB_DATA_TYPE_DOUBLE: {
            double c = obj.cast<double>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }

          case TSDB_DATA_TYPE_BOOL: {
            int8_t c = obj.cast<int8_t>();
            udfColDataSet(col, i, (char *)&c, false);
            break;
          }

          case TSDB_DATA_TYPE_NCHAR:
          case TSDB_DATA_TYPE_BINARY: {
            std::string             c = obj.cast<std::string>();
            std::unique_ptr<char[]> data{new char[c.size() + VARSTR_HEADER_SIZE]};
            varDataSetLen(data.get(), c.size());
            memcpy(varDataVal(data.get()), c.data(), c.size());
            udfColDataSet(col, i, data.get(), false);
            break;
          }
          case TSDB_DATA_TYPE_JSON:
          default: {
            throw py::type_error("unsupported python udf type");
            break;
          }
        }
      }
    }
  }

  py::tuple meta(int i) {
    if (i < 0 || i > _dataBlock->numOfCols) {
      throw py::index_error("out of range for meta call");
    }
    SUdfColumn *col = _dataBlock->udfCols[i];
    return py::make_tuple(col->colMeta.type, col->colMeta.bytes, col->colMeta.scale, col->colMeta.precision);
  }

  SUdfDataBlock *_dataBlock;
};

PYBIND11_EMBEDDED_MODULE(taospyudf, m) {
  py::class_<UdfDataBlock>(m, "UdfDataBlock")
      .def("shape", &UdfDataBlock::shape)
      .def("data", &UdfDataBlock::data)
      .def("meta", &UdfDataBlock::meta);
}

class PyUdf {
 protected:
  PyUdf(const SScriptUdfInfo *udfInfo)
      : _name(udfInfo->name),
        _path(udfInfo->path),
        _funcType(udfInfo->funcType),
        _outputType(udfInfo->outputType),
        _outputLen(udfInfo->outputLen),
        _bufSize(udfInfo->bufSize) {
    std::string baseFilename = _path.substr(_path.find_last_of("/\\") + 1);
    std::string::size_type const p(baseFilename.find_last_of('.'));
    std::string stem = baseFilename.substr(0, p);
    _module = py::module_::import(stem.c_str());
  }

 public:
  virtual void loadFunctions() {
    _init = _module.attr("init");
    _destroy = _module.attr("destroy");
  }

  virtual ~PyUdf() {}

  static PyUdf *createPyUdf(const SScriptUdfInfo *udfInfo);

 protected:
  py::module_ _module;
  std::string _name;
  std::string _path;

  EUdfFuncType _funcType;
  int8_t       _outputType;
  int32_t      _outputLen;
  int32_t      _bufSize;

 private:
  py::function _init;
  py::function _destroy;
};

class PyScalarUdf : public PyUdf {
 public:
  PyScalarUdf(const SScriptUdfInfo *udfInfo) : PyUdf(udfInfo) {}

  virtual void loadFunctions() override {
    PyUdf::loadFunctions();
    _scalarProc = _module.attr("process");
  }

  int32_t scalarProc(SUdfDataBlock *dataBlock, SUdfColumn *resultCol) {
    SUdfColumnData *resultData = &resultCol->colData;
    UdfDataBlock    block(dataBlock);
    py::object      pyblk = py::cast(&block);
    py::tuple       pyTuple = _scalarProc(pyblk);
    if (pyTuple.size() != dataBlock->numOfRows) {
      PLOGE << "scalar udf. row number: " << dataBlock->numOfRows << ", result size: " << pyTuple.size();
      throw std::runtime_error("python udf scalar function shall return each result for each row.");
    }
    UdfDataBlock::copyPyTuple2UdfColumn(resultCol, pyTuple);
    resultData->numOfRows = dataBlock->numOfRows;
    return 0;
  }

 private:
  py::function _scalarProc;
};

class PyAggUdf : public PyUdf {
 public:
  PyAggUdf(const SScriptUdfInfo *udfInfo) : PyUdf(udfInfo) {}
  virtual void loadFunctions() override {
    PyUdf::loadFunctions();
    _aggStart = _module.attr("start");
    _aggProc = _module.attr("reduce");
    if (py::hasattr(_module, "merge")) {
      _aggMerge = _module.attr("merge");
    }
    _aggFinish = _module.attr("finish");
  }

  void aggStart(SUdfInterBuf *buf) {
    py::object pyobj = _aggStart();
    if (pyobj.is_none()) {
      buf->numOfResult = 0;
      return;
    }
    if (py::isinstance<py::bytes>(pyobj)) {
      py::bytes        pybuf = _aggStart();
      std::string_view sv = pybuf.cast<std::string_view>();
      memcpy(buf->buf, sv.data(), sv.size());
      buf->bufLen = sv.size();
      buf->numOfResult = 1;
    }
  }

  void aggProc(SUdfDataBlock *dataBlock, SUdfInterBuf *buf, SUdfInterBuf *newBuf) {
    UdfDataBlock block(dataBlock);
    py::object   pyblk = py::cast(&block);
    py::object   pyobjBuf;
    if (buf->numOfResult == 0) {
      pyobjBuf = py::none();
    } else {
      py::bytes pybuf(buf->buf, buf->bufLen);
      pyobjBuf = pybuf;
    }
    py::object pyobjBufOut = _aggProc(pyblk, pyobjBuf);
    if (pyobjBufOut.is_none()) {
      newBuf->numOfResult = 0;
    } else {
      py::bytes        pybufOut = pyobjBufOut;
      std::string_view sv = pybufOut.cast<std::string_view>();
      memcpy(newBuf->buf, sv.data(), sv.size());
      newBuf->bufLen = sv.size();
      newBuf->numOfResult = 1;
    }
  }

  void aggMerge(SUdfInterBuf *inputBuf1, SUdfInterBuf *inputBuf2, SUdfInterBuf *outputBuf) {
    py::object pyobjBuf1;
    py::object pyobjBuf2;
    if (inputBuf1->numOfResult == 0) {
      pyobjBuf1 = py::none();
    } else {
      py::bytes pybuf1(inputBuf1->buf, inputBuf1->bufLen);
      pyobjBuf1 = pybuf1;
    }
    if (inputBuf2->numOfResult == 0) {
      pyobjBuf2 = py::none();
    } else {
      py::bytes pybuf2(inputBuf2->buf, inputBuf2->bufLen);
      pyobjBuf2 = pybuf2;
    }

    py::object pyobjBufOut = _aggMerge(pyobjBuf1, pyobjBuf2);
    if (pyobjBufOut.is_none()) {
      outputBuf->numOfResult = 0;
    } else {
      py::bytes        pybufOut = pyobjBufOut;
      std::string_view sv = pybufOut.cast<std::string_view>();
      memcpy(outputBuf->buf, sv.data(), sv.size());
      outputBuf->bufLen = sv.size();
      outputBuf->numOfResult = 1;
    }
  }

  template <typename ResultType>
  void copyPyObj2Buf(py::object obj, SUdfInterBuf *buf) {
    ResultType c = obj.cast<ResultType>();
    memcpy(buf->buf, &c, sizeof(ResultType));
    buf->bufLen = sizeof(ResultType);
    buf->numOfResult = 1;
  }

  void aggFinish(SUdfInterBuf *buf, SUdfInterBuf *resultData) {
    py::object pyobjBuf;
    if (buf->numOfResult == 0) {
      pyobjBuf = py::none();
    } else {
      py::bytes pybuf(buf->buf, buf->bufLen);
      pyobjBuf = pybuf;
    }
    py::object obj = _aggFinish(pyobjBuf);
    if (obj.is_none()) {
      resultData->numOfResult = 0;
    } else {
      switch (_outputType) {
        case TSDB_DATA_TYPE_TIMESTAMP: {
          copyPyObj2Buf<int64_t>(obj, resultData);
          ;
          break;
        }
        case TSDB_DATA_TYPE_TINYINT: {
          copyPyObj2Buf<int8_t>(obj, resultData);
          ;
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          copyPyObj2Buf<uint8_t>(obj, resultData);
          ;
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          copyPyObj2Buf<int16_t>(obj, resultData);
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          copyPyObj2Buf<uint16_t>(obj, resultData);
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          copyPyObj2Buf<int32_t>(obj, resultData);
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          copyPyObj2Buf<uint32_t>(obj, resultData);
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          copyPyObj2Buf<int64_t>(obj, resultData);
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          copyPyObj2Buf<uint64_t>(obj, resultData);
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          copyPyObj2Buf<float>(obj, resultData);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          copyPyObj2Buf<double>(obj, resultData);
          break;
        }

        case TSDB_DATA_TYPE_BOOL: {
          copyPyObj2Buf<int8_t>(obj, resultData);
          break;
        }

        case TSDB_DATA_TYPE_NCHAR:
        case TSDB_DATA_TYPE_BINARY: {
          std::string c = obj.cast<std::string>();
          varDataSetLen(resultData->buf, c.size());
          memcpy(varDataVal(resultData->buf), c.data(), c.size());
          resultData->bufLen = c.size() + 2;
          resultData->numOfResult = 1;
          break;
        }
        case TSDB_DATA_TYPE_JSON:
        default: {
          throw py::type_error("unsupported python udf type");
          break;
        }
      }
    }
  }

 private:
  py::function _aggStart;
  py::function _aggProc;
  py::function _aggMerge;
  py::function _aggFinish;
};

PyUdf *PyUdf::createPyUdf(const SScriptUdfInfo *udfInfo) {
  if (udfInfo->funcType == UDF_FUNC_TYPE_AGG) {
    return new PyAggUdf(udfInfo);
  } else if (udfInfo->funcType == UDF_FUNC_TYPE_SCALAR) {
    return new PyScalarUdf(udfInfo);
  } else {
    throw std::invalid_argument("udf type not supported");
  }
}

int32_t doPyUdfInit(SScriptUdfInfo *udf, void **pUdfCtx) {
  PLOGD << "python udf init. path: " << udf->path << ", name: " << udf->name;
  try {
    PyUdf *pyUdf = PyUdf::createPyUdf(udf);
    pyUdf->loadFunctions();
    *pUdfCtx = pyUdf;
  } catch (std::exception &e) {
    PLOGE << "call pyUdf init function. error " << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  PLOGI << "python udf init. name " << udf->name << ". context: " << static_cast<void *>(*pUdfCtx);
  return 0;
}

int32_t doPyUdfDestroy(void *udfCtx) {
  PLOGD << "python udf destory. context: " << static_cast<void *>(udfCtx);
  try {
    PyUdf *pyUdf = static_cast<PyUdf *>(udfCtx);
    delete pyUdf;
  } catch (std::exception &e) {
    PLOGE << "call pyUdf destory function. error " << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

int32_t doPyUdfScalarProc(SUdfDataBlock *block, SUdfColumn *resultCol, void *udfCtx) {
  PLOGD << "call pyUdfScalar proc function. context " << static_cast<void *>(udfCtx) << ". rows: " << block->numOfRows;
  try {
    PyUdf       *pyUdf = static_cast<PyUdf *>(udfCtx);
    PyScalarUdf *pyScalarUdf = dynamic_cast<PyScalarUdf *>(pyUdf);
    pyScalarUdf->scalarProc(block, resultCol);
    return 0;
  } catch (std::exception &e) {
    PLOGE << "call pyUdfScalar proc function. context " << static_cast<void *>(udfCtx) << ". error: " << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

int32_t doPyUdfAggStart(SUdfInterBuf *buf, void *udfCtx) {
  PLOGD << "call pyUdfAgg start function. context " << static_cast<void *>(udfCtx);
  try {
    PyUdf    *pyUdf = static_cast<PyUdf *>(udfCtx);
    PyAggUdf *pyAggUdf = dynamic_cast<PyAggUdf *>(pyUdf);
    pyAggUdf->aggStart(buf);
  } catch (std::exception &e) {
    PLOGE << "call pyUdfAgg start function. context " << static_cast<void *>(udfCtx) << " .error: " << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

int32_t doPyUdfAggProc(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf, void *udfCtx) {
  PLOGD << "call pyAggUdf proc function. context " << static_cast<void *>(udfCtx) << ". rows: " << block->numOfRows;
  try {
    PyUdf    *pyUdf = static_cast<PyUdf *>(udfCtx);
    PyAggUdf *pyAggUdf = dynamic_cast<PyAggUdf *>(pyUdf);
    pyAggUdf->aggProc(block, interBuf, newInterBuf);
  } catch (std::exception &e) {
    PLOGE << "call pyAggUdf proc function. context " << static_cast<void *>(udfCtx) << ". error " << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

int32_t doPyUdfAggMerge(SUdfInterBuf *inputBuf1, SUdfInterBuf *inputBuf2, SUdfInterBuf *outputBuf, void *udfCtx) {
#if 0
  try {
    PyUdf    *pyUdf = static_cast<PyUdf *>(udfCtx);
    PyAggUdf *pyAggUdf = dynamic_cast<PyAggUdf *>(pyUdf);
    pyAggUdf->aggMerge(inputBuf1, inputBuf2, outputBuf);
  } catch (std::exception &e) {
    PLOGE << "call pyAggUdf merge function. error " << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
#endif
  return 0;
}

int32_t doPyUdfAggFinish(SUdfInterBuf *buf, SUdfInterBuf *resultData, void *udfCtx) {
  PLOGD << "call pyAggUdf finish function. context " << static_cast<void *>(udfCtx);
  try {
    PyUdf    *pyUdf = static_cast<PyUdf *>(udfCtx);
    PyAggUdf *pyAggUdf = dynamic_cast<PyAggUdf *>(pyUdf);
    pyAggUdf->aggFinish(buf, resultData);
  } catch (std::exception &e) {
    PLOGE << "call pyAggUdf finish function. context " << static_cast<void *>(udfCtx) << ". error " << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

std::vector<std::string> resplit(const std::string &s, const std::regex &sep_regex = std::regex{"\\s+"}) {
  std::sregex_token_iterator iter(s.begin(), s.end(), sep_regex, -1);
  std::sregex_token_iterator end;
  return {iter, end};
}

int32_t doPyOpen(SScriptUdfEnvItem *items, int numItems) {
  PLOGI << "python udf plugin open";
  if (Py_IsInitialized() == 1) {
    return TSDB_UDF_PYTHON_WRONG_STATE;
  }
  try {
    py::initialize_interpreter();
    py::module_ pySys = py::module_::import("sys");
    for (int i = 0; i < numItems; ++i) {
      if (std::string_view(items[i].name) == std::string_view("PYTHONPATH")) {
        auto paths = resplit(std::string(items[i].value), std::regex("[;:]"));
        for (auto &path : paths) {
          pySys.attr("path").attr("append")(path);
        }
      }
    }
    auto taosPyUdf = py::module::import("taospyudf");
  } catch (std::exception &e) {
    PLOGE << "python udf plugin open error. " << e.what();
    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}

int32_t doPyClose() {
  PLOGI << "python udf plugin close.";
  if (Py_IsInitialized() == 0) {
    return TSDB_UDF_PYTHON_WRONG_STATE;
  }
  try {
    py::finalize_interpreter();
  } catch (std::exception &e) {
    PLOGE << "python udf plugin close. " << e.what();

    return TSDB_UDF_PYTHON_EXEC_FAILURE;
  }
  return 0;
}
// thread pool from https://maidamai0.github.io/post/a-simple-thread-pool/
class ThreadPool {
  using task_type = std::function<void()>;

 public:
  ThreadPool(size_t num = std::thread::hardware_concurrency()) {
    for (size_t i = 0; i < num; ++i) {
      workers_.emplace_back(std::thread([this] {
        while (true) {
          task_type task;
          {
            std::unique_lock<std::mutex> lock(task_mutex_);
            task_cond_.wait(lock, [this] { return !tasks_.empty(); });
            task = std::move(tasks_.front());
            tasks_.pop();
          }
          if (!task) {
            PLOGI << "worker #" << std::this_thread::get_id() << " exited";
            push_stop_task();
            return;
          }
          task();
        }
      }));
      PLOGI << "python udf worker #" << workers_.back().get_id() << " started";
    }
  }

  ~ThreadPool() { stop(); }

  void stop() {
    push_stop_task();
    for (auto &worker : workers_) {
      if (worker.joinable()) {
        worker.join();
      }
    }

    // clear all pending tasks
    std::queue<task_type> empty{};
    std::swap(tasks_, empty);
  }

  template <typename F, typename... Args>
  auto enqueue(F &&f, Args &&...args) {
    using return_type = std::invoke_result_t<F, Args...>;
    auto task =
        std::make_shared<std::packaged_task<return_type()>>(std::bind(std::forward<F>(f), std::forward<Args>(args)...));
    auto res = task->get_future();

    {
      std::lock_guard<std::mutex> lock(task_mutex_);
      tasks_.emplace([task]() { (*task)(); });
    }
    task_cond_.notify_one();

    return res;
  }

 private:
  void push_stop_task() {
    std::lock_guard<std::mutex> lock(task_mutex_);
    tasks_.push(task_type{});
    task_cond_.notify_one();
  }

  std::vector<std::thread> workers_;
  std::queue<task_type>    tasks_;
  std::mutex               task_mutex_;
  std::condition_variable  task_cond_;
};

static ThreadPool *pythonCaller = nullptr;

int32_t pyUdfInit(SScriptUdfInfo *udf, void **pUdfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfInit, udf, pUdfCtx);
  return f.get();
}

int32_t pyUdfDestroy(void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfDestroy, udfCtx);
  return f.get();
}

int32_t pyUdfScalarProc(SUdfDataBlock *block, SUdfColumn *resultCol, void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfScalarProc, block, resultCol, udfCtx);
  return f.get();
}

int32_t pyUdfAggStart(SUdfInterBuf *buf, void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfAggStart, buf, udfCtx);
  return f.get();
}

int32_t pyUdfAggProc(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf, void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfAggProc, block, interBuf, newInterBuf, udfCtx);
  return f.get();
}

int32_t pyUdfAggMerge(SUdfInterBuf *inputBuf1, SUdfInterBuf *inputBuf2, SUdfInterBuf *outputBuf, void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfAggMerge, inputBuf1, inputBuf2, outputBuf, udfCtx);
  return f.get();
}

int32_t pyUdfAggFinish(SUdfInterBuf *buf, SUdfInterBuf *resultData, void *udfCtx) {
  auto f = pythonCaller->enqueue(doPyUdfAggFinish, buf, resultData, udfCtx);
  return f.get();
}

int32_t pyOpen(SScriptUdfEnvItem *items, int numItems) {
  dlopen("libtaospyudf.so", RTLD_LAZY | RTLD_GLOBAL);

  std::string logPath("/tmp/");
  for (int i = 0; i < numItems; ++i) {
    if (std::string_view(items[i].name) == std::string_view("LOGDIR")) {
      logPath = std::string(items[i].value);
      break;
    }
  }
  logPath += std::string("/taospyudf.log");
  plog::init(plog::info, logPath.c_str(), 50 * 1024 * 1024, 5);
  PLOGI << "taos python udf plugin open";
  // only one caller
  pythonCaller = new ThreadPool(1);
  auto f = pythonCaller->enqueue(doPyOpen, items, numItems);
  return f.get();
}

int32_t pyClose() {
  auto    f = pythonCaller->enqueue(doPyClose);
  int32_t ret = f.get();
  delete pythonCaller;
  pythonCaller = nullptr;
  dlopen("libtaospyudf.so", RTLD_LAZY | RTLD_GLOBAL);
  PLOGI << "taos python udf plugin close";
  return ret;
}
