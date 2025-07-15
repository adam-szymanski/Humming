#pragma once

#include "util/io/buffered_file_input.h"
#include <string>

using namespace std;

namespace humming {

class DataFileMetadata {
private:
  string _path;
  size_t _entries_count;
  size_t _byte_size;
  int _fd = -1;

public:
  DataFileMetadata(string path, size_t entries_count, size_t byte_size)
      : _path(path), _entries_count(entries_count), _byte_size(byte_size) {
    _fd = open(path.c_str(), O_RDONLY);
    if (_fd == -1) {
      PLOGE << "could not open " << path << " because: " << strerror(errno);
      abort();
    }
  }

  DataFileMetadata(const DataFileMetadata &) = delete;
  DataFileMetadata &operator=(const DataFileMetadata &) = delete;
  DataFileMetadata(DataFileMetadata &&other) {
    *this = std::move(other);
  }
  DataFileMetadata &operator=(DataFileMetadata && other) {
    _path = std::move(other._path);
    _entries_count = other._entries_count;
    _byte_size = other._byte_size;
    _fd = other._fd;
    other._fd = -1;
    return *this;
  }
  int fd() { return _fd; }
  ~DataFileMetadata() {
    if (_fd != -1)
      close(_fd);
  }

  const string &path() const { return _path; }
  size_t entriesCount() const { return _entries_count; }
  size_t byteSize() const { return _byte_size; }
};

} // namespace humming
