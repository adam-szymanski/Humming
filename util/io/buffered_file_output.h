#pragma once

#include <algorithm> // For std::min
#include <cstring>   // For memcpy
#include <fcntl.h>   // For file control options (O_WRONLY, O_CREAT, etc.)
#include <iostream>
#include <memory>
#include <new> // For std::bad_alloc
#include <string>
#include <unistd.h> // For open, write, close, ftruncate
#include <vector>

#include "util/io/common.h"

namespace humming::util::io {

/**
 * @class BufferedFileOutput
 * @brief Represents a file descriptor that performs buffered writes using
 * system calls with a memory-aligned buffer, compatible with O_DIRECT.
 *
 * This class maintains an in-memory buffer aligned to a sector size and writes
 * data to a file only when the buffer is full or when explicitly closed.
 */
class BufferedFileOutput {
public:
private:
  int fd_ = -1; // File descriptor, -1 indicates not open
  size_t buffer_size_;
  std::unique_ptr<char, AlignedBufferDeleter> buffer_;
  size_t current_buffer_pos_ = 0;
  off_t total_bytes_written_ = 0;  // Tracks the true file size for O_DIRECT
  bool direct_io_enabled_ = false; // Flag to check if O_DIRECT is used

  /**
   * @brief Flushes the internal buffer to the file.
   * Assumes the buffer is full when called, which is required for O_DIRECT.
   * @return 0 on success, -1 on failure.
   */
  int flush() {
    if (fd_ == -1 || current_buffer_pos_ == 0) {
      return 0; // Nothing to flush or file not open
    }

    size_t total_bytes_written_in_call = 0;
    while (total_bytes_written_in_call < current_buffer_pos_) {
      ssize_t bytes_written_this_call =
          ::write(fd_, buffer_.get() + total_bytes_written_in_call,
                  current_buffer_pos_ - total_bytes_written_in_call);

      if (bytes_written_this_call < 0) {
        perror("Error writing to file during flush");
        return -1;
      }
      total_bytes_written_in_call += bytes_written_this_call;
    }

    current_buffer_pos_ = 0; // Reset buffer position
    return 0;
  }

public:
  /**
   * @brief Constructs a BufferedFileOutput object with an aligned buffer.
   * @param buffer_size The desired minimum size of the internal buffer.
   * The actual size will be rounded up to a multiple of k_sector_size.
   */
  explicit BufferedFileOutput(size_t buffer_size)
      : buffer_size_(calculate_aligned_size(buffer_size)),
        buffer_(allocate_aligned_buffer(buffer_size_)) {}

  /**
   * @brief Destructor. Ensures the buffer is flushed and the file is closed
   * correctly.
   */
  ~BufferedFileOutput() { close(); }

  // Disable copy constructor and copy assignment
  BufferedFileOutput(const BufferedFileOutput &) = delete;
  BufferedFileOutput &operator=(const BufferedFileOutput &) = delete;

  /**
   * @brief Opens a file for writing.
   * @param file_path The path to the file.
   * @param use_direct_io If true, opens the file with the O_DIRECT flag.
   * @return 0 on success, -1 on failure.
   */
  int open(const std::string &file_path, bool use_direct_io = false) {
    if (fd_ != -1) {
      std::cerr << "Error: A file is already open." << std::endl;
      return -1;
    }

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (use_direct_io) {
      flags |= O_DIRECT;
      direct_io_enabled_ = true;
    }

    fd_ = ::open(file_path.c_str(), flags, 0644);
    if (fd_ == -1) {
      perror("Error opening file");
      return -1;
    }
    return 0;
  }

  /**
   * @brief Writes data to the buffer, flushing to file if necessary.
   * @param data Pointer to the data to be written.
   * @param bytes The number of bytes to write.
   * @return The number of bytes written, or -1 on error.
   */
  ssize_t write(const char *data, size_t bytes) {
    if (fd_ == -1) {
      std::cerr << "Error: File is not open." << std::endl;
      return -1;
    }

    size_t bytes_remaining = bytes;
    const char *current_data_pos = data;

    while (bytes_remaining > 0) {
      size_t space_in_buffer = buffer_size_ - current_buffer_pos_;
      size_t bytes_to_copy = std::min(bytes_remaining, space_in_buffer);

      memcpy(buffer_.get() + current_buffer_pos_, current_data_pos,
             bytes_to_copy);
      current_buffer_pos_ += bytes_to_copy;
      current_data_pos += bytes_to_copy;
      bytes_remaining -= bytes_to_copy;

      if (current_buffer_pos_ == buffer_size_) {
        if (flush() == -1) {
          total_bytes_written_ += (bytes - bytes_remaining);
          return -1;
        }
      }
    }
    total_bytes_written_ += bytes;
    return bytes;
  }

  template <typename T> int writeSimple(T &val) {
    return write((const char *)&val, sizeof(val));
  }

  int writeString(const std::string &s) {
    size_t size = s.size();
    if (int ret = writeSimple(size); ret <= 0)
      return ret;
    if (int ret = write(s.data(), size); ret <= 0)
      return ret;
    return 0;
  }

  /**
   * @brief Flushes any remaining data and closes the file. Handles O_DIRECT
   * requirements.
   * @return 0 on success, -1 on failure.
   */
  int close() {
    if (fd_ == -1) {
      return 0;
    }

    int result = 0;

    if (direct_io_enabled_) {
      if (current_buffer_pos_ > 0) {
        size_t aligned_write_size = calculate_aligned_size(current_buffer_pos_);
        memset(buffer_.get() + current_buffer_pos_, 0,
               aligned_write_size - current_buffer_pos_);

        if (::write(fd_, buffer_.get(), aligned_write_size) < 0) {
          perror("Error on final padded write for O_DIRECT");
          result = -1;
        }
      }
      if (result == 0 && ftruncate(fd_, total_bytes_written_) != 0) {
        perror("Error truncating file for O_DIRECT");
        result = -1;
      }
    } else {
      if (flush() == -1) {
        result = -1;
      }
    }

    if (::close(fd_) != 0 && result == 0) {
      perror("Error closing file");
      result = -1;
    }

    fd_ = -1;
    return result;
  }
};

} // namespace humming::util::io
