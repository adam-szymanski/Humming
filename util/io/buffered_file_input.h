#pragma once

#include <algorithm> // For std::min
#include <cstring>   // For memcpy
#include <fcntl.h>   // For file control options (O_RDONLY, etc.)
#include <iostream>
#include <memory>
#include <string>
#include <unistd.h> // For open, read, pread, close, lseek
#include <vector>

#include "util/io/common.h"

namespace humming::util::io {

/**
 * @class BufferedFileInput
 * @brief Represents a file descriptor that performs buffered reads using system
 * calls with a memory-aligned buffer, compatible with O_DIRECT.
 */
class BufferedFileInput {
private:
  int fd_ = -1;
  bool _fd_owner = false;
  size_t buffer_size_;
  std::unique_ptr<char, AlignedBufferDeleter> buffer_;
  char *current_data_ptr_;       // Points to the current position in the buffer
  size_t valid_bytes_in_buffer_; // How many bytes in the buffer are valid data
  bool direct_io_enabled_ = false;

  /**
   * @brief Refills the internal buffer from the file's current position.
   * @return Number of bytes read from file, 0 on EOF, -1 on error.
   */
  ssize_t fill_buffer() {
    ssize_t bytes_read = ::read(fd_, buffer_.get(), buffer_size_);
    if (bytes_read < 0) {
      perror("Error reading into buffer");
      valid_bytes_in_buffer_ = 0;
      current_data_ptr_ = buffer_.get(); // Reset on error
      return -1;
    }
    valid_bytes_in_buffer_ = bytes_read;
    current_data_ptr_ = buffer_.get();
    return bytes_read;
  }

public:
  /**
   * @brief Constructs a BufferedFileInput object with an aligned buffer.
   * @param buffer_size The desired minimum size of the internal buffer.
   */
  explicit BufferedFileInput(size_t buffer_size = k_sector_size)
      : buffer_size_(calculate_aligned_size(buffer_size)),
        buffer_(allocate_aligned_buffer(buffer_size_)),
        current_data_ptr_(buffer_.get()), valid_bytes_in_buffer_(0) {}

  ~BufferedFileInput() { close(); }

  BufferedFileInput(const BufferedFileInput &) = delete;
  BufferedFileInput &operator=(const BufferedFileInput &) = delete;

  void passFd(int fd, bool use_direct_io = false) {
    fd_ = fd;
    _fd_owner = false;
    direct_io_enabled_ = use_direct_io;
  }

  /**
   * @brief Opens a file for reading.
   * @param file_path The path to the file.
   * @param use_direct_io If true, opens the file with the O_DIRECT flag.
   * @return 0 on success, -1 on failure.
   */
  int open(const std::string &file_path, bool use_direct_io = false) {
    if (fd_ != -1) {
      std::cerr << "Error: A file is already open." << std::endl;
      return -1;
    }

    int flags = O_RDONLY;
    if (use_direct_io) {
      flags |= O_DIRECT;
      direct_io_enabled_ = true;
    } else {
      direct_io_enabled_ = false;
    }

    fd_ = ::open(file_path.c_str(), flags);
    if (fd_ == -1) {
      perror("Error opening file");
      return -1;
    }
    _fd_owner = true;
    return 0;
  }

  /**
   * @brief Closes the file.
   * @return 0 on success, -1 on failure.
   */
  int close() {
    if (fd_ == -1 || !_fd_owner)
      return 0;
    int result = ::close(fd_);
    if (result != 0) {
      perror("Error closing file");
    }
    fd_ = -1;
    return result;
  }

  /**
   * @brief Reads data from the file into a user-provided buffer.
   * @param user_buffer Buffer to store the read data.
   * @param bytes_to_read Number of bytes to read.
   * @return Number of bytes actually read, 0 on EOF, -1 on error.
   */
  ssize_t read(char *user_buffer, size_t bytes_to_read) {
    if (fd_ == -1)
      return -1;

    size_t total_bytes_read = 0;
    while (total_bytes_read < bytes_to_read) {
      size_t bytes_left_in_buffer =
          valid_bytes_in_buffer_ - (current_data_ptr_ - buffer_.get());
      if (bytes_left_in_buffer == 0) {
        ssize_t fill_result = fill_buffer();
        if (fill_result <= 0) { // Error or EOF
          return (total_bytes_read > 0) ? total_bytes_read : fill_result;
        }
        bytes_left_in_buffer = valid_bytes_in_buffer_;
      }

      size_t bytes_to_copy =
          std::min(bytes_to_read - total_bytes_read, bytes_left_in_buffer);
      memcpy(user_buffer + total_bytes_read, current_data_ptr_, bytes_to_copy);

      current_data_ptr_ += bytes_to_copy;
      total_bytes_read += bytes_to_copy;
    }
    return total_bytes_read;
  }

  template <typename T> int readSimple(T &val) {
    return read((char *)&val, sizeof(val));
  }

  int readString(std::string &s) {
    size_t size;
    if (int ret = readSimple(size); ret <= 0)
      return ret;
    s.resize(size);
    if (int ret = read(s.data(), size); ret <= 0)
      return ret;
    return sizeof(size) + size;
  }

  /**
   * @brief Reads data from a specific offset in the file (random access).
   * This method uses the internal buffer to perform aligned reads, which will
   * invalidate the state of the sequential read buffer used by the `read()`
   * method.
   * @param user_buffer Buffer to store the read data.
   * @param bytes_to_read Number of bytes to read.
   * @param offset The offset in the file to start reading from.
   * @return Number of bytes actually read, 0 on EOF, -1 on error.
   */
  ssize_t pread(char *user_buffer, size_t bytes_to_read, off_t offset) {
    if (fd_ == -1)
      return -1;

    if (!direct_io_enabled_) {
      // For non-direct I/O, we can just use the system call directly.
      return ::pread(fd_, user_buffer, bytes_to_read, offset);
    }

    bool user_buffer_is_aligned =
        (reinterpret_cast<uintptr_t>(user_buffer) % k_sector_size) == 0;
    bool read_is_aligned =
        (offset % k_sector_size) == 0 && (bytes_to_read % k_sector_size) == 0;
//    PLOGD << "user_buffer_is_aligned: " << user_buffer_is_aligned << " read_is_aligned: " << read_is_aligned;
    if (user_buffer_is_aligned && read_is_aligned && bytes_to_read > 0) {
//      PLOGD << "direct pread";
      return ::pread(fd_, user_buffer, bytes_to_read, offset);
    }

    // O_DIRECT case: use the internal pre-allocated buffer in a loop.
    size_t total_bytes_copied_to_user = 0;
    off_t current_file_offset = offset;

    while (total_bytes_copied_to_user < bytes_to_read) {
      off_t aligned_offset =
          (current_file_offset / k_sector_size) * k_sector_size;
      ssize_t bytes_read_from_syscall =
          ::pread(fd_, buffer_.get(), buffer_size_, aligned_offset);

      if (bytes_read_from_syscall < 0) {
        perror("pread failed");
        return -1;
      }
      if (bytes_read_from_syscall == 0)
        break;

      off_t data_start_in_buffer_offset = current_file_offset - aligned_offset;
      if (bytes_read_from_syscall <= data_start_in_buffer_offset)
        break;

      size_t bytes_available_in_buffer =
          bytes_read_from_syscall - data_start_in_buffer_offset;
      size_t bytes_needed_by_user = bytes_to_read - total_bytes_copied_to_user;
      size_t bytes_to_copy =
          std::min(bytes_needed_by_user, bytes_available_in_buffer);

      memcpy(user_buffer + total_bytes_copied_to_user,
             buffer_.get() + data_start_in_buffer_offset, bytes_to_copy);

      total_bytes_copied_to_user += bytes_to_copy;
      current_file_offset += bytes_to_copy;
    }

    valid_bytes_in_buffer_ = 0;
    current_data_ptr_ = buffer_.get();

    return total_bytes_copied_to_user;
  }

  /**
   * @brief Repositions the file offset for subsequent `read()` calls.
   * This invalidates the internal read buffer. For O_DIRECT, it performs
   * an aligned seek and pre-fills the buffer.
   * @param offset The offset to seek to.
   * @param whence The positioning directive (SEEK_SET, SEEK_CUR, SEEK_END).
   * @return The resulting offset location as measured in bytes from the
   * beginning of the file, or -1 on error.
   */
  off_t seek(off_t offset, int whence = SEEK_SET) {
    if (fd_ == -1)
      return -1;

    if (!direct_io_enabled_) {
      // Standard seek: just call lseek and invalidate the buffer.
      off_t result = ::lseek(fd_, offset, whence);
      if (result != (off_t)-1) {
        valid_bytes_in_buffer_ = 0;
        current_data_ptr_ = buffer_.get();
      } else {
        perror("lseek failed");
      }
      return result;
    }

    // O_DIRECT seek: must align the offset and pre-fill the buffer.
    // First, resolve whence to get an absolute offset.
    off_t absolute_offset = ::lseek(fd_, offset, whence);
    if (absolute_offset == (off_t)-1) {
      perror("lseek failed to resolve offset");
      return -1;
    }

    // Calculate the aligned position for the actual system call.
    off_t aligned_seek_pos = (absolute_offset / k_sector_size) * k_sector_size;
    off_t seek_ahead_in_buffer = absolute_offset - aligned_seek_pos;

    // Perform the aligned seek.
    if (::lseek(fd_, aligned_seek_pos, SEEK_SET) == (off_t)-1) {
      perror("lseek failed for aligned seek");
      return -1;
    }

    // Pre-fill the buffer from the new aligned position.
    ssize_t bytes_read = fill_buffer();
    if (bytes_read < 0) {
      // fill_buffer already printed an error.
      return -1;
    }

    // If the seek was beyond the end of the file, our buffer will be empty.
    if (bytes_read == 0) {
      return absolute_offset;
    }

    // Check if the amount to seek ahead is within our newly filled buffer.
    if ((size_t)seek_ahead_in_buffer >= valid_bytes_in_buffer_) {
      // The seek was to a position at or beyond the end of the file.
      // Our buffer is valid but empty for the user.
      valid_bytes_in_buffer_ = 0;
      current_data_ptr_ = buffer_.get();
    } else {
      // Advance the internal pointer to the user's desired offset.
      current_data_ptr_ = buffer_.get() + seek_ahead_in_buffer;
    }

    return absolute_offset;
  }
};

} // namespace humming::util::io
