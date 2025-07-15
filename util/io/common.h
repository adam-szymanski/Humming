#pragma once

#include <cstdlib> // For aligned_alloc, free

namespace humming::util::io {

// The alignment for the buffer, typically a disk sector size.
static constexpr size_t k_sector_size = 4096;

/**
 * @brief Custom deleter for memory allocated with aligned_alloc.
 */
struct AlignedBufferDeleter {
  void operator()(char *p) const { free(p); }
};

/**
 * @brief Calculates a size rounded up to the nearest multiple of k_sector_size.
 */
static size_t calculate_aligned_size(size_t requested_size) {
  if (requested_size == 0) {
    return k_sector_size;
  }
  // Round up to the nearest multiple of k_sector_size
  return ((requested_size + k_sector_size - 1) / k_sector_size) * k_sector_size;
}

/**
 * @brief Allocates a buffer of a given size with k_sector_size alignment.
 */
static char *allocate_aligned_buffer(size_t size) {
  // aligned_alloc requires the size to be a multiple of the alignment.
  void *ptr = aligned_alloc(k_sector_size, size);
  if (!ptr) {
    throw std::bad_alloc();
  }
  return static_cast<char *>(ptr);
}

} // namespace humming::util::io
