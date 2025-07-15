#pragma once

#include <algorithm>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "plog/Appenders/ColorConsoleAppender.h"
#include "plog/Appenders/ConsoleAppender.h"
#include "plog/Formatters/TxtFormatter.h"
#include "plog/Init.h"
#include "plog/Log.h"
#include "util/io/buffered_file_input.h"
#include "util/io/buffered_file_output.h"
#include "util/perf/timer.h"
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <unistd.h>

#include "db/KV.h"
#include "db/data_file_metadata.h"

using namespace std;

namespace humming::DB {

struct ReadContext;

class Bucket {
private:
public:
  std::vector<DataFileMetadata> _files;
  void insert(KVs &&kvs);
  KVs read(const string &k, ReadContext &context);

private:
  void write(std::string path, KVs &&kvs);
};

struct IndexEntry {
  size_t _hash;
  size_t _offset;
};

struct IndexPage {
  // number of hashes for preceding and following IndexPages.
  static constexpr size_t k_hashes_num = 8;
  // number of entries in each index page
  static constexpr size_t k_entries_num =
      (util::io::k_sector_size - 2 * k_hashes_num * sizeof(size_t)) /
      sizeof(IndexEntry);
  // first hash of each of previous k_hashes_num index pages
  size_t _pre_hashes[k_hashes_num];
  // last hash of each of following k_hashes_num index pages
  size_t _post_hashes[k_hashes_num];
  // entries for each key
  IndexEntry _entries[k_entries_num];
};

struct PageIterator {
  static constexpr size_t k_entry_size = sizeof(IndexEntry);
  char _page_mem[sizeof(IndexPage) + util::io::k_sector_size - 1];
  IndexPage *_page =
      reinterpret_cast<IndexPage *>((reinterpret_cast<uintptr_t>(_page_mem) +
                                     util::io::k_sector_size - 1) &
                                    ~(util::io::k_sector_size - 1));
  size_t _curr_entry_in_block; // id of entry in block pointer by iterator
  size_t _size;                // number of entries loaded
  util::io::BufferedFileInput &_in;
  size_t _index_offset;
  size_t _entries_num;
  size_t _page_id;
  size_t _pages_num;

  PageIterator(util::io::BufferedFileInput &in);

  // returns false if loading did not succeed
  bool init(size_t page_for_entry,
            const size_t index_offset, const size_t entries_num);
  bool setPageId(size_t page_id);
  const IndexEntry &current() const;
  bool dec();
  bool inc();

  bool load();
};

struct ReadContext {
  util::io::BufferedFileInput _in;
  PageIterator _index_iterator{_in};
  vector<size_t> _result;
};

} // namespace humming::Bucket
