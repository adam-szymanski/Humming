#include "db/bucket.h"

using namespace std;

namespace humming::DB {

//vector<IndexPage> pages;

void Bucket::insert(KVs &&kvs) {
  write("/home/adam/KV/"s + std::to_string(_files.size()) + ".data",
        std::move(kvs));
}

PageIterator::PageIterator(util::io::BufferedFileInput &in) : _in(in) {}

// returns false if loading did not succeed
bool PageIterator::init(size_t page_for_entry, const size_t index_offset,
                        const size_t entries_num) {
  _index_offset = index_offset;
  _entries_num = entries_num;
  _pages_num =
      (entries_num + IndexPage::k_entries_num - 1) / IndexPage::k_entries_num;
  _curr_entry_in_block = page_for_entry % IndexPage::k_entries_num;
  return setPageId(page_for_entry / IndexPage::k_entries_num);
}

bool PageIterator::setPageId(size_t page_id) {
  _page_id = page_id;
  if ((_page_id + 1) * IndexPage::k_entries_num > _entries_num) {
    _size = _entries_num - _page_id * IndexPage::k_entries_num;
  } else {
    _size = IndexPage::k_entries_num;
  }
  return load();
}

const IndexEntry &PageIterator::current() const {
  return _page->_entries[_curr_entry_in_block];
}
bool PageIterator::dec() {
  if (_curr_entry_in_block > 0) {
    --_curr_entry_in_block;
    return true;
  }
  if (_page_id == 0)
    return false;
  --_page_id;
  _size = IndexPage::k_entries_num;
  _curr_entry_in_block = _size - 1;
  return !load();
}
bool PageIterator::inc() {
  if (_curr_entry_in_block + 1 < _size) {
    ++_curr_entry_in_block;
    return true;
  }
  if (_page_id + 1 >= _pages_num)
    return false;
  ++_page_id;
  if ((_page_id + 1) * IndexPage::k_entries_num > _entries_num) {
    _size = _entries_num - _page_id * IndexPage::k_entries_num;
  }
  _curr_entry_in_block = 0;
  return !load();
}

bool PageIterator::load() {
  //  _page = &pages[_page_id];
  //  return false;
  return _in.pread((char *)_page, sizeof(IndexPage),
                   _index_offset + _page_id * sizeof(IndexPage)) !=
         sizeof(IndexPage);
}

void getHashOffsets(ReadContext &context, size_t size, size_t hash,
                    size_t offset) {
  // estimated initial position of key
  size_t curr_entry = (hash >> 32) * size / (size_t(1) << 32);
  PageIterator &block = context._index_iterator;
  block.init(curr_entry, offset, size);

  const auto curr_hash = block.current()._hash;
  context._result.clear();
  if (curr_hash == hash) { // let's search for a range
    context._result.emplace_back(block.current()._offset);
    while (block.dec() && block.current()._hash == hash) {
      context._result.emplace_back(block.current()._offset);
    }
    block.init(curr_entry, offset, size);
    while (block.inc() && block.current()._hash == hash) {
      context._result.emplace_back(block.current()._offset);
    }
  } else if (curr_hash < hash) { // let's search for hash on right side
    while (block._page_id + 1 < block._pages_num &&
           block._page->_entries[block._size - 1]._hash < hash) {
      size_t following_hashes_num =
          std::min(block._pages_num - block._page_id, IndexPage::k_hashes_num);
      size_t p = 0;
      for (; p < following_hashes_num && block._page->_post_hashes[p] < hash;
           ++p)
        ;

      if (p == block._page_id)
        return; // there is no page meeting the criteria
      ++p;
      block.setPageId(block._page_id + p);
      block._curr_entry_in_block = 0;
    }
    do {
      if (block.current()._hash == hash) {
        context._result.emplace_back(block.current()._offset);
      }
      if (block.current()._hash > hash) {
        return;
      }
    } while (block.inc());
  } else { // curr_hash > hash -> let's search for hash on left side
    while (block._page_id > 0 && block._page->_entries[0]._hash > hash) {
      size_t preceding_hashes_num =
          std::min(block._page_id, IndexPage::k_hashes_num);
      size_t p = 0;
      for (; p < preceding_hashes_num && block._page->_pre_hashes[p] > hash;
           ++p)
        ;

      if (p == block._page_id)
        return; // there is no page meeting the criteria
      ++p;
      block.setPageId(block._page_id - p);
    }
    const auto &entries = block._page->_entries;
    unsigned int bot, mid, top;
    bot = 0;
    top = block._size;
    //    PLOGD << 1;
    while (top > 1) {
      mid = top / 2;
      if (hash >= entries[bot + mid]._hash)
        bot += mid;
      top -= mid;
    }
    if (hash != entries[bot]._hash)
      return;
    while (bot + 1 < block._size && entries[bot + 1]._hash == hash)
      ++bot;
    while (entries[bot]._hash == hash) {
      context._result.emplace_back(entries[bot]._offset);
      if (bot > 0) {
        --bot;
        continue;
      }
      if (block._page_id == 0)
        return;
      --block._page_id;
      block._size = IndexPage::k_entries_num;
      bot = IndexPage::k_entries_num - 1;
      block.load();
    }
  }
}

KVs Bucket::read(const string &k, ReadContext &context) {
  size_t hash = hasher(k);
  vector<KV> result;
  KV kv;
  for (auto &file_meta : _files) {
    context._in.passFd(file_meta.fd(), false);
    size_t size = file_meta.entriesCount();
    size_t file_size = file_meta.byteSize();
    size_t index_size = (size + IndexPage::k_entries_num - 1) /
                        IndexPage::k_entries_num * sizeof(IndexPage);
    getHashOffsets(context, size, hash, file_size - index_size);
    for (const auto offset : context._result) {
      context._in.seek(offset);
      context._in.readString(kv._k);
      if (kv._k == k) {
        context._in.readString(kv._v);
        result.emplace_back(std::move(kv));
        break;
      }
    }
    context._in.close();
  }
  return result;
}

void Bucket::write(std::string path, KVs &&kvs) {
  std::sort(kvs.begin(), kvs.end(),
            [](const KV &l, const KV &r) { return l._hash < r._hash; });
  util::io::BufferedFileOutput out(1 << 12);
  if (out.open(path, false) == -1) {
    PLOGE << "could not open a file " << path
          << " because: " << strerror(errno);
    abort();
  }
  // write elements
  size_t size = kvs.size();
  vector<size_t> offsets;
  offsets.reserve(size);
  size_t offset = 0;
  for (const auto &kv : kvs) {
    out.writeString(kv._k);
    out.writeString(kv._v);
    offsets.push_back(offset);
    offset += sizeof(size_t) * 2 + kv._k.size() + kv._v.size();
  }
  // add padding so index will be sector size aligned
  if (offset % util::io::k_sector_size > 0) {
    char padding[util::io::k_sector_size];
    memset(padding, 0, util::io::k_sector_size);
    out.write(padding,
              util::io::k_sector_size - (offset % util::io::k_sector_size));
  }

  // write hashes and offsets for given hash
  IndexPage page;
  size_t page_entry = 0;
  const size_t entries_num = kvs.size();
  for (size_t i = 0; i < entries_num; ++i) {
    page._entries[page_entry] = {._hash = kvs[i]._hash, ._offset = offsets[i]};
    ++page_entry;
    if (page_entry == IndexPage::k_entries_num || i + 1 == entries_num) {
      {
        // Fill last hash for following pages
        size_t pages_ahead = (entries_num + IndexPage::k_entries_num - 1) /
                             IndexPage::k_entries_num;
        size_t hashes_ahead = std::min(IndexPage::k_hashes_num, pages_ahead);
        size_t pos = i + IndexPage::k_entries_num;
        for (size_t k = 0; k < hashes_ahead; ++k) {
          page._post_hashes[k] = kvs[pos]._hash;
          pos += IndexPage::k_entries_num;
        }
      }
      {
        // Fill first hash for preceding pages
        size_t pages_preceding =
            (i + 1 - IndexPage::k_entries_num) / IndexPage::k_entries_num;
        size_t hashes_preceding =
            std::min(IndexPage::k_hashes_num, pages_preceding);
        size_t pos = i - IndexPage::k_entries_num * 2 + 1;
        for (size_t k = 0; k < hashes_preceding; ++k) {
          page._pre_hashes[k] = kvs[pos]._hash;
          pos -= IndexPage::k_entries_num;
        }
      }
//      pages.push_back(page);
      out.write((const char *)&page, sizeof(IndexPage));
      page_entry = 0;
    }
  }
  if (out.close() == -1) {
    PLOGE << "could not close a file " << path
          << " because: " << strerror(errno);
    abort();
  }
  _files.push_back(std::move(
      DataFileMetadata{path, size, std::filesystem::file_size(path)}));
}

} // namespace humming::Bucket
