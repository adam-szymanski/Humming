#include <optional>
#include <string>

#include "plog/Appenders/ColorConsoleAppender.h"
#include "plog/Appenders/ConsoleAppender.h"
#include "plog/Formatters/TxtFormatter.h"
#include "plog/Init.h"
#include "plog/Log.h"
#include "util/io/buffered_file_input.h"
#include "util/io/buffered_file_output.h"
#include "util/perf/timer.h"
#include <unistd.h>

#include "db/bucket.h"

#include "server/redis_value.h"
#include "server/resp3_parser.h"

using namespace std;
using namespace humming;

int main() {
  static plog::ColorConsoleAppender<plog::TxtFormatter> debug_console_appender;
  plog::init(plog::debug, &debug_console_appender);
  humming::DB::Bucket bucket;
  {
    humming::DB::KVs kvs;
    kvs.emplace_back("a", "ą");
    kvs.emplace_back("c", "ć");
    kvs.emplace_back("l", "ł");
    kvs.emplace_back("e", "ę");

    for (int i = 0; i < 1000000; ++i)
      kvs.emplace_back(std::to_string(i), std::to_string(-i));
    //  for (size_t i = 0; i < 5; ++i)
    util::perf::Timer _("store data: ");
    bucket.insert(std::move(kvs));
  }
  //  if (bucket._file_paths.empty())
  //    bucket._file_paths.push_back("/home/adam/KV/0.data");
  util::io::BufferedFileInput in{util::io::k_sector_size};
  humming::DB::ReadContext context;
  PLOGD << bucket.read("a", context);
  PLOGD << bucket.read("100", context);
  PLOGD << bucket.read("1000", context);
  PLOGD << bucket.read("631545", context);
  PLOGD << bucket.read("1231545", context);
  PLOGD << bucket.read("57", context);
  PLOGD << bucket.read("27876", context);
  PLOGD << bucket.read("41", context);

  util::perf::Timer _("read data: ");
  for (int i = 0; i < 2000000; ++i) {
    auto response = bucket.read(std::to_string(i), context);
    size_t response_size = response.size();
    if ((i < 1000000) != response_size) {
      PLOGE << "wrong result for " << i << " got: " << response;
      exit(0);
    }
  }

  RedisValue val(int64_t(123));
  Resp3Parser parser;
  //  try {
  //    short port = 6379;
  //    boost::asio::io_context io_context;
  //    server s(io_context, port);
  //
  //    std::vector<std::thread> threads;
  //    const auto thread_count = std::thread::hardware_concurrency();
  //    std::cout << "Server starting with " << thread_count << " threads on
  //    port "
  //              << port << "..." << std::endl;
  //
  //    for (unsigned int i = 0; i < thread_count; ++i) {
  //      threads.emplace_back([&io_context]() { io_context.run(); });
  //    }
  //    for (auto &t : threads) {
  //      t.join();
  //    }
  //  } catch (const std::exception &e) {
  //    std::cerr << "Exception: " << e.what() << "\n";
  //  }
  return 0;
}
