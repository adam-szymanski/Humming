#pragma once

#include <algorithm> // For std::search
#include <boost/asio.hpp>
#include <cctype>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>

#include "server/redis_value.h"

using boost::asio::ip::tcp;

class Resp3Parser {
public:
  std::optional<RedisValue> parse(const char *&current, const char *end);

private:
  std::optional<RedisValue> parseValue(const char *&current, const char *end);

  const char *findCRLF(const char *start, const char *end);

  std::optional<std::string> parseLine(const char *&current, const char *end);

  std::optional<RedisValue> parseSimpleString(const char *&current,
                                              const char *end);

  std::optional<RedisValue> parseError(const char *&current, const char *end);

  std::optional<RedisValue> parseInteger(const char *&current, const char *end);

  std::optional<RedisValue> parseBulkString(const char *&current,
                                            const char *end);

  std::optional<RedisValue> parseBulkError(const char *&current,
                                           const char *end);

  std::optional<RedisValue> parseVerbatimString(const char *&current,
                                                const char *end);

  template <typename T, RedisValue::Type Type>
  std::optional<RedisValue> parseAggregate(const char *&current,
                                           const char *end);

  std::optional<RedisValue> parseNil(const char *&current, const char *end);

  std::optional<RedisValue> parseBoolean(const char *&current, const char *end);

  std::optional<RedisValue> parseDouble(const char *&current, const char *end);

  std::optional<RedisValue> parseBigNumber(const char *&current,
                                           const char *end);
};
