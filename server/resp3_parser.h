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
// --- Stateless RESP3 Parser ---
class Resp3Parser {
public:
  std::optional<RedisValue> parse(const char *&current, const char *end) {
    return parseValue(current, end);
  }

private:
  std::optional<RedisValue> parseValue(const char *&current, const char *end) {
    if (current >= end)
      return std::nullopt;

    char type_char = *current;
    const char *original_pos = current;
    current++;

    std::optional<RedisValue> result;
    switch (type_char) {
    case '+':
      result = parseSimpleString(current, end);
      break;
    case '-':
      result = parseError(current, end);
      break;
    case ':':
      result = parseInteger(current, end);
      break;
    case '$':
      result = parseBlobString(current, end);
      break;
    case '*':
      result = parseAggregate<RedisValue::Vector, RedisValue::Type::LIST>(
          current, end);
      break;
    case '%':
      result =
          parseAggregate<RedisValue::Map, RedisValue::Type::MAP>(current, end);
      break;
    case '~':
      result =
          parseAggregate<RedisValue::Set, RedisValue::Type::SET>(current, end);
      break;
    case '#':
      result = parseBoolean(current, end);
      break;
    case ',':
      result = parseDouble(current, end);
      break;
    case '(':
      result = parseBigNumber(current, end);
      break;
    case '_':
      result = parseNil(current, end);
      break;
    case '!':
      result = parseBlobError(current, end);
      break;
    case '=':
      result = parseVerbatimString(current, end);
      break;
    case '|':
      result = parseAggregate<RedisValue::Map, RedisValue::Type::ATTRIBUTE>(
          current, end);
      break;
    case '>':
      result = parseAggregate<RedisValue::Vector, RedisValue::Type::PUSH>(
          current, end);
      break;
    default:
      current = original_pos;
      return std::nullopt;
    }

    if (!result) {
      current = original_pos;
    }
    return result;
  }

  const char *findCRLF(const char *start, const char *end) {
    const char crlf[] = "\r\n";
    return std::search(start, end, crlf, crlf + 2);
  }

  std::optional<std::string> parseLine(const char *&current, const char *end) {
    const char *crlf_pos = findCRLF(current, end);
    if (crlf_pos == end)
      return std::nullopt;
    std::string line(current, crlf_pos);
    current = crlf_pos + 2;
    return line;
  }

  std::optional<RedisValue> parseSimpleString(const char *&current,
                                              const char *end) {
    auto line = parseLine(current, end);
    return line ? std::make_optional(RedisValue::makeSimpleString(*line))
                : std::nullopt;
  }

  std::optional<RedisValue> parseError(const char *&current, const char *end) {
    auto line = parseLine(current, end);
    return line ? std::make_optional(RedisValue::makeError(*line))
                : std::nullopt;
  }

  std::optional<RedisValue> parseInteger(const char *&current,
                                         const char *end) {
    auto line = parseLine(current, end);
    if (!line)
      return std::nullopt;
    try {
      return std::make_optional(
          RedisValue(RedisValue::Integer(std::stoll(*line))));
    } catch (...) {
      return std::nullopt;
    }
  }

  std::optional<RedisValue> parseBlobString(const char *&current,
                                            const char *end) {
    auto len_line = parseLine(current, end);
    if (!len_line)
      return std::nullopt;
    try {
      long long len = std::stoll(*len_line);
      if (len == -1)
        return std::make_optional(RedisValue());
      if (len < 0)
        return std::nullopt;
      if (end - current < len + 2)
        return std::nullopt;
      const char *data_end = current + len;
      if (*data_end != '\r' || *(data_end + 1) != '\n')
        return std::nullopt;
      std::string data(current, data_end);
      current = data_end + 2;
      return std::make_optional(RedisValue(data));
    } catch (...) {
      return std::nullopt;
    }
  }

  std::optional<RedisValue> parseBlobError(const char *&current,
                                           const char *end) {
    auto blob = parseBlobString(current, end);
    return blob
               ? std::make_optional(RedisValue::makeBlobError(blob->asString()))
               : std::nullopt;
  }

  std::optional<RedisValue> parseVerbatimString(const char *&current,
                                                const char *end) {
    auto blob = parseBlobString(current, end);
    return blob ? std::make_optional(
                      RedisValue::makeVerbatimString(blob->asString()))
                : std::nullopt;
  }

  template <typename T, RedisValue::Type Type>
  std::optional<RedisValue> parseAggregate(const char *&current,
                                           const char *end) {
    auto len_line = parseLine(current, end);
    if (!len_line)
      return std::nullopt;
    try {
      long long len = std::stoll(*len_line);
      if (len < 0)
        return std::nullopt;

      T container;
      if constexpr (std::is_same_v<T, RedisValue::Map>) {
        for (long long i = 0; i < len; ++i) {
          auto key = parseValue(current, end);
          if (!key)
            return std::nullopt;
          auto val = parseValue(current, end);
          if (!val)
            return std::nullopt;
          container.insert(std::make_pair(std::move(*key), std::move(*val)));
        }
      } else { // List, Set, Push
        for (long long i = 0; i < len; ++i) {
          auto elem = parseValue(current, end);
          if (!elem)
            return std::nullopt;
          if constexpr (Type == RedisValue::Type::SET) {
            container.insert(std::move(*elem));
          } else {
            container.push_back(std::move(*elem));
          }
        }
      }

      if constexpr (Type == RedisValue::Type::LIST)
        return RedisValue::makeList(std::move(container));
      if constexpr (Type == RedisValue::Type::SET)
        return RedisValue(std::move(container));
      if constexpr (Type == RedisValue::Type::PUSH)
        return RedisValue::makePush(std::move(container));
      if constexpr (Type == RedisValue::Type::MAP)
        return RedisValue(std::move(container));
      return std::nullopt; // Should not happen
    } catch (...) {
      return std::nullopt;
    }
  }

  std::optional<RedisValue> parseNil(const char *&current, const char *end) {
    auto line = parseLine(current, end);
    return line && line->empty() ? std::make_optional(RedisValue())
                                 : std::nullopt;
  }

  std::optional<RedisValue> parseBoolean(const char *&current,
                                         const char *end) {
    auto line = parseLine(current, end);
    if (!line || line->length() != 1)
      return std::nullopt;
    if ((*line)[0] == 't')
      return std::make_optional(RedisValue(true));
    if ((*line)[0] == 'f')
      return std::make_optional(RedisValue(false));
    return std::nullopt;
  }

  std::optional<RedisValue> parseDouble(const char *&current, const char *end) {
    auto line = parseLine(current, end);
    if (!line)
      return std::nullopt;
    try {
      return std::make_optional(RedisValue(std::stod(*line)));
    } catch (...) {
      return std::nullopt;
    }
  }

  std::optional<RedisValue> parseBigNumber(const char *&current,
                                           const char *end) {
    auto line = parseLine(current, end);
    return line ? std::make_optional(RedisValue::makeBigNumber(*line))
                : std::nullopt;
  }
};
