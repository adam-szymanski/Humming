#include "server/resp3_parser.h"

#include <plog/Log.h>

using boost::asio::ip::tcp;

std::optional<RedisValue> Resp3Parser::parse(const char *&current,
                                             const char *end) {
  return parseValue(current, end);
}

std::optional<RedisValue> Resp3Parser::parseValue(const char *&current,
                                                  const char *end) {
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
    result = parseBulkString(current, end);
    break;
  case '*':
    result = parseAggregate<RedisValue::Vector, RedisValue::Type::ARRAY>(
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
    result = parseBulkError(current, end);
    break;
  case '=':
    result = parseVerbatimString(current, end);
    break;
  case '|':
    result = parseAggregate<RedisValue::Map, RedisValue::Type::ATTRIBUTE>(
        current, end);
    break;
  case '>':
    result = parseAggregate<RedisValue::Vector, RedisValue::Type::PUSH>(current,
                                                                        end);
    break;
  default:
    PLOGE << "unknown";
    current = original_pos;
    return std::nullopt;
  }

  if (!result) {
    current = original_pos;
  }
  return result;
}

const char *Resp3Parser::findCRLF(const char *start, const char *end) {
  const char crlf[] = "\r\n";
  return std::search(start, end, crlf, crlf + 2);
}

std::optional<std::string> Resp3Parser::parseLine(const char *&current,
                                                  const char *end) {
  const char *crlf_pos = findCRLF(current, end);
  if (crlf_pos == end)
    return std::nullopt;
  std::string line(current, crlf_pos);
  current = crlf_pos + 2;
  return line;
}

std::optional<RedisValue> Resp3Parser::parseSimpleString(const char *&current,
                                                         const char *end) {
  auto line = parseLine(current, end);
  return line ? std::make_optional(RedisValue::makeSimpleString(*line))
              : std::nullopt;
}

std::optional<RedisValue> Resp3Parser::parseError(const char *&current,
                                                  const char *end) {
  auto line = parseLine(current, end);
  return line ? std::make_optional(RedisValue::makeError(*line)) : std::nullopt;
}

std::optional<RedisValue> Resp3Parser::parseInteger(const char *&current,
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

std::optional<RedisValue> Resp3Parser::parseBulkString(const char *&current,
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

std::optional<RedisValue> Resp3Parser::parseBulkError(const char *&current,
                                                      const char *end) {
  auto bulk = parseBulkString(current, end);
  return bulk ? std::make_optional(RedisValue::makeBlobError(bulk->asString()))
              : std::nullopt;
}

std::optional<RedisValue> Resp3Parser::parseVerbatimString(const char *&current,
                                                           const char *end) {
  auto bulk = parseBulkString(current, end);
  return bulk ? std::make_optional(
                    RedisValue::makeVerbatimString(bulk->asString()))
              : std::nullopt;
}

template <typename T, RedisValue::Type Type>
std::optional<RedisValue> Resp3Parser::parseAggregate(const char *&current,
                                                      const char *end) {
  auto len_line = parseLine(current, end);
  if (!len_line)
    return std::nullopt;
  try {
    long long len = std::stoll(*len_line);
    if (len < 0) {
      if constexpr (Type == RedisValue::Type::ARRAY) {
        if (len == -1) {
          return RedisValue::makeNullArray();
        }
      }
      return std::nullopt;
    }

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

    if constexpr (Type == RedisValue::Type::ARRAY)
      return RedisValue::makeList(std::move(container));
    if constexpr (Type == RedisValue::Type::SET)
      return RedisValue(std::move(container));
    if constexpr (Type == RedisValue::Type::PUSH)
      return RedisValue::makePush(std::move(container));
    if constexpr (Type == RedisValue::Type::MAP)
      return RedisValue(std::move(container));
    if constexpr (Type == RedisValue::Type::ATTRIBUTE)
      return RedisValue::makeAttribute(std::move(container));
    return std::nullopt; // Should not happen
  } catch (...) {
    return std::nullopt;
  }
}

std::optional<RedisValue> Resp3Parser::parseNil(const char *&current,
                                                const char *end) {
  auto line = parseLine(current, end);
  return line && line->empty() ? std::make_optional(RedisValue())
                               : std::nullopt;
}

std::optional<RedisValue> Resp3Parser::parseBoolean(const char *&current,
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

std::optional<RedisValue> Resp3Parser::parseDouble(const char *&current,
                                                   const char *end) {
  auto line = parseLine(current, end);
  if (!line)
    return std::nullopt;
  try {
    return std::make_optional(RedisValue(std::stod(*line)));
  } catch (...) {
    return std::nullopt;
  }
}

std::optional<RedisValue> Resp3Parser::parseBigNumber(const char *&current,
                                                      const char *end) {
  auto line = parseLine(current, end);
  return line ? std::make_optional(RedisValue::makeBigNumber(*line))
              : std::nullopt;
}

bool RedisValue::operator==(const RedisValue &other) const {
  if (_type != other._type)
    return false;
  return std::visit(
      [&other](auto &&arg) -> bool {
        using T = std::decay_t<decltype(arg)>;
        auto &other_arg = std::get<T>(other._value);
        return arg == other_arg;
      },
      _value);
}

std::size_t RedisValueHash::operator()(const RedisValue &val) const {
  size_t type_hash = std::hash<int>{}(static_cast<int>(val.type()));
  size_t value_hash = std::visit(
      [](auto &&arg) -> std::size_t {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, RedisValue::Nil>) {
          return 0;
        } else if constexpr (std::is_same_v<T, RedisValue::Integer> ||
                             std::is_same_v<T, std::string> ||
                             std::is_same_v<T, RedisValue::Boolean> ||
                             std::is_same_v<T, RedisValue::Double>) {
          return std::hash<T>{}(arg);
        } else if constexpr (std::is_same_v<T, RedisValue::Vector> ||
                             std::is_same_v<T, RedisValue::Set>) {
          std::size_t seed = arg.size();
          for (const auto &elem : arg) {
            seed ^=
                RedisValueHash{}(elem) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          }
          return seed;
        } else if constexpr (std::is_same_v<T, RedisValue::Map>) {
          std::size_t seed = arg.size();
          for (const auto &[key, val] : arg) {
            seed ^=
                RedisValueHash{}(key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^=
                RedisValueHash{}(val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          }
          return seed;
        }
        return 0;
      },
      val._value);
  return type_hash ^ (value_hash << 1);
}

bool RedisValueEqual::operator()(const RedisValue &lhs,
                                 const RedisValue &rhs) const {
  return lhs == rhs;
}

void RedisValue::toStringHelper(const RedisValue &val, std::ostringstream &oss,
                                int indent_level) {
  std::string indent(indent_level * 2, ' ');
  oss << indent;
  switch (val.type()) {
  case Type::NIL:
    oss << "(nil)";
    break;
  case Type::ERROR:
    oss << "(error) " << val.asString();
    break;
  case Type::INTEGER:
    oss << "(integer) " << val.asInteger();
    break;
  case Type::SIMPLE_STRING:
    oss << val.asString();
    break;
  case Type::BULK_STRING:
    oss << "\"" << val.asString() << "\"";
    break;
  case Type::BOOLEAN:
    oss << (val.asBoolean() ? "(boolean) true" : "(boolean) false");
    break;
  case Type::DOUBLE:
    oss << "(double) " << val.asDouble();
    break;
  case Type::BIG_NUMBER:
    oss << "(bignumber) " << val.asString();
    break;
  case Type::BULK_ERROR:
    oss << "(bulk-error) \"" << val.asString() << "\"";
    break;
  case Type::VERBATIM_STRING:
    oss << "(verbatim-string) \"" << val.asString() << "\"";
    break;
  case Type::ARRAY:
  case Type::PUSH: {
    if (val.type() == Type::ARRAY)
      oss << "(array)\n";
    else if (val.type() == Type::PUSH)
      oss << "(push)\n";
    int i = 1;
    const auto &container = val.asVector(); // Works for List, Push, Set
    for (const auto &item : container) {
      oss << indent << i++ << ") ";
      toStringHelper(item, oss, indent_level + 1);
      if (i <= (int)container.size())
        oss << "\n";
    }
    break;
  }
  case Type::SET: {
    oss << "(set)\n";
    int i = 1;
    const auto &container = val.asSet(); // Works for List, Push, Set
    for (const auto &item : container) {
      oss << indent << i++ << ") ";
      toStringHelper(item, oss, indent_level + 1);
      if (i <= (int)container.size())
        oss << "\n";
    }
    break;
  }
  case Type::MAP:
  case Type::ATTRIBUTE: {
    if (val.type() == Type::MAP)
      oss << "(map)\n";
    else
      oss << "(attribute)\n";
    int i = 1;
    const auto &container = val.asMap();
    for (const auto &[key, value] : container) {
      oss << indent << i++ << ") ";
      toStringHelper(key, oss, indent_level + 1);
      oss << "\n";
      oss << indent << i++ << ") ";
      toStringHelper(value, oss, indent_level + 1);
      if (i <= (int)container.size() * 2)
        oss << "\n";
    }
    break;
  }
  case Type::NULL_ARRAY:
    oss << "(null array)";
    break;
  }
}

void RedisValue::serializeHelper(const RedisValue &val,
                                 std::ostringstream &oss) {
  switch (val.type()) {
  case Type::NIL:
    oss << "_\r\n";
    break;
  case Type::ERROR:
    oss << "-" << val.asString() << "\r\n";
    break;
  case Type::INTEGER:
    oss << ":" << val.asInteger() << "\r\n";
    break;
  case Type::SIMPLE_STRING:
    oss << "+" << val.asString() << "\r\n";
    break;
  case Type::BULK_STRING:
    oss << "$" << val.asString().length() << "\r\n" << val.asString() << "\r\n";
    break;
  case Type::BOOLEAN:
    oss << "#" << (val.asBoolean() ? 't' : 'f') << "\r\n";
    break;
  case Type::DOUBLE:
    oss << "," << val.asDouble() << "\r\n";
    break;
  case Type::BIG_NUMBER:
    oss << "(" << val.asString() << "\r\n";
    break;
  case Type::BULK_ERROR:
    oss << "!" << val.asString().length() << "\r\n" << val.asString() << "\r\n";
    break;
  case Type::VERBATIM_STRING:
    oss << "=" << val.asString().length() << "\r\n" << val.asString() << "\r\n";
    break;
  case Type::ARRAY: {
    const auto &list = val.asVector();
    oss << "*" << list.size() << "\r\n";
    for (const auto &item : list) {
      serializeHelper(item, oss);
    }
    break;
  }
  case Type::SET: {
    const auto &set = val.asSet();
    oss << "~" << set.size() << "\r\n";
    for (const auto &item : set) {
      serializeHelper(item, oss);
    }
    break;
  }
  case Type::PUSH: {
    const auto &push = val.asVector();
    oss << ">" << push.size() << "\r\n";
    for (const auto &item : push) {
      serializeHelper(item, oss);
    }
    break;
  }
  case Type::MAP: {
    const auto &map = val.asMap();
    oss << "%" << map.size() << "\r\n";
    for (const auto &[key, value] : map) {
      serializeHelper(key, oss);
      serializeHelper(value, oss);
    }
    break;
  }
  case Type::ATTRIBUTE: {
    const auto &attr = val.asMap();
    oss << "|" << attr.size() << "\r\n";
    for (const auto &[key, value] : attr) {
      serializeHelper(key, oss);
      serializeHelper(value, oss);
    }
    break;
  }
  case Type::NULL_ARRAY: {
    oss << "*-1\r\n";
    break;
  }
  }
}

// --- RedisValue Implementation ---
RedisValue::RedisValue(const RedisValue &other)
    : _type(other._type), _value(other._value) {}

RedisValue &RedisValue::operator=(const RedisValue &other) {
  if (this != &other) {
    _type = other._type;
    _value = RedisValue(other)._value;
  }
  return *this;
}
