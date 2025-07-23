#pragma once

#include <algorithm> // For std::search
#include <atomic>
#include <boost/asio.hpp>
#include <cctype>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

using boost::asio::ip::tcp;

// Forward declaration is needed for recursive types like List, Set, etc.
class RedisValue;

struct RedisValueHash {
  std::size_t operator()(const RedisValue &val) const;
};

struct RedisValueEqual {
  bool operator()(const RedisValue &lhs, const RedisValue &rhs) const;
};

/**
 * @class RedisValue
 * @brief A C++ class to represent any of the RESP3 data types using a tagged
 * union approach.
 */
class RedisValue {
public:
  enum class Type {
    NIL,
    ERROR,
    INTEGER,
    SIMPLE_STRING,
    BLOB_STRING,
    BOOLEAN,
    DOUBLE,
    BIG_NUMBER,
    BLOB_ERROR,
    VERBATIM_STRING,
    LIST,
    MAP,
    SET,
    ATTRIBUTE,
    PUSH
  };

  using Nil = std::monostate;
  using Integer = int64_t;
  using Boolean = bool;
  using Double = double;

  // Use unique_ptr for indirection to break circular dependency and allow
  // unordered containers.
  using Vector = std::vector<RedisValue>;
  using Set = std::unordered_set<RedisValue, RedisValueHash, RedisValueEqual>;
  using Map = std::unordered_map<RedisValue, RedisValue, RedisValueHash,
                                 RedisValueEqual>;
  using Attribute = std::map<std::string, RedisValue>;

private:
  // All string-like types are stored as std::string in the variant.
  // The _type member distinguishes them.
  using RedisVariant = std::variant<Nil, Integer, Boolean, Double, std::string,
                                    Vector, Map, Set, Attribute>;

  Type _type;
  RedisVariant _value;

  friend struct RedisValueHash;
  friend struct RedisValueEqual;
  friend struct Resp3Parser;

  static void toStringHelper(const RedisValue &val, std::ostringstream &oss,
                             int indent_level);
  static void serializeHelper(const RedisValue &val, std::ostringstream &oss);

  // Private constructor for factory methods
  RedisValue(Type type, std::string &&val)
      : _type(type), _value(std::move(val)) {}
  RedisValue(Type type, Vector &&val) : _type(type), _value(std::move(val)) {}

public:
  // --- Constructors for Unambiguous Types ---
  RedisValue() : _type(Type::NIL), _value(Nil{}) {}
  RedisValue(Integer val) : _type(Type::INTEGER), _value(val) {}
  RedisValue(Boolean val) : _type(Type::BOOLEAN), _value(val) {}
  RedisValue(Double val) : _type(Type::DOUBLE), _value(val) {}
  RedisValue(Set &&val) : _type(Type::SET), _value(std::move(val)) {}
  RedisValue(Map &&val) : _type(Type::MAP), _value(std::move(val)) {}
  RedisValue(Attribute &&val)
      : _type(Type::ATTRIBUTE), _value(std::move(val)) {}

  // Default string constructors create a BlobString
  RedisValue(const std::string &val) : _type(Type::BLOB_STRING), _value(val) {}
  RedisValue(std::string &&val)
      : _type(Type::BLOB_STRING), _value(std::move(val)) {}
  RedisValue(const char *val)
      : _type(Type::BLOB_STRING), _value(std::string(val)) {}

  // --- Static Factory Functions for String-based Types ---
  static RedisValue makeError(std::string val) {
    return RedisValue(Type::ERROR, std::move(val));
  }
  static RedisValue makeSimpleString(std::string val) {
    return RedisValue(Type::SIMPLE_STRING, std::move(val));
  }
  static RedisValue makeBigNumber(std::string val) {
    return RedisValue(Type::BIG_NUMBER, std::move(val));
  }
  static RedisValue makeBlobError(std::string val) {
    return RedisValue(Type::BLOB_ERROR, std::move(val));
  }
  static RedisValue makeVerbatimString(std::string val) {
    return RedisValue(Type::VERBATIM_STRING, std::move(val));
  }
  static RedisValue makeList(Vector val = {}) {
    return RedisValue(Type::LIST, std::move(val));
  }
  static RedisValue makePush(Vector val = {}) {
    return RedisValue(Type::PUSH, std::move(val));
  }

  // Copy and move constructors for deep copies
  RedisValue(const RedisValue &other);
  RedisValue &operator=(const RedisValue &other);
  RedisValue(RedisValue &&other) = default;
  RedisValue &operator=(RedisValue &&other) = default;

  bool operator==(const RedisValue &other) const;

  Type type() const { return _type; }

  // --- Accessors ---
  Integer &asInteger() { return std::get<Integer>(_value); }
  const Integer &asInteger() const { return std::get<Integer>(_value); }
  Boolean &asBoolean() { return std::get<Boolean>(_value); }
  const Boolean &asBoolean() const { return std::get<Boolean>(_value); }
  Double &asDouble() { return std::get<Double>(_value); }
  const Double &asDouble() const { return std::get<Double>(_value); }
  Vector &asVector() { return std::get<Vector>(_value); }
  const Vector &asVector() const { return std::get<Vector>(_value); }
  Set &asSet() { return std::get<Set>(_value); }
  const Set &asSet() const { return std::get<Set>(_value); }
  Map &asMap() { return std::get<Map>(_value); }
  const Map &asMap() const { return std::get<Map>(_value); }
  Attribute &asAttribute() { return std::get<Attribute>(_value); }
  const Attribute &asAttribute() const { return std::get<Attribute>(_value); }

  // Accessors for string-based types
  std::string &asString() { return std::get<std::string>(_value); }
  const std::string &asString() const { return std::get<std::string>(_value); }

  std::string toString() const {
    std::ostringstream oss;
    toStringHelper(*this, oss, 0);
    return oss.str();
  }

  std::string serialize() const {
    std::ostringstream oss;
    serializeHelper(*this, oss);
    return oss.str();
  }
};

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
        } else if constexpr (std::is_same_v<T, RedisValue::Attribute>) {
          std::size_t seed = arg.size();
          for (const auto &[key, val] : arg) {
            seed ^= std::hash<std::string>{}(key) + 0x9e3779b9 + (seed << 6) +
                    (seed >> 2);
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
  case Type::BLOB_STRING:
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
  case Type::BLOB_ERROR:
    oss << "(blob-error) \"" << val.asString() << "\"";
    break;
  case Type::VERBATIM_STRING:
    oss << "(verbatim-string) \"" << val.asString() << "\"";
    break;
  case Type::LIST:
  case Type::PUSH:
  case Type::SET: {
    if (val.type() == Type::LIST)
      oss << "(list)\n";
    else if (val.type() == Type::PUSH)
      oss << "(push)\n";
    else
      oss << "(set)\n";
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
  case Type::BLOB_STRING:
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
  case Type::BLOB_ERROR:
    oss << "!" << val.asString().length() << "\r\n" << val.asString() << "\r\n";
    break;
  case Type::VERBATIM_STRING:
    oss << "=" << val.asString().length() << "\r\n" << val.asString() << "\r\n";
    break;
  case Type::LIST: {
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
    const auto &attr = val.asAttribute();
    oss << "|" << attr.size() << "\r\n";
    for (const auto &[key, value] : attr) {
      serializeHelper(RedisValue::makeSimpleString(key), oss);
      serializeHelper(value, oss);
    }
    break;
  }
  }
}
