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
    BULK_STRING,
    BOOLEAN,
    DOUBLE,
    BIG_NUMBER,
    BULK_ERROR,
    VERBATIM_STRING,
    ARRAY,
    MAP,
    SET,
    ATTRIBUTE,
    NULL_ARRAY,
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

private:
  // All string-like types are stored as std::string in the variant.
  // The _type member distinguishes them.
  using RedisVariant = std::variant<Nil, Integer, Boolean, Double, std::string,
                                    Vector, Map, Set>;

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
  RedisValue(Type type) : _type(type), _value(std::move(Nil{})) {}
  RedisValue(Type type, RedisVariant &&val)
      : _type(type), _value(std::move(val)) {}

public:
  // --- Constructors for Unambiguous Types ---
  RedisValue() : _type(Type::NIL), _value(Nil{}) {}
  RedisValue(Integer val) : _type(Type::INTEGER), _value(val) {}
  RedisValue(Boolean val) : _type(Type::BOOLEAN), _value(val) {}
  RedisValue(Double val) : _type(Type::DOUBLE), _value(val) {}
  RedisValue(Set &&val) : _type(Type::SET), _value(std::move(val)) {}
  RedisValue(Map &&val) : _type(Type::MAP), _value(std::move(val)) {}

  // Default string constructors create a BlobString
  RedisValue(const std::string &val) : _type(Type::BULK_STRING), _value(val) {}
  RedisValue(std::string &&val)
      : _type(Type::BULK_STRING), _value(std::move(val)) {}
  RedisValue(const char *val)
      : _type(Type::BULK_STRING), _value(std::string(val)) {}

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
    return RedisValue(Type::BULK_ERROR, std::move(val));
  }
  static RedisValue makeVerbatimString(std::string val) {
    return RedisValue(Type::VERBATIM_STRING, std::move(val));
  }
  static RedisValue makeList(Vector val = {}) {
    return RedisValue(Type::ARRAY, std::move(val));
  }
  static RedisValue makePush(Vector val = {}) {
    return RedisValue(Type::PUSH, std::move(val));
  }
  static RedisValue makeNullArray() { return RedisValue(Type::NULL_ARRAY); }
  static RedisValue makeAttribute(Map val) {
    return RedisValue(Type::ATTRIBUTE, std::move(val));
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
