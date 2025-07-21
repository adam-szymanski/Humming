#include <algorithm> // For std::search
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

#include "server/redis_value.h"
#include "server/resp3_parser.h"

using boost::asio::ip::tcp;

class session : public std::enable_shared_from_this<session> {
public:
  session(tcp::socket socket)
      : _socket(std::move(socket)), _strand(_socket.get_executor()) {}

  void start() { doRead(); }

private:
  void doRead() {
    auto self(shared_from_this());
    _socket.async_read_some(
        boost::asio::buffer(_read_buffer_raw),
        [this, self](boost::system::error_code ec, std::size_t length) {
          if (!ec) {
            boost::asio::post(_strand, [this, self, length]() {
              _read_buffer.append(_read_buffer_raw.data(), length);

              const char *start = _read_buffer.data();
              const char *end = start + _read_buffer.size();
              const char *current = start;

              while (true) {
                auto value = _parser.parse(current, end);
                if (value) {
                  process(std::move(*value));
                } else {
                  break;
                }
              }

              size_t consumed = current - start;
              if (consumed > 0) {
                _read_buffer.erase(0, consumed);
              }
            });
            doRead();
          } else if (ec != boost::asio::error::eof) {
            std::cerr << "Read error: " << ec.message() << std::endl;
          } else {
            std::cout << "Client disconnected." << std::endl;
          }
        });
  }

  void process(RedisValue &&value) {
    std::cout << "Received value from " << _socket.remote_endpoint() << ":\n"
              << value.toString() << "\n"
              << std::endl;

    doWrite("+OK\r\n");
  }

  void doWrite(const std::string &msg) {
    bool write_in_progress = !_write_msgs.empty();
    _write_msgs.push_back(msg);

    if (!write_in_progress) {
      startWriteLoop();
    }
  }

  void startWriteLoop() {
    auto self(shared_from_this());
    boost::asio::async_write(
        _socket, boost::asio::buffer(_write_msgs.front()),
        boost::asio::bind_executor(
            _strand,
            [this, self](boost::system::error_code ec, std::size_t /*length*/) {
              if (!ec) {
                _write_msgs.pop_front();
                if (!_write_msgs.empty()) {
                  startWriteLoop();
                }
              } else {
                std::cerr << "Write error: " << ec.message() << std::endl;
              }
            }));
  }

  tcp::socket _socket;
  boost::asio::strand<boost::asio::io_context::executor_type> _strand;
  std::string _read_buffer;
  std::array<char, 4096> _read_buffer_raw;
  Resp3Parser _parser;
  std::deque<std::string> _write_msgs;
};

// Represents the server that listens for and accepts incoming connections.
class server {
public:
  server(boost::asio::io_context &io_context, short port)
      : _acceptor(io_context, tcp::endpoint(tcp::v4(), port)) {
    doAccept();
  }

private:
  void doAccept() {
    _acceptor.async_accept([this](boost::asio::error_code ec,
                                  tcp::socket socket) {
      if (!ec) {
        std::cout << "Accepted connection from: " << socket.remote_endpoint()
                  << std::endl;
        std::make_shared<session>(std::move(socket))->start();
      }
      doAccept();
    });
  }
  tcp::acceptor _acceptor;
};
