#pragma once

#include "plog/Log.h"
#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
using std::chrono::milliseconds;
using std::chrono::minutes;
using std::chrono::nanoseconds;
using std::chrono::seconds;

namespace humming::util::perf {

std::string printElapsed(const nanoseconds &elapsed_ns);

class Timer {
private:
  std::chrono::time_point<std::chrono::high_resolution_clock> _start =
      high_resolution_clock::now();
  std::string _message;
  size_t _event_count = 1;

public:
  Timer(std::string message) : _message(std::move(message)) {}
  ~Timer() { printMessage(); }
  void newMeasure(std::string message) {
    printMessage();
    _message = std::move(message);
    _event_count = 1;
  }
  void addCount(size_t count = 1) { _event_count += count; }

private:
  void printMessage() {
    auto end = high_resolution_clock::now();
    auto elapsed = duration_cast<nanoseconds>(end - _start);
    if (_event_count == 1) {
      PLOGI << _message << printElapsed(elapsed);
    } else {
      PLOGI << _message << printElapsed(elapsed) << " events: " << _event_count
            << " time per event: " << printElapsed(elapsed / _event_count);
    }
    _start = end;
  }
};

} // namespace humming::util::perf
