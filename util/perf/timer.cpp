#include "timer.h"

namespace humming::util::perf {

std::string printElapsed(const nanoseconds &elapsed_ns) {
  std::stringstream ss;
  ss << std::fixed << std::setprecision(3);
  auto ns_count = elapsed_ns.count();
  if (ns_count < 1000) {
    ss << elapsed_ns.count() << " ns";
  } else if (ns_count < 1000000) {
    auto elapsed_us = duration_cast<microseconds>(elapsed_ns);
    ss << elapsed_ns.count() / 1000.0 << " µs";
  } else if (ns_count < 1000000000) {
    std::chrono::duration<double, std::milli> elapsed_ms = elapsed_ns;
    ss << elapsed_ms.count() << " ms";
  } else if (ns_count < 60000000000LL) {
    std::chrono::duration<double> elapsed_s = elapsed_ns;
    ss << elapsed_s.count() << " s";
  } else {
    std::chrono::duration<double, std::ratio<60>> elapsed_min = elapsed_ns;
    ss << elapsed_min.count() << " min";
  }
  return ss.str();
}

} // namespace humming::util::perf
