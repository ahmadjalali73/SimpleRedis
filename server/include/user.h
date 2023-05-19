#include <chrono>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string>

class User {
private:
  uint32_t ip_address;
  bool is_auth = false;
  timespec auth_time;
  //   uint64_t auth_time;

public:
  User() {
    this->ip_address = 0;
    clock_gettime(CLOCK_MONOTONIC, &this->auth_time);
  }

  User(uint32_t ip) {
    this->ip_address = ip;
    clock_gettime(CLOCK_MONOTONIC, &this->auth_time);
  }

  void set_ip(uint32_t ip) { this->ip_address = ip; }

  uint32_t get_ip() { return this->ip_address; }

  bool is_auth_yet(uint64_t now) {
    return ((uint64_t)(this->auth_time.tv_sec + 300) * 1000000 +
            this->auth_time.tv_nsec / 1000) > now
               ? true
               : false;
  }

  void update_time() { clock_gettime(CLOCK_MONOTONIC, &this->auth_time); }

  bool operator==(User usr) {
    return this->ip_address == usr.ip_address ? true : false;
  }
};