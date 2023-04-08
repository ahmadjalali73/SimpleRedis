#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <iostream>
#include <netinet/ip.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include "../include/common.h"

const size_t k_msg_max = 4096;

static void die(const char *msg) {
  int err = errno;
  std::cerr << "[" << err << "] " << msg << std::endl;
  abort();
}

static int32_t read_full(int fd, char *rbuf, size_t len) {
  while (len > 0) {
    ssize_t rv = read(fd, rbuf, len);
    if (rv <= 0) {
      return -1;
    }
    assert((size_t)rv <= len);
    len -= (size_t)rv;
    rbuf += rv;
  }
  return 0;
}

static int32_t write_all(int fd, const char *wbuf, size_t n) {
  while (n > 0) {
    ssize_t rv = write(fd, wbuf, n);
    if (rv <= 0) {
      return -1;
    }
    assert((size_t)rv <= n);
    n -= (size_t)rv;
    wbuf += rv;
  }
  return 0;
}

static int32_t send_req(int fd, std::vector<std::string> cmd) {
  uint32_t len = 4;
  for (const std::string &s : cmd) {
    len += s.size();
  }
  if (len > k_msg_max)
    return -1;

  char wbuf[4 + k_msg_max];
  memcpy(&wbuf[0], &len, 4);
  uint32_t n = cmd.size();
  memcpy(&wbuf[4], &n, 4);
  size_t cur = 8;
  for (const std::string &s : cmd) {
    int p = s.size();
    memcpy(&wbuf[cur], &p, 4);
    memcpy(&wbuf[cur + 4], s.data(), s.size());
    cur += 4 + s.size();
  }
  return write_all(fd, wbuf, 4 + len);
}

static int on_response(const uint8_t *data, size_t size) {
  if (size < 1) {
    std::cerr << "bad response 1" << std::endl;
    return -1;
  }
  switch (data[0]) {
  case SER_NIL:
    printf("(nil)\n");
    return 1;
  case SER_ERR:
    if (size < 1 + 8) {
      std::cerr << "bad response 2" << std::endl;
      return -1;
    }
    {
      int32_t code = 0;
      uint32_t len = 0;
      memcpy(&code, &data[1], 4);
      memcpy(&len, &data[1 + 4], 4);
      if (size < 1 + 8 + len) {
        std::cerr << "bad response 3" << std::endl;
        return -1;
      }
      printf("(err) %d %.*s\n", code, len, &data[1 + 8]);
      return 1 + 8 + len;
    }
  case SER_STR:
    if (size < 1 + 4) {
      std::cerr << "bad response 4" << std::endl;
      return -1;
    }
    {
      uint32_t len = 0;
      memcpy(&len, &data[1], 4);
      if (size < 1 + 4 + len) {
        std::cerr << "bad response 5" << std::endl;
        return -1;
      }
      printf("(str) %.*s\n", len, &data[1 + 4]);
      return 1 + 4 + len;
    }
  case SER_INT:
    if (size < 1 + 8) {
      std::cerr << "bad response 6" << std::endl;
      return -1;
    }
    {
      int64_t val = 0;
      memcpy(&val, &data[1], 8);
      printf("(int) %ld\n", val);
      return 1 + 8;
    }
  case SER_DBL:
    if (size < 1 + 8) {
      std::cerr << "bad response 7" << std::endl;
      return -1;
    }
    {
      double val = 0;
      memcpy(&val, &data[1], 8);
      printf("(dbl) %g\n", val);
      return 1 + 8;
    }
  case SER_ARR:
    if (size < 1 + 4) {
      std::cerr << "bad response 8" << std::endl;
      return -1;
    }
    {
      uint32_t len = 0;
      memcpy(&len, &data[1], 4);
      printf("(arr) len=%u\n", len);
      size_t arr_bytes = 1 + 4;
      for (uint32_t i = 0; i < len; ++i) {
        int32_t rv = on_response(&data[arr_bytes], size - arr_bytes);
        if (rv < 0) {
          return rv;
        }
        arr_bytes += (size_t)rv;
      }
      printf("(arr) end\n");
      return (int32_t)arr_bytes;
    }
  default:
    std::cerr << "bad response 9" << std::endl;
    return -1;
  }
}

static int32_t read_res(int fd) {
  char rbuf[4 + k_msg_max + 1];
  errno = 0;
  int32_t err = read_full(fd, rbuf, 4);
  if (err) {
    if (errno == 0) {
      std::cerr << "EOF" << std::endl;
    } else {
      std::cerr << "read() error" << std::endl;
    }
    return err;
  }

  uint32_t len = 0;
  memcpy(&len, rbuf, 4);
  if (len > k_msg_max) {
    std::cerr << "msg too long" << std::endl;
    return -1;
  }

  err = read_full(fd, &rbuf[4], len);
  if (err) {
    std::cerr << "read() error" << std::endl;
    return err;
  }

  int rv = on_response((uint8_t *)&rbuf[4], len);
  if (rv > 0 && (uint32_t)rv != len) {
    std::cerr << "bad response 10" << std::endl;
    return -1;
  }
  return rv;
}

int main(int argc, char **argv) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    die("socket() problem");
  }
  struct sockaddr_in addr = {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
  addr.sin_port = ntohs(1234);
  int rv = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
  if (rv) {
    die("connect() problem");
  }

  std::vector<std::string> cmd;
  for (int i = 0; i < argc; i++)
    cmd.push_back(argv[i]);
  int32_t err = send_req(fd, cmd);
  if (err)
    goto L_DONE;
  err = read_res(fd);
  if (err)
    goto L_DONE;

L_DONE:
  close(fd);
  return 0;
}