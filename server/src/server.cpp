#include "../include/hashtable.h"
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <netinet/ip.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#define container_of(ptr, type, member)                                        \
  ({                                                                           \
    const typeof(((type *)0)->member) *__mptr = (ptr);                         \
    (type *)((char *)__mptr - offsetof(type, member));                         \
  })

const size_t k_max_msg = 4096;

struct Conn {
  int fd = -1;
  uint32_t state = 0; // either STATE_REQ or STATE_RES
  // buffer for reading
  size_t rbuf_size = 0;
  uint8_t rbuf[4 + k_max_msg];
  // buffer for writing
  size_t wbuf_size = 0;
  size_t wbuf_sent = 0;
  uint8_t wbuf[4 + k_max_msg];
};

enum {
  STATE_REQ = 0,
  STATE_RES = 1,
  STATE_END = 2, // mark the connection for deletion
};

enum {
  RES_OK = 0,
  RES_ERR = 1,
  RES_NX = 2,
};

static struct {
  HMap db;
} g_data;

struct Entry {
  struct HNode node;
  std::string val;
  std::string key;
};

static bool entry_eq(HNode *lhs, HNode *rhs) {
  struct Entry *le = container_of(lhs, struct Entry, node);
  struct Entry *re = container_of(rhs, struct Entry, node);
  return lhs->hcode == rhs->hcode && le->key == re->key;
}

static void die(const char *msg) {
  int err = errno;
  fprintf(stderr, "[%d] %s\n", err, msg);
  abort();
}

static bool cmd_is(const std::string &word, const char *cmd) {
  return 0 == strcasecmp(word.c_str(), cmd);
}

static void fd_set_nb(int fd) {
  errno = 0;
  int flag = fcntl(fd, F_GETFL, 0);
  if (errno) {
    die("fcntl get error");
  }

  flag |= O_NONBLOCK;
  fcntl(fd, F_SETFL, flag);
  if (errno) {
    die("fcntl set error");
  }
}

static int32_t parse_req(const uint8_t *data, size_t len,
                         std::vector<std::string> &out) {
  if (len < 4)
    return -1;
  std::cout << std::string((char *)&data[4], len) << std::endl;
  uint32_t n = 0;
  memcpy(&n, &data[0], 4);
  if (n > k_max_msg) {
    return -1;
  }
  size_t pos = 4;
  int i = 0;
  while (n--) {
    if (pos + 4 > len) {
      return -1;
    }
    uint32_t sz = 0;
    memcpy(&sz, &data[pos], 4);
    if (pos + 4 + sz > len) {
      std::cout << "pos + 4 + sz > len" << std::endl;
      return -1;
    }
    out.push_back(std::string((char *)&data[pos + 4], sz));
    // std::cout << "out[" << i << "]: " << out[i] << std::endl;
    i++;

    pos += 4 + sz;
  }

  if (pos != len) {
    std::cout << "pos != len" << std::endl;
    return -1;
  }
  return 0;
}

static uint64_t str_hash(uint8_t *data, size_t size) {
  uint32_t h = 0x811C9DC5;
  for (int i = 0; i < size; i++) {
    h = (h + data[i]) * 0x01000193;
  }
  return h;
}

static uint32_t do_get(std::vector<std::string> &cmd, uint8_t *res,
                       uint32_t *reslen) {
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
  if (!node) {
    return RES_NX;
  }
  const std::string &val = container_of(node, Entry, node)->val;
  std::cout << "val.data: " << val.data() << " val.size: " << val.size()
            << std::endl;
  assert(val.size() <= k_max_msg);
  memcpy(res, val.data(), val.size());
  *reslen = (uint32_t)val.size();
  return RES_OK;
}

static uint32_t do_set(std::vector<std::string> &cmd, uint8_t *res,
                       uint32_t *reslen) {
  (void)res;
  (void)reslen;
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
  if (node) {
    container_of(node, Entry, node)->val.swap(cmd[2]);
  } else {
    Entry *ent = new Entry();
    ent->key.swap(key.key);
    ent->node.hcode = key.node.hcode;
    ent->val.swap(cmd[2]);
    hm_insert(&g_data.db, &ent->node);
  }
  return RES_OK;
}

static uint32_t do_del(std::vector<std::string> &cmd, uint8_t *res,
                       uint32_t *reslen) {
  (void)res;
  (void)reslen;

  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  HNode *node = hm_pop(&g_data.db, &key.node, &entry_eq);
  if (node) {
    delete container_of(node, Entry, node);
  }
  return RES_OK;
}

static int32_t do_request(const uint8_t *req, uint32_t reqlen,
                          uint32_t *rescode, uint8_t *res, uint32_t *reslen) {
  std::vector<std::string> cmd;
  if (parse_req(req, reqlen, cmd) != 0) {
    std::cerr << "bad request" << std::endl;
    return -1;
  }
  if (cmd.size() == 2 && cmd_is(cmd[0], "get")) {
    *rescode = do_get(cmd, res, reslen);

  } else if (cmd.size() == 3 && cmd_is(cmd[0], "set")) {
    *rescode = do_set(cmd, res, reslen);
  } else if (cmd.size() == 3 && cmd_is(cmd[0], "del")) {
    *rescode = do_del(cmd, res, reslen);
  } else {
    *rescode = RES_ERR;
    const char *msg = "unknown command";
    strcpy((char *)res, msg);
    *reslen = sizeof(msg);
    return 0;
  }
  return 0;
}

static void conn_put(std::vector<Conn *> &fd_vector, Conn *conn) {
  if (fd_vector.size() <= (size_t)conn->fd) {
    fd_vector.resize(conn->fd + 1);
  }
  fd_vector[conn->fd] = conn;
}

static int32_t accept_new_connection(std::vector<Conn *> &fd_vector, int fd) {
  struct sockaddr_in client_addr = {};
  socklen_t socklen = sizeof(client_addr);
  int connfd = accept(fd, (struct sockaddr *)&client_addr, &socklen);
  if (connfd < 0) {
    std::cout << "new connection malloc" << std::endl;
    return -1;
  }

  fd_set_nb(connfd);
  struct Conn *new_conn = (struct Conn *)malloc(sizeof(struct Conn));
  if (!new_conn) {
    std::cout << "connection wil close" << std::endl;
    close(connfd);
    return -1;
  }
  new_conn->fd = connfd;
  new_conn->state = STATE_REQ;
  new_conn->rbuf_size = 0;
  new_conn->wbuf_size = 0;
  new_conn->wbuf_sent = 0;
  conn_put(fd_vector, new_conn);
  return 0;
}

static bool try_flush_buffer(Conn *conn) {
  ssize_t rv = 0;
  do {
    size_t remain = conn->wbuf_size - conn->wbuf_sent;
    rv = write(conn->fd, &conn->wbuf[conn->wbuf_sent], remain);
  } while (rv < 0 && errno == EINTR);
  if (rv < 0 && errno == EAGAIN) {
    // got EAGAIN, stop.
    return false;
  }
  if (rv < 0) {
    std::cerr << "write() error rv: " << rv << " errno: " << errno << std::endl;
    conn->state = STATE_END;
    return false;
  }
  conn->wbuf_sent += (size_t)rv;
  assert(conn->wbuf_sent <= conn->wbuf_size);
  if (conn->wbuf_sent == conn->wbuf_size) {
    conn->state = STATE_REQ;
    conn->wbuf_size = 0;
    conn->wbuf_sent = 0;
    return false;
  }
  return true;
}

static void state_res(Conn *conn) {
  while (try_flush_buffer(conn)) {
  }
}

static bool try_one_request(Conn *conn) {
  if (conn->rbuf_size < 4) {
    return false;
  }

  uint32_t len = 0;
  memcpy(&len, &conn->rbuf[0], 4);
  if (len > k_max_msg) {
    std::cerr << "msg too long" << std::endl;
    conn->state = STATE_END;
    return false;
  }
  if (4 + len > conn->rbuf_size) {
    return false;
  }

  uint32_t rescode = 0;
  uint32_t wlen = 0;

  int32_t err =
      do_request(&conn->rbuf[4], len, &rescode, &conn->wbuf[4 + 4], &wlen);
  if (err) {
    conn->state = STATE_END;
    return false;
  }
  wlen += 4;
  memcpy(&conn->wbuf[0], &wlen, 4);
  memcpy(&conn->wbuf[4], &rescode, 4);
  conn->wbuf_size = 4 + wlen;

  // remove the request from the buffer.
  // note: frequent memmove is inefficient.
  // note: need better handling for production code.
  size_t remain = conn->rbuf_size - 4 - len;
  if (remain) {
    memmove(conn->rbuf, &conn->rbuf[4 + len], remain);
  }
  conn->rbuf_size = remain;

  // change state
  conn->state = STATE_RES;
  state_res(conn);

  // continue the outer loop if the request was fully processed
  return (conn->state == STATE_REQ);
}

static bool try_fill_buffer(Conn *conn) {
  assert(conn->rbuf_size < sizeof(conn->rbuf));

  ssize_t rv = 0;
  do {
    size_t cap = sizeof(conn->rbuf) - conn->rbuf_size;
    rv = read(conn->fd, &conn->rbuf[conn->rbuf_size], cap);
    // size_t cap = sizeof(conn->rbuf) - conn->rbuf_size;
    // rv = read(conn->fd, &conn->rbuf[conn->rbuf_size], cap);
  } while (rv < 0 && errno == EAGAIN);

  if (rv < 0 && errno == EAGAIN)
    return false;
  if (rv < 0) {
    std::cerr << "read() error" << std::endl;
    conn->state = STATE_END;
    return false;
  }
  if (rv == 0) {
    if (conn->rbuf_size > 0) {
      std::cerr << "unexpected EOF" << std::endl;
    } else {
      std::cerr << "EOF" << std::endl;
    }
    conn->state = STATE_END;
    return false;
  }
  conn->rbuf_size += (size_t)rv;
  assert(conn->rbuf_size <= sizeof(conn->rbuf) - conn->rbuf_size);

  while (try_one_request(conn)) {
  }
  return (conn->state == STATE_REQ);
}

static void state_req(Conn *conn) {
  while (try_fill_buffer(conn)) {
  }
}

static void connection_io(Conn *conn) {
  if (conn->state == STATE_REQ) {
    state_req(conn);
  } else if (conn->state == STATE_RES) {
    state_res(conn);
  } else {
    assert(0);
  }
}

int main() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    die("socket() error");
  }
  int val = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
  struct sockaddr_in server_addr = {};
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = ntohs(1234);
  server_addr.sin_addr.s_addr = ntohl(0);

  int rv = bind(fd, (const sockaddr *)&server_addr, sizeof(server_addr));
  if (rv) {
    die("bind() error");
  }

  rv = listen(fd, SOMAXCONN);
  if (rv) {
    die("listen() error");
  }

  std::vector<Conn *> fd_vector;

  fd_set_nb(fd);

  std::vector<struct pollfd> poll_args;
  while (true) {
    poll_args.clear();

    struct pollfd pfd = {fd, POLLIN, 0};
    poll_args.push_back(pfd);
    for (Conn *conn : fd_vector) {
      if (!conn)
        continue;
      struct pollfd pfd = {};
      pfd.fd = conn->fd;
      pfd.events = (conn->state == STATE_REQ) ? POLLIN : POLLOUT;
      pfd.events |= POLLERR;
      poll_args.push_back(pfd);
    }
    int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), 1000);
    if (rv < 0) {
      die("poll");
    }

    for (size_t i = 0; i < poll_args.size(); ++i) {
      if (poll_args[i].revents) {
        if (fd_vector.size() >= poll_args[i].fd) {
          Conn *conn = fd_vector[poll_args[i].fd];
          if (conn) {
            connection_io(conn);
            if (conn->state == STATE_END) {
              fd_vector[conn->fd] = NULL;
              // fd_vector.erase(std::next(fd_vector.begin(), conn->fd));
              (void)close(conn->fd);
              free(conn);
            }
          }
        }
      }
    }

    if (poll_args[0].revents) {
      (void)accept_new_connection(fd_vector, fd);
    }
  }

  return 0;
}
