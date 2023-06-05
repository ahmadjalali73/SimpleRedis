#include "../include/common.h"
#include "../include/hashtable.h"
#include "../include/heap.h"
#include "../include/list.h"
#include "../include/thread_pool.h"
#include "../include/user.h"
#include "../include/zset.h"
#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <math.h>
#include <netinet/ip.h>
#include <poll.h>
#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

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
  uint64_t idle_start = 0;
  DList idle_list;
  User client;
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

enum {
  ERR_UNKNOWN = 1,
  ERR_2BIG = 2,
  ERR_TYPE = 3,
  ERR_ARG = 4,
  ERR_AUTH = 5,
  ERR_FILE = 6,
};

static struct {
  HMap db;
  HMap user_db;
  HMap user_pass_db;
  std::vector<Conn *> fd2conn;
  DList idle_list;
  std::vector<HeapItem> heap;
  TheadPool tp;
} g_data;

enum {
  T_STR = 0,
  T_ZSET = 1,
};

struct Entry {
  struct HNode node;
  std::string val;
  std::string key;
  uint32_t type = 0;
  ZSet *zset = NULL;
  size_t heap_idx = -1;
};

struct UserEntry {
  struct HNode node;
  User username;
  ZSet *zset = NULL;
  size_t heap_idx;
};

static bool entry_eq(HNode *lhs, HNode *rhs) {
  struct Entry *le = container_of(lhs, struct Entry, node);
  struct Entry *re = container_of(rhs, struct Entry, node);
  return lhs->hcode == rhs->hcode && le->key == re->key;
}

static bool user_entry_eq(HNode *lhs, HNode *rhs) {
  struct Entry *le = container_of(lhs, struct Entry, node);
  struct Entry *re = container_of(rhs, struct Entry, node);
  return lhs->hcode == rhs->hcode && le->key == re->key && le->val == re->val;
}

static bool user_eq(HNode *lhs, HNode *rhs) {
  struct UserEntry *le = container_of(lhs, struct UserEntry, node);
  struct UserEntry *re = container_of(rhs, struct UserEntry, node);
  return lhs->hcode == rhs->hcode && le->username == re->username;
}

static void die(const char *msg) {
  int err = errno;
  fprintf(stderr, "[%d] %s\n", err, msg);
  abort();
}

static uint64_t get_monotonic_usec() {
  timespec tv = {0, 0};
  clock_gettime(CLOCK_MONOTONIC, &tv);
  return uint64_t(tv.tv_sec) * 1000000 + tv.tv_nsec / 1000;
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
  // std::cout << std::string((char *)&data[4], len) << std::endl;
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

static void out_nil(std::string &out) { out.push_back(SER_NIL); }

static void out_str(std::string &out, const char *s, size_t size) {
  out.push_back(SER_STR);
  uint32_t len = (uint32_t)size;
  out.append((char *)&len, 4);
  out.append(s, len);
}

static void out_str(std::string &out, const std::string &val) {
  return out_str(out, val.data(), val.size());
}

static void out_int(std::string &out, int64_t val) {
  out.push_back(SER_INT);
  out.append((char *)&val, 8);
}

static void out_dbl(std::string &out, double val) {
  out.push_back(SER_DBL);
  out.append((char *)&val, 8);
}

static void out_err(std::string &out, int32_t code, const std::string &msg) {
  out.push_back(SER_ERR);
  out.append((char *)&code, 4);
  uint32_t len = (uint32_t)msg.size();
  out.append((char *)&len, 4);
  out.append(msg);
}

static void out_arr(std::string &out, uint32_t n) {
  out.push_back(SER_ARR);
  out.append((char *)&n, 4);
}

static void out_update_arr(std::string &out, uint32_t n) {
  out.push_back(SER_ARR);
  out.append((char *)&n, 4);
}

static bool is_auth(User &client) {
  UserEntry new_user;
  new_user.username.set_ip(client.get_ip());
  new_user.node.hcode = int_hash(new_user.username.get_ip());

  HNode *node = hm_lookup(&g_data.user_db, &new_user.node, &user_eq);
  if (node) {
    UserEntry *uent = container_of(node, UserEntry, node);
    uint64_t now = get_monotonic_usec();
    if (uent->username.is_auth_yet(now)) {
      uent->username.update_time();
      return true;
    } else {
      return false;
    }
  } else {
    return false;
  }
}

static void do_get(std::vector<std::string> &cmd, std::string &out,
                   User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }

  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
  if (!node) {
    return out_nil(out);
  }
  Entry *ent = container_of(node, Entry, node);
  if (ent->type != T_STR)
    return out_err(out, ERR_TYPE, "expect string type");
  return out_str(out, ent->val);
}

static void do_set(std::vector<std::string> &cmd, std::string &out,
                   User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
  if (node) {
    Entry *ent = container_of(node, Entry, node);
    if (ent->type != T_STR)
      return out_err(out, ERR_TYPE, "expect string type");
  } else {
    Entry *ent = new Entry();
    ent->key.swap(key.key);
    ent->node.hcode = key.node.hcode;
    ent->val.swap(cmd[2]);
    hm_insert(&g_data.db, &ent->node);
  }
  return out_nil(out);
}

static void entry_set_ttl(Entry *ent, int64_t ttl_ms) {
  if (ttl_ms < 0 && ent->heap_idx != (size_t)-1) {
    size_t pos = ent->heap_idx;
    g_data.heap[pos] = g_data.heap.back();
    g_data.heap.pop_back();
    if (pos < g_data.heap.size())
      heap_update(g_data.heap.data(), pos, g_data.heap.size());
    ent->heap_idx = -1;
  } else if (ttl_ms >= 0) {
    size_t pos = ent->heap_idx;
    if (pos == (size_t)-1) {
      HeapItem item;
      item.ref = &ent->heap_idx;
      g_data.heap.push_back(item);
      pos = g_data.heap.size() - 1;
    }
    g_data.heap[pos].val = get_monotonic_usec() + (uint64_t)ttl_ms * 1000;
    heap_update(g_data.heap.data(), pos, g_data.heap.size());
  }
}

static void entry_destroy(Entry *ent) {
  switch (ent->type) {
  case T_ZSET:
    zset_dispose(ent->zset);
    delete ent->zset;
    break;
  }
  delete ent;
}

static void entry_del_async(void *arg) { entry_destroy((Entry *)arg); }

static void entry_del(Entry *ent) {
  entry_set_ttl(ent, -1);
  const size_t k_large_container_size = 10000;
  bool too_big = false;
  switch (ent->type) {
  case T_ZSET:
    too_big = hm_size(&ent->zset->hmap) > k_large_container_size;
    break;
  }

  if (too_big) {
    thread_pool_queue(&g_data.tp, &entry_del_async, ent);
  } else {
    entry_destroy(ent);
  }
}

static void do_del(std::vector<std::string> &cmd, std::string &out,
                   User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  HNode *node = hm_pop(&g_data.db, &key.node, &entry_eq);
  if (node) {
    entry_del(container_of(node, Entry, node));
  }
  return out_int(out, node ? 1 : 0);
}

static void h_scan(HTab *tab, void (*f)(HNode *, void *), void *arg) {
  if (tab->size == 0)
    return;
  for (size_t i = 0; i < tab->mask + 1; ++i) {
    HNode *node = tab->tab[i];
    while (node) {
      f(node, arg);
      node = node->next;
    }
  }
}

static void cb_scan(HNode *node, void *arg) {
  std::string &out = *(std::string *)arg;
  out_str(out, container_of(node, Entry, node)->key);
}

static void do_keys(std::vector<std::string> &cmd, std::string &out,
                    User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }
  (void)cmd;
  out_arr(out, (uint32_t)hm_size(&g_data.db));
  h_scan(&g_data.db.ht1, &cb_scan, &out);
  h_scan(&g_data.db.ht2, &cb_scan, &out);
}

static bool str2dbl(const std::string &s, double &out) {
  char *endp = NULL;
  out = strtod(s.c_str(), &endp);
  return endp == s.c_str() + s.size() && !isnan(out);
}

static bool str2int(const std::string &s, int64_t &out) {
  char *endp = NULL;
  out = strtoll(s.c_str(), &endp, 10);
  return endp == s.c_str() + s.size();
}

static void do_expire(std::vector<std::string> &cmd, std::string &out,
                      User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }
  int64_t ttl_ms = 0;
  if (!str2int(cmd[2], ttl_ms)) {
    return out_err(out, ERR_ARG, "expect int 64");
  }
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
  if (node) {
    Entry *ent = container_of(node, Entry, node);
    entry_set_ttl(ent, ttl_ms);
    return out_int(out, node ? 1 : 0);
  }
}

static void do_ttl(std::vector<std::string> &cmd, std::string &out,
                   User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

  HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
  if (!node)
    return out_int(out, -2);
  Entry *ent = container_of(node, Entry, node);
  if (ent->heap_idx == (size_t)-1)
    return out_int(out, -1);
  uint64_t expire_at = g_data.heap[ent->heap_idx].val;
  uint64_t now_us = get_monotonic_usec();
  return out_int(out, expire_at > now_us ? (expire_at - now_us) / 1000 : 0);
}

static void do_zadd(std::vector<std::string> &cmd, std::string &out,
                    User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }
  double score = 0;
  if (!str2dbl(cmd[2], score))
    return out_err(out, ERR_ARG, "expect fp number");
  Entry key;
  key.key.swap(cmd[1]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
  HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);

  Entry *ent = NULL;
  if (!hnode) {
    ent = new Entry();
    ent->key.swap(key.key);
    ent->node.hcode = key.node.hcode;
    ent->type = T_ZSET;
    ent->zset = new ZSet();
    hm_insert(&g_data.db, &ent->node);
  } else {
    ent = container_of(hnode, Entry, node);
    if (ent->type != T_ZSET)
      return out_err(out, ERR_TYPE, "expect zset");
  }
  const std::string &name = cmd[3];
  bool added = zset_add(ent->zset, name.data(), name.size(), score);
  return out_int(out, (int64_t)added);
}

static bool expect_zset(std::string &out, std::string &s, Entry **ent) {
  Entry key;
  key.key.swap(s);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
  HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
  if (!hnode) {
    out_nil(out);
    return false;
  }

  *ent = container_of(hnode, Entry, node);
  if ((*ent)->type != T_ZSET) {
    out_err(out, ERR_TYPE, "expect zset");
    return false;
  }
  return true;
}

static void do_zrem(std::vector<std::string> &cmd, std::string &out,
                    User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }
  Entry *ent = NULL;
  if (!expect_zset(out, cmd[1], &ent))
    return;
  const std::string &name = cmd[2];
  ZNode *znode = zset_pop(ent->zset, name.data(), name.size());
  if (znode)
    znode_del(znode);

  return out_int(out, znode ? 1 : 0);
}

static void do_zscore(std::vector<std::string> &cmd, std::string &out,
                      User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }
  Entry *ent = NULL;
  if (!expect_zset(out, cmd[1], &ent))
    return;
  const std::string &name = cmd[1];
  ZNode *znode = zset_lookup(ent->zset, name.data(), name.size());
  return znode ? out_dbl(out, znode->score) : out_nil(out);
}

static void do_zquery(std::vector<std::string> &cmd, std::string &out,
                      User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }
  double score = 0;
  if (str2dbl(cmd[2], score))
    return out_err(out, ERR_ARG, "expect fp number");

  const std::string &name = cmd[3];
  int64_t offset = 0;
  int64_t limit = 0;
  if (!str2int(cmd[4], offset))
    return out_err(out, ERR_ARG, "expect int");
  if (!str2int(cmd[5], limit))
    return out_err(out, ERR_ARG, "expect int");
  Entry *ent = NULL;
  if (!expect_zset(out, cmd[1], &ent)) {
    if (out[0] == SER_NIL) {
      out.clear();
      out_arr(out, 0);
    }
    return;
  }

  if (limit <= 0)
    return out_arr(out, 0);
  ZNode *znode = zset_query(ent->zset, score, name.data(), name.size(), offset);
  out_arr(out, 0);
  uint32_t n = 0;
  while (znode && (int64_t)n < limit) {
    out_str(out, znode->name, znode->len);
    out_dbl(out, znode->score);
    znode = container_of(avl_offset(&znode->tree, +1), ZNode, tree);
    n += 2;
  }
  return out_update_arr(out, n);
}

static void do_auth(std::vector<std::string> &cmd, std::string &out,
                    User &client) {
  Entry key;
  key.key.swap(cmd[1]);
  key.val.swap(cmd[2]);
  key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
  HNode *node_auth = hm_lookup(&g_data.user_pass_db, &key.node, &user_entry_eq);
  if (!node_auth) {
    return out_err(out, ERR_AUTH, "wrong authentication");
  }

  // Entry *new_key = container_of(node_auth, Entry, node);
  // if()

  UserEntry new_user;
  new_user.username.set_ip(client.get_ip());
  new_user.node.hcode = int_hash(new_user.username.get_ip());

  HNode *node = hm_lookup(&g_data.user_db, &new_user.node, &user_eq);
  if (node) {
    UserEntry *uent = container_of(node, UserEntry, node);
    uent->username.update_time();
  } else {
    UserEntry *uent = new UserEntry();
    uent->username.set_ip(new_user.username.get_ip());
    uent->node.hcode = new_user.node.hcode;
    hm_insert(&g_data.user_db, &uent->node);
  }
  // out = "successfull authentication";
  out_nil(out);
}

static void w_scan(HTab *tab, void (*f)(HNode *, void *), void *arg,
                   std::string path) {
  if (tab->size == 0)
    return;
  for (size_t i = 0; i < tab->mask + 1; ++i) {
    HNode *node = tab->tab[i];
    while (node) {
      f(node, arg);
      node = node->next;
    }
  }
}

static void write_to_file(std::string key, std::string value,
                          std::ofstream &out_file) {
  out_file << key << "," << value << std::endl;
}

static void h_scan(HTab *tab, std::ofstream &out_file) {
  for (size_t i; i < tab->mask + 1; ++i) {
    HNode *node = tab->tab[i];
    while (node) {
      Entry *ent = container_of(node, Entry, node);
      // std::cout << ent->key << std::endl;
      write_to_file(ent->key, ent->val, out_file);
      node = node->next;
    }
  }
}

static void do_write(std::vector<std::string> &cmd, std::string &out,
                     User &client) {
  if (!is_auth(client)) {
    return out_err(out, ERR_AUTH, "you are not authenticated");
  }

  std::string path(cmd[1]);
  std::ofstream out_file;
  out_file.open(path);
  if (out_file.fail()) {
    out_err(out, ERR_FILE, "Could not open file");
    return;
  }

  h_scan(&g_data.db.ht1, out_file);
  h_scan(&g_data.db.ht2, out_file);

  out_file.close();
  out_arr(out, (uint32_t)hm_size(&g_data.db));
}

static void do_request(std::vector<std::string> &cmd, std::string &out,
                       User &client) {
  if (cmd.size() == 1 && cmd_is(cmd[0], "keys")) {
    do_keys(cmd, out, client);
  } else if (cmd.size() == 2 & cmd_is(cmd[0], "get")) {
    do_get(cmd, out, client);
  } else if (cmd.size() == 3 && cmd_is(cmd[0], "set")) {
    do_set(cmd, out, client);
  } else if (cmd.size() == 2 && cmd_is(cmd[0], "del")) {
    do_del(cmd, out, client);
  } else if (cmd.size() == 3 && cmd_is(cmd[0], "pexpire")) {
    do_expire(cmd, out, client);
  } else if (cmd.size() == 2 && cmd_is(cmd[0], "pttl")) {
    do_ttl(cmd, out, client);
  } else if (cmd.size() == 4 && cmd_is(cmd[0], "zadd")) {
    do_zadd(cmd, out, client);
  } else if (cmd.size() == 3 && cmd_is(cmd[0], "zrem")) {
    do_zrem(cmd, out, client);
  } else if (cmd.size() == 3 && cmd_is(cmd[0], "zscore")) {
    do_zscore(cmd, out, client);
  } else if (cmd.size() == 6 && cmd_is(cmd[0], "zquery")) {
    do_zquery(cmd, out, client);
  } else if (cmd.size() == 3 && cmd_is(cmd[0], "auth")) {
    do_auth(cmd, out, client);
  } else if (cmd.size() == 2 && cmd_is(cmd[0], "write")) {
    do_write(cmd, out, client);
  } else {
    out_err(out, ERR_UNKNOWN, "Unkown command");
  }
}

static void conn_put(std::vector<Conn *> &fd_vector, Conn *conn) {
  if (fd_vector.size() <= (size_t)conn->fd) {
    fd_vector.resize(conn->fd + 1);
  }
  fd_vector[conn->fd] = conn;
}

static int32_t accept_new_connection(int fd) {
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
  new_conn->idle_start = get_monotonic_usec();
  new_conn->client.set_ip(client_addr.sin_addr.s_addr);
  dlist_insert_before(&g_data.idle_list, &new_conn->idle_list);
  conn_put(g_data.fd2conn, new_conn);
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

  std::vector<std::string> cmd;
  if (0 != parse_req(&conn->rbuf[4], len, cmd)) {
    std::cout << "bad req" << std::endl;
    conn->state = STATE_END;
    return false;
  }

  std::string out;
  do_request(cmd, out, conn->client);

  if (4 + out.size() > k_max_msg) {
    out.clear();
    out_err(out, ERR_2BIG, "response is too big");
  }
  uint32_t wlen = (uint32_t)out.size();

  memcpy(&conn->wbuf[0], &wlen, 4);
  memcpy(&conn->wbuf[4], out.data(), out.size());
  conn->wbuf_size = 4 + wlen;

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
  conn->idle_start = get_monotonic_usec();
  dlist_detach(&conn->idle_list);
  dlist_insert_before(&g_data.idle_list, &conn->idle_list);
  if (conn->state == STATE_REQ) {
    state_req(conn);
  } else if (conn->state == STATE_RES) {
    state_res(conn);
  } else {
    assert(0);
  }
}

const uint64_t k_idle_timeout_ms = 5 * 1000;

static uint32_t next_timer_ms() {
  if (dlist_empty(&g_data.idle_list))
    return 10000;
  uint64_t now_us = get_monotonic_usec();
  Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
  uint64_t next_us = next->idle_start + k_idle_timeout_ms * 1000;
  if (next_us <= now_us)
    return 0;
  return (uint32_t)((next_us - now_us) / 1000);
  return 10000;
}

static void conn_done(Conn *conn) {
  g_data.fd2conn[conn->fd] = NULL;
  (void)close(conn->fd);
  dlist_detach(&conn->idle_list);
  free(conn);
}

static bool hnode_same(HNode *lhs, HNode *rhs) { return lhs == rhs; }

static void process_timers() {
  uint64_t now_us = get_monotonic_usec() + 1000;
  while (!dlist_empty(&g_data.idle_list)) {
    Conn *next = container_of(g_data.idle_list.next, Conn, idle_list);
    uint64_t next_us = next->idle_start + k_idle_timeout_ms * 1000;
    if (next_us >= now_us)
      break;
    std::cout << "removing idle connection: " << next->fd << std::endl;
    conn_done(next);
  }
  const size_t k_max_works = 2000;
  size_t nworks = 0;
  while (!g_data.heap.empty() && g_data.heap[0].val < now_us) {
    Entry *ent = container_of(g_data.heap[0].ref, Entry, heap_idx);
    HNode *node = hm_pop(&g_data.db, &ent->node, &hnode_same);
    assert(node == &ent->node);
    entry_del(ent);
    if (nworks >= k_max_works) {
      break;
    }
  }
}

static void insert_user_pass() {
  Entry *ahmad = new Entry();
  Entry *abbas = new Entry();

  ahmad->key = "ahmad";
  ahmad->val = "123";
  ahmad->node.hcode = str_hash((uint8_t *)ahmad->key.data(), ahmad->key.size());
  abbas->key = "abbas";
  abbas->node.hcode = str_hash((uint8_t *)abbas->key.data(), abbas->key.size());
  abbas->val = "123";

  hm_insert(&g_data.user_pass_db, &ahmad->node);
  hm_insert(&g_data.user_pass_db, &abbas->node);
}

int main() {
  insert_user_pass();
  std::string file_location("/etc/redis/config.json");
  FILE *fp = fopen(file_location.c_str(), "rb");
  if (!fp) {
    die("Error: unable to open config file at /etc/redis/config.json");
  }
  char readBuffer[65536];

  rapidjson::FileReadStream streamed_file(fp, readBuffer, sizeof(readBuffer));
  rapidjson::Document document;
  document.ParseStream(streamed_file);
  if (document.HasParseError()) {
    fclose(fp);
    die("Error: failed to parse JSON document");
  }
  fclose(fp);
  std::string ip;
  uint16_t port = -1;
  int num_threads = -1;

  bool correct_config = true;

  if (document.HasMember("ip") && document["ip"].IsString()) {
    ip = document["ip"].GetString();
  } else {
    correct_config = false;
  }
  if (document.HasMember("port") && document["port"].IsInt()) {
    port = (uint16_t)document["port"].GetInt();
  } else {
    correct_config = false;
  }

  if (document.HasMember("num_thread") && document["num_thread"].IsInt()) {
    num_threads = document["num_thread"].GetInt();
  } else {
    correct_config = false;
  }
  if (!correct_config) {
    die("your config has some problems!");
  }

  dlist_init(&g_data.idle_list);
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    die("socket() error");
  }
  int val = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
  struct sockaddr_in server_addr = {};
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = ntohs(port);
  if (inet_pton(AF_INET, ip.c_str(), &server_addr.sin_addr) != 1) {
    die("wrong ip address in config file");
  }

  int rv = bind(fd, (const sockaddr *)&server_addr, sizeof(server_addr));
  if (rv) {
    die("bind() error");
  }

  rv = listen(fd, SOMAXCONN);
  if (rv) {
    die("listen() error");
  }

  fd_set_nb(fd);

  thread_pool_init(&g_data.tp, num_threads);

  std::vector<struct pollfd> poll_args;
  while (true) {
    poll_args.clear();

    struct pollfd pfd = {fd, POLLIN, 0};
    poll_args.push_back(pfd);
    for (Conn *conn : g_data.fd2conn) {
      if (!conn)
        continue;
      struct pollfd pfd = {};
      pfd.fd = conn->fd;
      pfd.events = (conn->state == STATE_REQ) ? POLLIN : POLLOUT;
      pfd.events |= pfd.events | POLLERR;
      poll_args.push_back(pfd);
    }

    int timeout_ms = (int)next_timer_ms();
    int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), timeout_ms);
    if (rv < 0) {
      die("poll");
    }

    for (size_t i = 0; i < poll_args.size(); ++i) {
      if (poll_args[i].revents) {
        if (g_data.fd2conn.size() >= poll_args[i].fd) {
          Conn *conn = g_data.fd2conn[poll_args[i].fd];
          if (conn) {
            connection_io(conn);
            if (conn->state == STATE_END) {
              conn_done(conn);
            }
          }
        }
      }
    }

    process_timers();

    if (poll_args[0].revents) {
      (void)accept_new_connection(fd);
    }
  }

  return 0;
}
