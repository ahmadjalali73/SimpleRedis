#include "../include/hashtable.h"
#include <assert.h>
#include <iostream>
#include <stdlib.h>

const size_t k_max_load_factor = 8;
const size_t k_resizing_work = 128;

enum {
  SER_NIL = 0,
  SER_ERR = 1,
  SER_STR = 2,
  SER_INT = 3,
  SER_ARR = 4,
};

static void h_init(HTab *htab, size_t size) {
  assert(size > 0 && ((size - 1) & size) == 0);
  htab->tab = (HNode **)calloc(sizeof(HNode *), size);
  htab->mask = size - 1;
  htab->size = 0;
}

static void h_insert(HTab *htab, HNode *hnode) {
  size_t pos = hnode->hcode & htab->mask;
  HNode *next = htab->tab[pos];
  hnode->next = next;
  htab->tab[pos] = hnode;
  htab->size++;
}

static HNode *h_detach(HTab *htab, HNode **from) {
  HNode *node = *from;
  *from = (*from)->next;
  htab->size--;
  return node;
}

static void hm_start_resizing(HMap *hmap) {
  assert(hmap->ht2.tab == NULL);
  hmap->ht2 = hmap->ht1;
  h_init(&hmap->ht1, (hmap->ht1.mask + 1) * 2);
  hmap->resizing_pos = 0;
}

static void hm_help_resizing(HMap *hmap) {
  if (hmap->ht2.tab == NULL)
    return;

  size_t nwork = 0;
  while (nwork < k_resizing_work && hmap->ht2.size > 0) {
    HNode **from = &hmap->ht2.tab[hmap->resizing_pos];
    if (!*from) {
      hmap->resizing_pos++;
      continue;
    }
    h_insert(&hmap->ht1, h_detach(&hmap->ht2, from));
    nwork++;
  }

  if (hmap->ht2.size == 0) {
    free(hmap->ht2.tab);
    hmap->ht2 = HTab{};
  }
}

HNode **h_lookup(HTab *htab, HNode *key, bool (*cmp)(HNode *, HNode *)) {
  if (!htab->tab)
    return NULL;

  size_t pos = key->hcode & htab->mask;
  HNode **from = &htab->tab[pos];
  while (*from) {
    if (cmp(*from, key))
      return from;
    from = &(*from)->next;
  }
  return NULL;
}

HNode *hm_lookup(HMap *hmap, HNode *key, bool (*cmp)(HNode *, HNode *)) {
  hm_help_resizing(hmap);
  HNode **from = h_lookup(&hmap->ht1, key, cmp);
  if (!from) {
    from = h_lookup(&hmap->ht2, key, cmp);
  }
  return from ? *from : NULL;
}

void hm_insert(HMap *hmap, HNode *node) {
  if (!hmap->ht1.tab)
    h_init(&hmap->ht1, 4);

  h_insert(&hmap->ht1, node);
  if (!hmap->ht2.tab) {
    size_t load_factor = hmap->ht1.size / (hmap->ht1.mask + 1);
    if (load_factor >= k_max_load_factor)
      hm_start_resizing(hmap);
  }
  hm_help_resizing(hmap);
}

HNode *hm_pop(HMap *hmap, HNode *key, bool (*cmp)(HNode *, HNode *)) {
  hm_help_resizing(hmap);
  HNode **from = h_lookup(&hmap->ht1, key, cmp);
  if (from)
    return h_detach(&hmap->ht1, from);
  from = h_lookup(&hmap->ht2, key, cmp);
  if (from)
    return h_detach(&hmap->ht2, from);
  return NULL;
}

size_t hm_size(HMap *hmap) { return hmap->ht1.size + hmap->ht2.size; }

void hm_destroy(HMap *hmap) {
  assert(hmap->ht1.size + hmap->ht2.size == 0);
  free(hmap->ht1.tab);
  free(hmap->ht2.tab);
  *hmap = HMap{};
}