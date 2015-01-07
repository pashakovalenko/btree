#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>

#include <map>
#include <list>
#include <iostream>
#include <string>
#include <cstring>

using namespace std;

#include "mydb.h"

#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))

int db_close(struct DB *db) {
	db->close(db);
	return 0;
}

int db_del(struct DB *db, char *key, size_t key_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	return db->del(db, &keyt);
}

int db_get(struct DB *db, char *key, size_t key_len,
	   void **val, size_t *val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {0, 0};
	int rc = db->get(db, &keyt, &valt);
	*val = valt.data;
	*val_len = valt.size;
	return rc;
}

int db_put(struct DB *db, char *key, size_t key_len,
	   char *val, size_t val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {
		.data = val,
		.size = val_len
	};
	return db->put(db, &keyt, &valt);
}

int db_flush(struct DB *db)
{
    return 0;
}


//=======================================End of interface block========================================

void reser_set(char state, int pos, struct DB *db)
{
    db->reser[pos / 8] = db->reser[pos / 8] & !(1 << (pos % 8)) || (state << (pos % 8));
    return;
}

int reser_get(int pos, struct DB *db)
{
    return (db->reser[pos / 8] >> (pos % 8)) & 1;
}

struct BtreeNode *newNode(struct DB *db);

struct DB *dbcreate(const char *file, const struct DBC conf)
{
    //printf("Trying to create a DB\n");
    struct DB *db = (DB *)malloc(sizeof(*db));
    db->config = conf;
    db->dbfl = open(file, O_RDWR | O_CREAT, 0);
    db->maxnodecnt = conf.db_size / conf.chunk_size;
    db->get = get_inter;
    db->put = put_inter;
    db->del = del_inter;
    db->close = close_inter;
    db->froff = ceil(db->maxnodecnt * 1.0 / conf.chunk_size) + 1;
    db->reser = (char *)calloc(db->maxnodecnt / 8, 1);
    for (int i = 0; i < db->froff; i++) {
        reser_set(1, i, db);
    }

    db->chsize = conf.mem_size / conf.chunk_size;
    //db->chsize = 1000;
    //cout << db->chsize << endl;
    db->cache = (char *)malloc(db->chsize * conf.chunk_size);
    db->chmap = new map<int, int>;
    db->queue = (int *)malloc(db->chsize * sizeof(*(db->queue)));
    db->pos = (int *)malloc(db->chsize * sizeof(*(db->pos)));
    db->qp = 0;
    for (int i = 0; i < db->chsize; i++) {
        db->queue[i] = -i - 1;
        db->pos[i] = i;
        //(*db->chmap)[-i - 1] = i;
    }

    db->root = newNode(db);
    db->root->n = 0;
    db->root->maxn = (conf.chunk_size - 32) / (2 * sizeof(struct Record));
    db->root->islist = 1;

    //printf("DB successfully created\n");
    return db;
}


int keycmp(struct Key *a, struct Key *b) { //returns -1 if less, 0 if equal, 1 if more
    int res = memcmp(&(a->data), &(b->data), min(a->size, b->size));
    if (res == 0) {
        res = a->size - b->size;
    }
    return res;
}

/*int cache_pop(struct DB *db) {
    int t = db->lru->front();
    db->lru->pop_front();
    int res = (*db->chmap)[t];
    db->chmap->erase(t);
    return res;
}*/

int qfind(int off, struct DB *db) {
    /*if (db->chmap->find(off) != db->chmap->end()) {
        return (*db->chmap)[off];
    } else {
        return -1;
    }*/
    for (int i = 0; i < db->chsize; i++) {
        if (db->queue[i] == off) {
            return i;
        }
    }
    return -1;
}

struct BtreeNode *readNode(int offset, struct DB *db) {
    //cout << "read1" << endl;
    int pos;
    if ((pos = qfind(offset, db)) >= 0) {
        int p = db->qp;
        /*int k = db->queue[pos];
        db->queue[pos] = db->queue[p];
        db->queue[p] = k;
        k = db->pos[pos];
        db->pos[pos] = db->pos[p];
        db->pos[p] = k;
        (*db->chmap)[offset] = p;
        (*db->chmap)[db->queue[pos]] = pos;
        */
        pos = db->pos[pos];   //pos
        db->qp = (p + 1) % db->chsize;
    } else {
        int p = db->qp;
        //db->chmap->erase(db->queue[p]);
        pos = db->pos[p];
        db->queue[p] = offset;
        //(*db->chmap)[offset] = p;
        int l = 0;
        lseek(db->dbfl, offset * db->config.chunk_size, SEEK_SET);
        while ((l += read(db->dbfl, db->cache + db->config.chunk_size * pos + l,
            db->config.chunk_size - l)) < db->config.chunk_size) {}
        db->qp = (p + 1) % db->chsize;
    }
    struct BtreeNode *res = (BtreeNode *)calloc(1, db->config.chunk_size);
    memcpy(res, db->cache + pos * db->config.chunk_size, db->config.chunk_size);
    //cout << "read2" << endl;
    return res;
}

void writeNode(struct BtreeNode *node, struct DB *db) {
    //cout << "write1" << endl;
    int pos;
    if ((pos = qfind(node->offset, db)) >= 0) {
        pos = db->pos[pos];
        //db->lru->remove(offset);
        //db->lru->push_back(offset);
    } else {
        int p = db->qp;
        //db->chmap->erase(db->queue[p]);
        pos = db->pos[p];
        db->queue[p] = node->offset;
        //(*db->chmap)[node->offset] = p;
        db->qp = (p + 1) % db->chsize;
    }
    memcpy(db->cache + pos * db->config.chunk_size, node, db->config.chunk_size);
    int l = 0;
    lseek(db->dbfl, node->offset * db->config.chunk_size, SEEK_SET);
    while ((l += write(db->dbfl, ((char *)node) + l, db->config.chunk_size - l)) < db->config.chunk_size) {}
    //cout << "write2" << endl;
    return;
}

struct BtreeNode *newNode(struct DB *db) {
    struct BtreeNode *res = (BtreeNode *)calloc(1, db->config.chunk_size);//db->cache + cache_pos * db->config.chunk_size;
    while (reser_get(db->froff, db)) {
        db->froff++;
    }
    reser_set(1, db->froff, db);
    res->offset = db->froff;
    db->froff++;
    return res;
}

int close_inter(struct DB *db) {
    writeNode(db->root, db);
    db->root_off = db->root->offset;
    lseek(db->dbfl, 0, SEEK_SET);
    int l = 0;
    while ((l += write(db->dbfl, ((char *)db) + l, sizeof(*db) - l)) < sizeof(*db)) {};
    lseek(db->dbfl, db->config.chunk_size, SEEK_SET);
    l = 0;
    while ((l += write(db->dbfl, db->reser + l, db->maxnodecnt - l)) < db->maxnodecnt) {}
    free(db->reser);
    free(db->root);
    close(db->dbfl);
    free(db);
    //printf("Successfully closed\n");
    return 0;
}

struct DB *dbopen (const char *file)
{
    struct DB *db = (struct DB *)malloc(sizeof(*db));
    int f = open(file, O_RDWR | O_CREAT, 0);
    lseek(f, 0, SEEK_SET);
    int l = 0;
    while ((l += read(f, ((char *)db) + l, sizeof(*db) - l)) < sizeof(*db)) {}

    db->dbfl = f;
    db->get = get_inter;
    db->put = put_inter;
    db->del = del_inter;
    db->close = close_inter;

    db->reser = (char *)calloc(db->maxnodecnt, 1);
    lseek(db->dbfl, db->config.chunk_size, SEEK_SET);
    l = 0;
    while ((l += read(db->dbfl, db->reser + l, db->maxnodecnt - l)) < db->maxnodecnt) {}

    db->cache = (char *)malloc(db->chsize * db->config.chunk_size);
    db->chmap = new map<int, int>;
    db->queue = (int *)malloc(db->chsize * sizeof(*(db->queue)));
    db->pos = (int *)malloc(db->chsize * sizeof(*(db->pos)));
    db->qp = 0;
    for (int i = 0; i < db->chsize; i++) {
        db->queue[i] = -i - 1;
        db->pos[i] = i;
        //(*db->chmap)[-i - 1] = i;
    }

    db->root = readNode(db->root_off, db);
    //printf("DB successfully opened\n");
    return db;
}



void freeNode(struct BtreeNode *node, struct DB *db)
{
    reser_set(0, node->offset, db);
    db->froff = min(db->froff, node->offset);
    return;
}

int splitChild(struct BtreeNode *node, int pos, struct DB *db)
{
    struct BtreeNode *nch = newNode(db);
    struct BtreeNode *och = readNode(node->rec[pos].link, db);
    nch->maxn = och->maxn;
    nch->islist = och->islist;
    int size = och->maxn;
    nch->n = och->n - size;
    for (int i = 0; i < nch->n; i++) {
        nch->rec[i] = och->rec[i + size];
    }
    nch->rec[nch->n].link = och->rec[och->n].link;
    node->rec[node->n + 1].link = node->rec[node->n].link;
    for (int i = node->n; i > pos; i--) {
        node->rec[i] = node->rec[i - 1];
    }
    node->rec[pos].link = och->offset;
    node->rec[pos + 1].link = nch->offset;
    node->rec[pos].key = och->rec[size - 1].key;
    node->rec[pos].data = och->rec[size - 1].data;
    och->n = size - 1;
    node->n++;
    writeNode(nch, db);
    writeNode(och, db);
    writeNode(node, db);
    free(nch);
    free(och);
    return 0;
}

int get(struct BtreeNode *node, struct Key *key, struct Data *data, struct DB *db)
{
    int i = 0;
    while ((i < node->n) && (keycmp(&(node->rec[i].key), key) < 0)) {
        i++;
    }
    if ((i < node->n) && (keycmp(&(node->rec[i].key), key) == 0)) {
        *data = node->rec[i].data;
        return 0;
    } else if (!node->islist) {
        struct BtreeNode *child = readNode(node->rec[i].link, db);
        int result = get(child, key, data, db);
        free(child);
        return result;
    } else {
        return -1;
    }
}

int get_inter(struct DB *db, struct DBT *key, struct DBT *data)
{
    struct Key skey;
    skey.size = key->size;
    memcpy(&skey.data, key->data, skey.size);
    struct Data sdata;
    int res = get(db->root, &skey, &sdata, db);
    if (res < 0) {
        data->size = 0;
        data->data = NULL;
        return res;
    }
    data->size = sdata.size;
    data->data = (char *)calloc(sdata.size, 1);
    memcpy(data->data, &sdata.data, sdata.size);
    return res;
}

int put(struct BtreeNode *node, struct Key *key, struct Data *data, struct DB *db)
{
    int i = 0;
    while ((i < node->n) && (keycmp(&(node->rec[i].key), key) < 0)) {
        i++;
    }
    if (keycmp(&(node->rec[i].key), key) == 0) {
        node->rec[i].data = *data;
        writeNode(node, db);
        return 0;
    }
    if (!node->islist) {
        struct BtreeNode *child = readNode(node->rec[i].link, db);
        if (child->n == 2 * child->maxn - 1) {
            free(child);
            if (splitChild(node, i, db) < 0) {
                return -1;
            }
            if (keycmp(&(node->rec[i].key), key) < 0) {
                i++;
            }
            child = readNode(node->rec[i].link, db);
        }
        int result = put(child, key, data, db);
        free(child);
        return result;
    } else {
        node->rec[node->n + 1].link = node->rec[node->n].link;
        for (int j = node->n - 1; j >= i; j--) {
            node->rec[j + 1] = node->rec[j];
        }
        node->rec[i].key = *key;
        node->rec[i].data = *data;
        node->n++;
        writeNode(node, db);
        return 0;
    }
}

int put_inter(struct DB *db, struct DBT *key, struct DBT *data)
{
    struct BtreeNode *root = db->root;
    if (root->n >= 2 * root->maxn - 1) {
        struct BtreeNode *oroot = root;
        root = newNode(db);
        db->root = root;
        root->islist = 0;
        root->n = 0;
        root->maxn = oroot->maxn;
        root->rec[0].link = oroot->offset;
        free(oroot);
        splitChild(root, 0, db);
    }

    struct Key skey;
    skey.size = key->size;
    memcpy(&skey.data, key->data, skey.size);
    struct Data sdata;
    sdata.size = data->size;
    memcpy(&sdata.data, data->data, sdata.size);
    return put(root, &skey, &sdata, db);
}

void findmax(struct BtreeNode *node, struct Key *key, struct Data *data, struct DB *db)
{
    if (node->islist) {
        *key = node->rec[node->n - 1].key;
        *data = node->rec[node->n - 1].data;
        return;
    } else {
        struct BtreeNode *ch = readNode(node->rec[node->n].link, db);
        findmax(ch, key, data, db);
        free(ch);
        return;
    }
}

void findmin(struct BtreeNode *node, struct Key *key, struct Data *data, struct DB *db)
{
    if (node->islist) {
        *key = node->rec[0].key;
        *data = node->rec[0].data;
        return;
    } else {
        struct BtreeNode *ch = readNode(node->rec[0].link, db);
        findmin(ch, key, data, db);
        free(ch);
        return;
    }
}

int del(struct BtreeNode *node, struct Key *key, struct DB *db)
{
    int i = 0;
    while ((i < node->n) && (keycmp(&(node->rec[i].key), key) < 0)) {
        i++;
    }
    if ((i < node->n) && (keycmp(&(node->rec[i].key), key) == 0)) {
        if (node->islist) {
            for (int j = i; j < node->n - 1; j++) {
                node->rec[j] = node->rec[j + 1];
                //Copying links is not required here
            }
            node->n--;
            writeNode(node, db);
            return 0;
        } else {
            struct BtreeNode *left = readNode(node->rec[i].link, db);
            if (left->n >= left->maxn) {
                findmax(left, &node->rec[i].key, &node->rec[i].data, db);
                del(left, &node->rec[i].key, db);
                writeNode(node, db);
                free(left);
                return 0;
            }
            struct BtreeNode *right = readNode(node->rec[i + 1].link, db);
            if (right->n >= right->maxn) {
                findmin(right, &node->rec[i].key, &node->rec[i].data, db);
                del(right, &node->rec[i].key, db);
                writeNode(node, db);
                free(left);
                free(right);
                return 0;
            }
            int s1 = left->n;
            left->rec[s1].key = node->rec[i].key;
            left->rec[s1].data = node->rec[i].data;
            for (int j = i; j < node->n - 1; j++) {
                node->rec[j].key = node->rec[j + 1].key;
                node->rec[j].data = node->rec[j + 1].data;
                node->rec[j + 1].link = node->rec[j + 2].link;
            }
            node->n--;
            for (int j = 0; j < right->n; j++) {
                left->rec[s1 + j + 1] = right->rec[j];
            }
            left->n = s1 + right->n + 1;
            left->rec[left->n].link = right->rec[right->n].link;
            freeNode(right, db);
            writeNode(node, db);
            int res = del(left, key, db);
            free(left);
            return res;
        }
    } else {
        struct BtreeNode *ch = readNode(node->rec[i].link, db);
        if (ch->n < ch->maxn) {
            struct BtreeNode *left = NULL, *right = NULL;
            if (i > 0) {
                left = readNode(node->rec[i - 1].link, db);
                if (left->n >= left->maxn) {
                    //Taking one child from the left neighbour
                    ch->rec[ch->n + 1].link = ch->rec[ch->n].link;
                    for (int j = ch->n - 1; j >= 0; j--) {
                        ch->rec[j + 1] = ch->rec[j];
                    }
                    ch->n++;
                    ch->rec[0].key = node->rec[i - 1].key;
                    ch->rec[0].data = node->rec[i - 1].data;
                    node->rec[i - 1].key = left->rec[left->n - 1].key;
                    node->rec[i - 1].data = left->rec[left->n - 1].data;
                    ch->rec[0].link = left->rec[left->n].link;
                    left->n--;
                    writeNode(left, db);
                    free(left);
                    writeNode(node, db);
                    int res = del(ch, key, db);
                    free(ch);
                    return res;
                }
            }
            if (i < node->n) {
                right = readNode(node->rec[i + 1].link, db);
                if (right->n >= right->maxn) {
                    //Taking one child from the right neighbour
                    ch->rec[ch->n].key = node->rec[i].key;
                    ch->rec[ch->n].data = node->rec[i].data;
                    ch->rec[ch->n + 1].link = right->rec[0].link;
                    ch->n++;
                    node->rec[i].key = right->rec[0].key;
                    node->rec[i].data = right->rec[0].data;

                    for (int j = 0; j < right->n - 1; j++) {
                        right->rec[j] = right->rec[j + 1];
                    }
                    right->rec[right->n - 1].link = right->rec[right->n].link;
                    right->n--;

                    free(left);
                    writeNode(right, db);
                    free(right);
                    writeNode(node, db);
                    int res = del(ch, key, db);
                    free(ch);
                    return res;
                }
            }
            if (i > 0) {
                //Merging with left neighbour
                int size = left->n;
                left->rec[size].key = node->rec[i - 1].key;
                left->rec[size].data = node->rec[i - 1].data;
                for (int j = 0; j < ch->n; j++) {
                    left->rec[size + j + 1] = ch->rec[j];
                }
                left->n = size + ch->n + 1;
                left->rec[left->n].link = ch->rec[ch->n].link;
                freeNode(ch, db);
                free(ch);
                for(int j = i - 1; j < node->n - 1; j++) {
                    node->rec[j].key = node->rec[j + 1].key;
                    node->rec[j].data = node->rec[j + 1].data;
                    node->rec[j + 1].link = node->rec[j + 2].link;
                }
                //node->link[node->n - 1] = node->link[node->n];
                node->n--;
                writeNode(node, db);
                free(right);
                writeNode(left, db);
                int res = del(left, key, db);
                free(left);
                return res;
            }
            if (i < node->n) {
                //Merging with right neighbour
                int size = ch->n;
                ch->rec[size].key = node->rec[i].key;
                ch->rec[size].data = node->rec[i].data;
                for (int j = 0; j < right->n; j++) {
                    ch->rec[size + j + 1] = right->rec[j];
                }
                ch->n = size + right->n + 1;
                ch->rec[ch->n].link = right->rec[right->n].link;
                freeNode(right, db);
                free(right);
                for(int j = i; j < node->n - 1; j++) {
                    node->rec[j].key = node->rec[j + 1].key;
                    node->rec[j].data = node->rec[j + 1].data;
                    node->rec[j + 1].link = node->rec[j + 2].link;
                }
                //node->link[node->n - 1] = node->link[node->n];
                node->n--;
                writeNode(node, db);
                free(left);
                writeNode(ch, db);
                int res = del(ch, key, db);
                free(ch);
                return res;
            }
            return -1;
        } else {
            if (!node->islist) {
                int res = del(ch, key, db);
                free(ch);
                return res;
            } else {
                return 0;
            }
        }
    }
}

void printTree(struct BtreeNode *node, struct DB *db, int k)
{
    struct BtreeNode *ch;
    for (int i = 0; i < node->n; i++) {
        if (!node->islist) {
            ch = readNode(node->rec[i].link, db);
            printTree(ch, db, k + 1);
            free(ch);
        }
        for(int j = 0; j < k; j++) printf("--");
        printf("{%s : %s}\n", node->rec[i].key.data, node->rec[i].data.data);
    }
    if (!node->islist) {
        ch = readNode(node->rec[node->n].link, db);
        printTree(ch, db, k + 1);
        free(ch);
    }
    return;
}

int del_inter(struct DB *db, struct DBT *key)
{
    struct Key skey;
    skey.size = key->size;
    memcpy(&skey.data, key->data, skey.size);
    int res = del(db->root, &skey, db);
    if (db->root->n == 0) {
        struct BtreeNode *temp = readNode(db->root->rec[0].link, db);
        freeNode(db->root, db);
        free(db->root);
        db->root = temp;
    }
    return res;
}
