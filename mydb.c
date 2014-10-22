#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <math.h>

#include "mydb.h"

#define min(a, b) ((a) < (b) ? (a) : (b))
#define max(a, b) ((a) > (b) ? (a) : (b))


int db_close(struct DB *db) {
	db->close(db);
}

int db_del(struct DB *db, void *key, size_t key_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	return db->del(db, &keyt);
}

int db_get(struct DB *db, void *key, size_t key_len,
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

int db_put(struct DB *db, void *key, size_t key_len,
	   void *val, size_t val_len) {
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
    struct DB *db = malloc(sizeof(*db));
    db->config = conf;
    db->dbfl = fopen(file, "w+");
    db->maxnodecnt = conf.db_size / conf.chunk_size;
    db->get = get_inter;
    db->put = put_inter;
    db->del = del_inter;
    db->close = close_inter;
    db->froff = ceil(db->maxnodecnt * 1.0 / conf.chunk_size) + 1;
    db->reser = calloc(db->maxnodecnt, 1);
    for (int i = 0; i < db->froff; i++) {
        reser_set(1, i, db);
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

struct BtreeNode *readNode(int offset, struct DB *db) {
    struct BtreeNode *res = calloc(1, db->config.chunk_size);
    fseek(db->dbfl, offset * db->config.chunk_size, SEEK_SET);
    fread(res, db->config.chunk_size, 1, db->dbfl);
    return res;
}

void writeNode(struct BtreeNode *node, struct DB *db) {
    fseek(db->dbfl, node->offset * db->config.chunk_size, SEEK_SET);
    fwrite(node, db->config.chunk_size - 1, 1, db->dbfl);
    return;
}

struct BtreeNode *newNode(struct DB *db) {
    struct BtreeNode *res = calloc(1, db->config.chunk_size);
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
    fseek(db->dbfl, 0, SEEK_SET);
    fwrite(db, sizeof(*db), 1, db->dbfl);
    fseek(db->dbfl, db->config.chunk_size, SEEK_SET);
    fwrite(db->reser, 1, db->maxnodecnt, db->dbfl);
    free(db->reser);
    free(db->root);
    fclose(db->dbfl);
    free(db);
    //printf("Successfully closed\n");
    return 0;
}

struct DB *dbopen (const char *file)
{
    struct DB *db = malloc(sizeof(*db));
    FILE *f = fopen(file, "r+");
    fseek(f, 0, SEEK_SET);
    fread(db, sizeof(*db), 1, f);

    db->dbfl = f;
    db->get = get_inter;
    db->put = put_inter;
    db->del = del_inter;
    db->close = close_inter;

    db->reser = calloc(db->maxnodecnt, 1);
    fseek(db->dbfl, db->config.chunk_size, SEEK_SET);
    fread(db->reser, 1, db->maxnodecnt, db->dbfl);
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
    data->data = calloc(sdata.size, 1);
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
