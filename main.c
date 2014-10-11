#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#define MAX_KEY_SIZE 30
#define MAX_DATA_SIZE 100
#define PAGE_SIZE (1024 * 64)
#define NODE_SIZE ((PAGE_SIZE - 30) / (2 * (MAX_KEY_SIZE + 16)))
//#define LIST_SIZE ((PAGE_SIZE - 30) / (2 * (MAX_DATA_SIZE + MAX_KEY_SIZE + 16)))
#define LIST_SIZE 2
#define min(a, b) ((a) < (b) ? (a) : (b))

/* check `man dbopen` */
struct DBT {
     void  *data;
     size_t size;
};

struct DBC {
        /* Maximum on-disk file size */
        /* 512MB by default */
        size_t db_size;
        /* Maximum page (node/data chunk) size */
        /* 4KB by default */
        size_t page_size;
        /* Maximum memory size */
        /* 16MB by default */
        size_t mem_size;
};

struct Key {
    size_t size;
    char data[MAX_KEY_SIZE];
};

struct Data {
    size_t size;
    char data[MAX_DATA_SIZE];
};

struct BtreeNode {
    char islist;
    int n, maxn;
    int offset;
    struct Key key[2 * LIST_SIZE];
    struct Data data[2 * LIST_SIZE];
    int link[2 * LIST_SIZE + 1];
};

struct DB {
    /* Public API */
    int (*close)(struct DB *db);
    int (*del)(struct DB *db, const struct DBT *key);
    int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db, struct DBT *key, const struct DBT *data);
    //int (*sync)(const struct DB *db);
    /* Private API */
    struct DBC config;

    FILE *dbfl;
    int froff;
    struct BtreeNode *root;
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DB *dbcreate(const char *file, const struct DBC conf);
struct DB *dbopen  (const char *file); /* Metadata in file */



struct BtreeNode *newNode(struct DB *db);

struct DB *dbcreate(const char *file, const struct DBC conf)
{
    struct DB *db = malloc(sizeof(*db));
    db->config = conf;
    db->dbfl = fopen(file, "w+");
    //posix_fallocate(fileno(db->dbfl), 0, conf.db_size);

    db->froff = 0;
    db->root = newNode(db);
    db->root->n = 0;
    db->root->maxn = LIST_SIZE;
    db->root->islist = 1;
    return db;
}


int keycmp(struct Key *a, struct Key *b) { //returns -1 if less, 0 if equal, 1 if more
    int res = memcmp(&(a->data), &(b->data), min(a->size, b->size));
    return res;
}

struct BtreeNode *readNode(int offset, struct DB *db) {
    struct BtreeNode *res = calloc(1, PAGE_SIZE);
    fseek(db->dbfl, offset * PAGE_SIZE, SEEK_SET);
    fread(res, PAGE_SIZE, 1, db->dbfl);
    return res;
}

void writeNode(struct BtreeNode *node, struct DB *db) {
    fseek(db->dbfl, node->offset * PAGE_SIZE, SEEK_SET);
    fwrite(node, PAGE_SIZE, 1, db->dbfl);
    return;
}

struct BtreeNode *newNode(struct DB *db) {
    struct BtreeNode *res = calloc(1, PAGE_SIZE);
    res->offset = db->froff;
    db->froff++;
    return res;
}

int splitChild(struct BtreeNode *node, int pos, struct DB *db) {
    struct BtreeNode *nch = newNode(db);
    struct BtreeNode *och = readNode(node->link[pos], db);
    nch->maxn = och->maxn;
    nch->islist = och->islist;
    int size = och->maxn;
    nch->n = och->n - size;
    //och->n = size - 1;
    for (int i = 0; i < nch->n; i++) {
        nch->key[i] = och->key[i + size];
        nch->data[i] = och->data[i + size];
        nch->link[i] = nch->link[i + size];
    }
    nch->link[nch->n] = och->link[och->n];
    node->link[node->n + 1] = node->link[node->n];
    for (int i = node->n; i > pos; i--) {
        node->key[i] = node->key[i - 1];
        node->data[i] = node->data[i - 1];
        node->link[i] = node->link[i - 1];
    }
    node->link[pos] = och->offset;
    node->link[pos + 1] = nch->offset;
    node->key[pos] = och->key[size - 1];
    node->data[pos] = och->data[size - 1];
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
    while ((i < node->n) && (keycmp(&(node->key[i]), key) < 0)) {
        i++;
    }
    if ((i < node->n) && (keycmp(&(node->key[i]), key) == 0)) {
        *data = node->data[i];
        return 0;
    } else if (!node->islist) {
        struct BtreeNode *child = readNode(node->link[i], db);
        int result = get(child, key, data, db);
        free(child);
        return result;
    } else {
        return -1;
    }
}

int put(struct BtreeNode *node, struct Key *key, struct Data *data, struct DB *db)
{
    int i = 0;
    while ((i < node->n) && (keycmp(&(node->key[i]), key) < 0)) {
        i++;
    }
    if (!node->islist) {
        struct BtreeNode *child = readNode(node->link[i], db);
        if (child->n == 2 * child->maxn - 1) {
            free(child);
            if (splitChild(node, i, db) < 0) {
                return -1;
            }
            if (keycmp(&(node->key[i]), key) < 0) {
                i++;
            }
            child = readNode(node->link[i], db);
        }
        int result = put(child, key, data, db);
        free(child);
        return result;
    } else {
        node->link[node->n + 1] = node->link[node->n];
        for (int j = node->n - 1; j >= i; j--) {
            node->key[j + 1] = node->key[j];
            node->data[j + 1] = node->data[j];
            node->link[j + 1] = node->link[j];
        }
        node->key[i] = *key;
        node->data[i] = *data;
        node->n++;
        writeNode(node, db);
        return 0;
    }
}

int put_inter(struct Key *key, struct Data *data, struct DB *db) {
    struct BtreeNode *root = db->root;
    if (root->n >= 2 * root->maxn - 1) {
        struct BtreeNode *oroot = root;
        root = newNode(db);
        db->root = root;
        root->islist = 0;
        root->n = 0;
        root->maxn = oroot->maxn;
        root->link[0] = oroot->offset;
        free(oroot);
        splitChild(root, 0, db);
    }

    return put(root, key, data, db);
    //free(root);
}

void stk(struct Key *a, char *b)
{
    a->size = strlen(b);
    strcpy(a->data, b);
    return;
}

void findmax(struct BtreeNode *node, struct Key *key, struct Data *data, struct DB *db) {
    if (node->islist) {
        *key = node->key[node->n - 1];
        *data = node->data[node->n - 1];
        node->n--;
        writeNode(node, db);
        return;
    } else {
        struct BtreeNode *ch = readNode(node->link[node->n], db);
        findmax(ch, key, data, db);
        free(ch);
        return;
    }
}

void findmin(struct BtreeNode *node, struct Key *key, struct Data *data, struct DB *db) {
    if (node->islist) {
        *key = node->key[0];
        *data = node->data[0];
        for (int i = 0; i < node->n - 1)
        node->n--;
        writeNode(node, db);
        return;
    } else {
        struct BtreeNode *ch = readNode(node->link[0], db);
        findmin(ch, key, data, db);
        free(ch);
        return;
    }
}

int del(struct BtreeNode *node, struct Key *key, struct DB *db)
{
    int i = 0;
    while ((i < node->n) && (keycmp(&(node->key[i]), key) < 0)) {
        i++;
    }
    if (keycmp(&(node->key[i]), key) == 0) {
        if (node->islist) {
            for (int j = i; j < node->n; j++) {
                node->key[j] = node->key[j + 1];
                node->data[j] = node->data[j + 1];
            }
            node->n--;
            return 0;
        } else {
            struct BtreeNode *left = readNode(node->link[i], db);
            if (left->n >= left->maxn) {
                findmax(left, &node->key[i], &node->data[i], db);
                free(left);
                return 0;
            }
            struct BtreeNode *right = readNode(node->link[i + 1], db);
            if (right->n >= right->maxn) {

            }
        }
    }
}

int main()
{
    struct DBC sett;
    sett.db_size = 400096;
    struct DB *mydb = dbcreate("db.txt", sett);

    int size = 300, k = 8;
    struct Key mykey[size];
    struct Data mydata[size];
    //memset(mydata, 0, sizeof(mydata));
    //memset(mykey, 0, sizeof(mykey));

    stk(&mykey[0], "rtyhv");
    stk(&mydata[0], "My Little Pony");
    stk(&mykey[1], "ftgvbjnhugf5");
    stk(&mydata[1], "Fire brigade");
    stk(&mykey[2], "8bvf67un  d");
    stk(&mydata[2], "Trick and Treat");
    stk(&mykey[3], "h v567tgv ");
    stk(&mydata[3], "\\(``)/");
    stk(&mykey[4], " b y677ft");
    stk(&mydata[4], "For the glory");
    stk(&mykey[5], "jbuvgyc6578ui");
    stk(&mydata[5], "Friendship is magic");
    stk(&mykey[6], " hug767gyb");
    stk(&mydata[6], "Monty Python");
    stk(&mykey[7], "9087tyvgh b");
    stk(&mydata[7], "Griffindor");

    for (int i = 0; i < k; i++)
        put_inter(&mykey[i], &mydata[i], mydb);
    for (int i = 0; i < k; i++) {
        get(mydb->root, &mykey[i], &mydata[0], mydb);
        puts(mydata[0].data);
        //printf("%d %d%d%d%d%\n", mydata[0].size, mydata[0].data[0], mydata[0].data[1], mydata[0].data[2], mydata[0].data[3]);
    }

    printf("%d %d\n", (int) sizeof(struct BtreeNode), LIST_SIZE);
    //scanf("\n");
    return 0;
}
