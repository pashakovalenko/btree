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
     char *data;
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
    int islist;
    int n, maxn;
    int offset;
    struct Key key[2 * LIST_SIZE];
    struct Data data[2 * LIST_SIZE];
    int link[2 * LIST_SIZE + 1];
};

struct DB {
    /* Public API */
	/* Returns 0 on OK, -1 on Error */
	int (*close)(struct DB *db);
	int (*del)(struct DB *db, struct DBT *key);
	/* * * * * * * * * * * * * *
	 * Returns malloc'ed data into 'struct DBT *data'.
	 * Caller must free data->data. 'struct DBT *data' must be alloced in
	 * caller.
	 * * * * * * * * * * * * * */
	int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
	int (*put)(struct DB *db, struct DBT *key, struct DBT *data);
	/* For future uses - sync cached pages with disk
	 * int (*sync)(const struct DB *db)
	 * */
	/* Private API */
	/*     ...     */
    struct DBC config;

    FILE *dbfl;
    int froff;
    struct BtreeNode *root;
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DB *dbcreate(const char *file, const struct DBC conf);
struct DB *dbopen  (const char *file); /* Metadata in file */

int del_inter(struct DB *db, struct DBT *key);
int get_inter (struct DB *db, struct DBT *key, struct DBT *data);
int put_inter (struct DB *db, struct DBT *key, struct DBT *data);



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
    db->get = get_inter;
    db->put = put_inter;
    db->del = del_inter;
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
    fwrite(node, PAGE_SIZE - 1, 1, db->dbfl);
    return;
}

struct BtreeNode *newNode(struct DB *db) {
    struct BtreeNode *res = calloc(1, PAGE_SIZE);
    res->offset = db->froff;
    db->froff++;
    return res;
}

void freeNode(struct BtreeNode *node, struct DB *db) {
    return;
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
        nch->link[i] = och->link[i + size];
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

int get_inter(struct DB *db, struct DBT *key, struct DBT *data)
{
    struct Key skey;
    skey.size = key->size;
    memcpy(&skey.data, key->data, skey.size);
    struct Data sdata;
    int res = get(db->root, &skey, &sdata, db);
    if (res < 0)
        return res;
    data->size = sdata.size;
    data->data = calloc(sdata.size, 1);
    memcpy(data->data, &sdata.data, sdata.size);
    return res;
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
        root->link[0] = oroot->offset;
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
    //free(root);
}

void findmax(struct BtreeNode *node, struct Key *key, struct Data *data, struct DB *db)
{
    if (node->islist) {
        *key = node->key[node->n - 1];
        *data = node->data[node->n - 1];
        return;
    } else {
        struct BtreeNode *ch = readNode(node->link[node->n], db);
        findmax(ch, key, data, db);
        free(ch);
        return;
    }
}

void findmin(struct BtreeNode *node, struct Key *key, struct Data *data, struct DB *db)
{
    if (node->islist) {
        *key = node->key[0];
        *data = node->data[0];
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
    /*printf("\n\n\n");
    printTree(db->root, db, 2);*/
    int i = 0;
    while ((i < node->n) && (keycmp(&(node->key[i]), key) < 0)) {
        i++;
    }
    if ((i < node->n) && (keycmp(&(node->key[i]), key) == 0)) {
        if (node->islist) {
            for (int j = i; j < node->n - 1; j++) {
                node->key[j] = node->key[j + 1];
                node->data[j] = node->data[j + 1];
            }
            node->n--;
            writeNode(node, db);
            return 0;
        } else {
            struct BtreeNode *left = readNode(node->link[i], db);
            if (left->n >= left->maxn) {
                findmax(left, &node->key[i], &node->data[i], db);
                del(left, &node->key[i], db);
                writeNode(node, db);
                free(left);
                return 0;
            }
            struct BtreeNode *right = readNode(node->link[i + 1], db);
            if (right->n >= right->maxn) {
                findmin(right, &node->key[i], &node->data[i], db);
                del(right, &node->key[i], db);
                writeNode(node, db);
                free(left);
                free(right);
                return 0;
            }
            int s1 = left->n;
            left->key[s1] = node->key[i];
            left->data[s1] = node->data[i];
            for (int j = i; j < node->n - 1; j++) {
                node->key[j] = node->key[j + 1];
                node->data[j] = node->data[j + 1];
                node->link[j + 1] = node->link[j + 2];
            }
            node->n--;
            for (int j = 0; j < right->n; j++) {
                left->key[s1 + j + 1] = right->key[j];
                left->data[s1 + j + 1] = right->data[j];
                left->link[s1 + j + 1] = right->link[j];
            }
            left->n = s1 + right->n + 1;
            left->link[left->n] = right->link[right->n];
            freeNode(right, db);
            writeNode(node, db);
            int res = del(left, key, db);
            free(left);
            return res;
        }
    } else {
        struct BtreeNode *ch = readNode(node->link[i], db);
        if (ch->n < ch->maxn) {
            struct BtreeNode *left, *right;
            if (i > 0) {
                left = readNode(node->link[i - 1], db);
                if (left->n >= left->maxn) {
                    //Taking one child from the left neighbour
                    ch->link[ch->n + 1] = ch->link[ch->n];
                    for (int j = ch->n - 1; j >= 0; j--) {
                        ch->key[j + 1] = ch->key[j];
                        ch->data[j + 1] = ch->data[j];
                        ch->link[j + 1] = ch->link[j];
                    }
                    ch->n++;
                    ch->key[0] = node->key[i - 1];
                    ch->data[0] = node->data[i - 1];
                    node->key[i - 1] = left->key[left->n - 1];
                    node->data[i - 1] = left->data[left->n - 1];
                    ch->link[0] = left->link[left->n];
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
                right = readNode(node->link[i + 1], db);
                if (right->n >= right->maxn) {
                    //Taking one child from the right neighbour
                    ch->key[ch->n] = node->key[i];
                    ch->data[ch->n] = node->data[i];
                    ch->link[ch->n + 1] = right->link[0];
                    ch->n++;
                    node->key[i] = right->key[0];
                    node->data[i] = right->data[0];

                    for (int j = 0; j < right->n - 1; j++) {
                        right->key[j] = right->key[j + 1];
                        right->data[j] = right->data[j + 1];
                        right->link[j] = right->link[j + 1];
                    }
                    right->link[right->n - 1] = right->link[right->n];
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
                left->key[size] = node->key[i - 1];
                left->data[size] = node->data[i - 1];
                for (int j = 0; j < ch->n; j++) {
                    left->key[size + j + 1] = ch->key[j];
                    left->data[size + j + 1] = ch->data[j];
                    left->link[size + j + 1] = ch->link[j];
                }
                left->n = size + ch->n + 1;
                left->link[left->n] = ch->link[ch->n];
                freeNode(ch, db);
                free(ch);
                for(int j = i - 1; j < node->n - 1; j++) {
                    node->key[j] = node->key[j + 1];
                    node->data[j] = node->data[j + 1];
                    node->link[j + 1] = node->link[j + 2];
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
                ch->key[size] = node->key[i];
                ch->data[size] = node->data[i];
                for (int j = 0; j < right->n; j++) {
                    ch->key[size + j + 1] = right->key[j];
                    ch->data[size + j + 1] = right->data[j];
                    ch->link[size + j + 1] = right->link[j];
                }
                ch->n = size + right->n + 1;
                ch->link[ch->n] = right->link[right->n];
                freeNode(right, db);
                free(right);
                for(int j = i; j < node->n - 1; j++) {
                    node->key[j] = node->key[j + 1];
                    node->data[j] = node->data[j + 1];
                    node->link[j + 1] = node->link[j + 2];
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
                return -1;
            }
        }
    }
}

void printTree(struct BtreeNode *node, struct DB *db, int k)
{
    struct BtreeNode *ch;
    for (int i = 0; i < node->n; i++) {
        if (!node->islist) {
            ch = readNode(node->link[i], db);
            printTree(ch, db, k + 1);
            free(ch);
        }
        for(int j = 0; j < k; j++) printf("--");
        printf("{%s : %s}\n", node->key[i].data, node->data[i].data);
    }
    if (!node->islist) {
        ch = readNode(node->link[node->n], db);
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
        struct BtreeNode *temp = readNode(db->root->link[0], db);
        freeNode(db->root, db);
        free(db->root);
        db->root = temp;
    }
    return res;
}

void stk(struct Key *a, char *b)
{
    a->size = strlen(b) + 1;
    strcpy(a->data, b);
    return;
}

void init(struct Key *mykey, struct Data *mydata, struct DBT *key, struct DBT *data, int k) {
    stk(&mykey[0], "0");
    stk(&mydata[0], "My Little Pony");
    stk(&mykey[1], "1");
    stk(&mydata[1], "Fire brigade");
    stk(&mykey[2], "20");
    stk(&mydata[2], "Trick and Treat");
    stk(&mykey[3], "3");
    stk(&mydata[3], "\\(``)/");
    stk(&mykey[4], "4");
    stk(&mydata[4], "For the glory");
    stk(&mykey[5], "5");
    stk(&mydata[5], "Friendship is magic");
    stk(&mykey[6], "6");
    stk(&mydata[6], "Monty Python");
    stk(&mykey[7], "7");
    stk(&mydata[7], "Griffindor");
    stk(&mykey[8], "8");
    stk(&mydata[8], "Conquest");
    for (int i = 0; i < k; i++) {
        key[i].size = mykey[i].size;
        key[i].data = &mykey[i].data;
        data[i].size = mydata[i].size;
        data[i].data = &mydata[i].data;

    }
    return;
}

int main()
{
    struct DBC sett;
    sett.db_size = 400096;
    struct DB *mydb = dbcreate("db.txt", sett);

    int size = 300, k = 9;
    struct Key mykey[size];
    struct Data mydata[size];
    struct DBT key[size], data[size];
    //memset(mydata, 0, sizeof(mydata));
    //memset(mykey, 0, sizeof(mykey));

    init(&mykey, &mydata, &key, &data, k);

    for (int i = 0; i < k; i++)
        //put_inter(&mykey[i], &mydata[i], mydb);
        mydb->put(mydb, &key[i], &data[i]);
    printTree(mydb->root, mydb, 1);

    for (int i = 0; i < k - 4; i++) {
        printf("%d\n", mydb->del(mydb, &key[i]));
        printf("\n\n\n\n");
        printTree(mydb->root, mydb, 1);
    }

    printf("\n\n\n\n");
    int res;
    for (int i = 0; i < k; i++) {
        printf("%d\n", res = mydb->get(mydb, &key[i], &data[0]));
        if (res >= 0) puts(data[0].data);
        //printf("%d %d%d%d%d%\n", mydata[0].size, mydata[0].data[0], mydata[0].data[1], mydata[0].data[2], mydata[0].data[3]);
    }


    /*
    printf("%d\n", del(mydb->root, &mykey[2], mydb));
    printf("\n\n\n\n");
    printTree(mydb->root, mydb, 1);
    */


    //scanf("\n");
    return 0;
}
