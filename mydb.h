#define MAX_KEY_SIZE 20
#define MAX_DATA_SIZE 100

struct DBT {
     char *data;
     size_t size;
};

struct DBC {
	size_t db_size;
	size_t chunk_size;
};


struct Key {
    size_t size;
    char data[MAX_KEY_SIZE];
};

struct Data {
    size_t size;
    char data[MAX_DATA_SIZE];
};

struct Record {
    struct Key key;
    struct Data data;
    int link;
};

struct BtreeNode {
    int islist;
    int n, maxn;
    int offset;
    struct Record rec[];
    /*struct Key key[2 * LIST_SIZE];
    struct Data data[2 * LIST_SIZE];
    int link[2 * LIST_SIZE + 1];*/
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
    int froff, maxnodecnt, root_off;
    struct BtreeNode *root;
    char *reser;
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DB *dbcreate(const char *file, const struct DBC conf);
struct DB *dbopen  (const char *file); /* Metadata in file */

int close_inter (struct DB *db);
int del_inter (struct DB *db, struct DBT *key);
int get_inter (struct DB *db, struct DBT *key, struct DBT *data);
int put_inter (struct DB *db, struct DBT *key, struct DBT *data);

