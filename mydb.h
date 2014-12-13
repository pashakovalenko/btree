#define MAX_KEY_SIZE 40
#define MAX_DATA_SIZE 100
#include <map>
#include <list>
#include <string>

struct DBC {
	size_t db_size;
	size_t chunk_size;
	size_t mem_size;
};

struct DBT {
     char *data;
     size_t size;
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
    int lol;
    int islist;
    int n, maxn;
    int offset;
    struct Record rec[];
};

struct DB {
    /* Public API */
	/* Returns 0 on OK, -1 on Error */
	int (*close)(struct DB *db);
	int (*del)(struct DB *db, struct DBT *key);
	int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
	int (*put)(struct DB *db, struct DBT *key, struct DBT *data);
	/* For future uses - sync cached pages with disk*/
    //int (*sync)(const struct DB *db);

	/* Private API */
	struct DBC config;

    int dbfl;
    int froff, maxnodecnt, root_off;
    struct BtreeNode *root;
    char *reser;
    int chsize;
    char *cache;
    std::map <int, int> *chmap;
    std::list <int> *lru;
};
extern "C" {
struct DB *dbcreate(const char *file, const struct DBC conf);
struct DB *dbopen  (const char *file); /* Metadata in file */
int db_close(struct DB *db);
int db_del(struct DB *db, char *key, size_t key_len);
int db_get(struct DB *db, char *key, size_t key_len,
	   void **val, size_t *val_len);
int db_put(struct DB *db, char *key, size_t key_len,
	   char *val, size_t val_len);
int db_flush(struct DB *db);
}

int close_inter (struct DB *db);
int del_inter (struct DB *db, struct DBT *key);
int get_inter (struct DB *db, struct DBT *key, struct DBT *data);
int put_inter (struct DB *db, struct DBT *key, struct DBT *data);

