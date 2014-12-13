
void stk(struct Key *a, char *b)
{
    a->size = strlen((const char *)b) + 1;
    strcpy(a->data, (const char *)b);
    return;
}

void stk(struct Data *a, char *b)
{
    a->size = strlen((const char *)b) + 1;
    strcpy(a->data, (const char *)b);
    return;
}

void init(struct Key *mykey, struct Data *mydata, struct DBT *key, struct DBT *data, int k) {
    stk(&mykey[0], "qwer");
    stk(&mydata[0], "My Little Pony");
    stk(&mykey[1], "iuhiugu");
    stk(&mydata[1], "Fire brigade");
    stk(&mykey[2], "2hgvcty");
    stk(&mydata[2], "Trick and Treat");
    stk(&mykey[3], "56fv");
    stk(&mydata[3], "\\(``)/");
    stk(&mykey[4], "bgyugy d");
    stk(&mydata[4], "For the glory");
    stk(&mykey[5], "2 ghvvtyft");
    stk(&mydata[5], "Friendship is magic");
    stk(&mykey[6], "bhgvyt");
    stk(&mydata[6], "Monty Python");
    stk(&mykey[7], "moinu=-nkj");
    stk(&mydata[7], "Griffindor");
    stk(&mykey[8], " hugv6t7guy");
    stk(&mydata[8], "Conquest");
    for (int i = 0; i < k; i++) {
        key[i].size = mykey[i].size;
        key[i].data = (char *)&mykey[i].data;
        data[i].size = mydata[i].size;
        data[i].data = (char *)&mydata[i].data;

    }
    return;
}

int main()
{
    struct DBC sett;
    sett.db_size = 400096;
    sett.chunk_size = 4096;
    struct DB *mydb = dbcreate("db.txt", sett);

    int size = 300, k = 9;
    struct Key mykey[size];
    struct Data mydata[size];
    struct DBT key[size], data[size];

    init((Key *)&mykey, (Data *)&mydata, (DBT *)&key, (DBT *)&data, k);

    for (int i = 0; i < k; i++)
        mydb->put(mydb, &key[i], &data[i]);
    printTree(mydb->root, mydb, 1);

    mydb->close(mydb);
    mydb = dbopen("db.txt");

    for (int i = 0; i < k - 4; i++) {
        printf("%d\n", mydb->del(mydb, &key[i]));
        printf("\n\n\n\n");
        printTree(mydb->root, mydb, 1);
    }

    printf("\n\n\n\n");
    int res;
    for (int i = 0; i < k; i++) {
        printf("%d\n", res = mydb->get(mydb, &key[i], &data[0]));
        if (res >= 0) puts(data[0].data);//printf("%s\n", data[0].data);
        //printf("%d %d%d%d%d%\n", mydata[0].size, mydata[0].data[0], mydata[0].data[1], mydata[0].data[2], mydata[0].data[3]);
    }

    //scanf("\n");
    return 0;
}

