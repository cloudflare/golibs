// Copyright (C) 2013  gokabinet authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

#define MAX_LUA_RESULT_SIZE 64
#define MAX_RECORD_SIZE 1024
#define nil 0

struct strary
{
	char    **v;
    size_t  *s;
	int64_t n;
};

typedef struct strary strary;

static char**
make_char_array(int size) {
    return calloc(sizeof(char*), size);
}

static void 
set_array_string(char **a, char *s, int n) {
    a[n] = s;
}

static void 
free_char_array(char **a, int size) {
    int i;
    for (i = 0; i < size; i++)
        free(a[i]);
    free(a);
}

char *strary_item(strary *s, int64_t position);
size_t strary_size(strary *s, int64_t position);
strary match_prefix(KTRDB *db, char *prefix, size_t max);
strary get_bulk_binary(KTRDB *db, const char **keys, size_t nkeys);
strary play_script(KTRDB *db, const char *script, const char **params, size_t nparams);
void free_strary(strary *s);

