// Copyright (C) 2013  gokabinet authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

#define MAX_RECORD_SIZE 1024
#define nil 0

struct strary
{
	char    **v;
	int64_t n;
};

typedef struct strary strary;

// strary_item returns the item at the given position.
char *strary_item(strary *s, int64_t position);
KCREC gokccurget(KCCUR *cur);
strary match_prefix(KCDB *db, char *prefix, size_t max);
strary match_regex(KCDB *db, char *regex, size_t max);
void free_strary(strary *s);
