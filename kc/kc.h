// Copyright 2013 gokabinet authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#define MAX_RECORD_SIZE 1024
#define nil 0

struct strary
{
	char    **v;
	int64_t n;
};

// strary_item returns the item at the given position.
char *strary_item(struct strary *s, int64_t position);
KCREC gokccurget(KCCUR *cur);
struct strary match_prefix(KCDB *db, char *prefix, size_t max);
void free_strary(struct strary *s);
