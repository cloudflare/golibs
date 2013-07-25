// Copyright (C) 2013  gokabinet authors.
// Use of this source code is governed by a GPLv3
// license that can be found in the LICENSE file.

#include <ktlangc.h>
#include "kt.h"

void
_alloc(char ***v, size_t n)
{
	int i;
	*v = (char **)malloc(n * sizeof(char *));
	for(i = 0; i < n; ++i) {
		(*v)[i] = (char *)malloc(MAX_RECORD_SIZE * sizeof(char));
	}
}

void
_free(char ***v, size_t n)
{
	int i;
	for(i = 0; i < n; ++i) {
		free((*v)[i]);
	}
	free(*v);
}

strary
_match(KTRDB *db, char *match, size_t max, int64_t (*mfunc)(KTRDB *, const char *, char **, size_t))
{
    int i;
    int64_t n;
    strary s;
    _alloc(&s.v, max);
    s.s = (size_t*)malloc(sizeof(size_t)*1);
    n = mfunc(db, match, s.v, max);
    if(n == -1) {
        s.n = max;
        return s;
    }
    s.n = n;
    if(n < max) {
        for(i = n; i < max; ++i) {
            free(s.v[i]);
        }
        s.v = (char **)realloc(s.v, s.n * sizeof(char *));
    }
    return s;
}

strary
match_prefix(KTRDB *db, char *prefix, size_t max)
{
	return _match(db, prefix, max, ktdbmatchprefix);
}

strary 
get_bulk_binary(KTRDB *db, const char **keys, size_t nkeys) {
	int i;
	int64_t n;
	strary s;
	_alloc(&s.v, nkeys);
    s.s = (size_t*)malloc(sizeof(size_t)*nkeys);
    n = ktdbgetbulkbinary(db, keys, nkeys, s.v, s.s);
	if(n == -1) {
        s.n = nkeys;
		return s;
	}
	s.n = n;
	if(n < nkeys) {
		for(i = n; i < nkeys; ++i) {
			free(s.v[i]);
		}
		s.v = (char **)realloc(s.v, s.n * sizeof(char *));
	}
	return s;
}

strary 
play_script(KTRDB *db, const char *script, const char **params, size_t nparams) {
	int i;
	int64_t n;
	strary s;
	_alloc(&s.v, MAX_LUA_RESULT_SIZE);
	n = ktdbplayscript(db, script, params, nparams, s.v);
	if(n == -1) {
        s.n = MAX_LUA_RESULT_SIZE;
		return s;
	}
	s.n = n;
	if(n < MAX_LUA_RESULT_SIZE) {
		for(i = n; i < MAX_LUA_RESULT_SIZE; ++i) {
			free(s.v[i]);
		}
		s.v = (char **)realloc(s.v, s.n * sizeof(char *));
	}
	return s;
}

char *
strary_item(strary *s, int64_t position)
{
	if (position < s->n) {
		return s->v[position];
	}
	return nil;
}

size_t
strary_size(strary *s, int64_t position)
{
	if (position < s->n) {
		return s->s[position];
	}
	return nil;
}

bool
strary_present(strary *s, int64_t position)
{
	if (position < s->n) {
		return s->s[position] != -1;
	}
	return false;
}

void
free_strary(strary *s)
{
	_free(&s->v, s->n);
    free(s->s);
}
