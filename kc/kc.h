/*
 * This file depends on kclangc.h
 *
 * Make sure you include it in your code :)
 */

typedef struct
{
	char *key;
	const char *value;
} _pair;

void free_pair(_pair p);
_pair gokccurget(KCCUR *cur);
