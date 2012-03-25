// Copyright 2012 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include <kclangc.h>
#include "kc.h"

void
free_pair(_pair p)
{
	if (p.key != NULL) {
		free(p.key);
		p.key = NULL;
	}
}

_pair
gokccurget(KCCUR *cur)
{
	_pair p;
	size_t ksiz, vsiz;
	const char *argvbuf;
	p.key = kccurget(cur, &ksiz, &argvbuf, &vsiz, 1);
	p.value = argvbuf;
	return p;
}
