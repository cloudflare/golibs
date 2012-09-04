// Copyright 2012 gokabinet authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include <kclangc.h>
#include "kc.h"

KCREC
gokccurget(KCCUR *cur)
{
	KCREC p;
	const char *argvbuf;
	p.key.buf = kccurget(cur, &p.key.size, &argvbuf, &p.value.size, 1);
	p.value.buf = (char *)argvbuf;
	return p;
}
