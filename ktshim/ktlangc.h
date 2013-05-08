/*************************************************************************************************
 * C language binding
 *************************************************************************************************/


#ifndef _KTLANGC_H                       /* duplication check */
#define _KTLANGC_H

#if defined(__cplusplus)
extern "C" {
#endif

#if !defined(__STDC_LIMIT_MACROS)
#define __STDC_LIMIT_MACROS  1           /**< enable limit macros for C++ */
#endif

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <float.h>
#include <limits.h>
#include <locale.h>
#include <math.h>
#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <stdint.h>

/**
 * C wrapper of remote database.
 */
    typedef struct {
    void* db;                              /**< dummy member */
} KTRDB;
    
    KTRDB* ktdbnew(void);

    void ktdbdel(KTRDB* db);
    
    int32_t ktdbopen(KTRDB* db, const char* host, int32_t port, double timeout);
    
    int32_t ktdbclose(KTRDB* db);

    int32_t ktdbset(KTRDB* db, const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz);

    int32_t ktdbadd(KTRDB* db, const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz);

    int32_t ktdbreplace(KTRDB* db, const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz);

    int32_t ktdbappend(KTRDB* db, const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz);
    
    char* ktdbget(KTRDB* db, const char* kbuf, size_t ksiz, size_t* sp);
    
    int64_t ktdbmatchprefix(KTRDB* db, const char* prefix, char** strary, size_t max);
    
    int64_t ktdbgetbulkbinary(KTRDB* db, const char** keys, size_t ksiz, char** strary);
    
    int32_t ktdbecode(KTRDB* db);

    const char* ktecodename(int32_t code);

    int64_t ktdbcount(KTRDB* db);
    
#if defined(__cplusplus)
}
#endif

#endif      

