
#include "ktremotedb.h"
#include "ktlangc.h"

using namespace kyototycoon;

extern "C" {

    KTRDB* ktdbnew(void) {
        _assert_(true);
        return (KTRDB*)new RemoteDB;
    }
    
    void ktdbdel(KTRDB* db) {
        _assert_(db);
        RemoteDB* pdb = (RemoteDB*)db;
        if (pdb != NULL) {
            delete pdb;
        }
    }

    /**
     * Open a database file.
     */
    int32_t ktdbopen(KTRDB* db, const char* host, int32_t port, double timeout) {
        _assert_(db && path);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->open(host, port, timeout);
    }
    
    /**
     * Close the database file.
     */
    int32_t ktdbclose(KTRDB* db) {
        _assert_(db);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->close();
    }

    int32_t ktdbset(KTRDB* db, const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) {
        _assert_(db && kbuf && ksiz <= MEMMAXSIZ && vbuf && vsiz <= MEMMAXSIZ);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->set(kbuf, ksiz, vbuf, vsiz);
    }

    int32_t ktdbadd(KTRDB* db, const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) {
        _assert_(db && kbuf && ksiz <= MEMMAXSIZ && vbuf && vsiz <= MEMMAXSIZ);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->add(kbuf, ksiz, vbuf, vsiz);
    }

    /**
     * Replace the value of a record.
     */
    int32_t ktdbreplace(KTRDB* db, const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) {
        _assert_(db && kbuf && ksiz <= MEMMAXSIZ && vbuf && vsiz <= MEMMAXSIZ);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->replace(kbuf, ksiz, vbuf, vsiz);
    }
    
    /**
     * Append the value of a record.
     */
    int32_t ktdbappend(KTRDB* db, const char* kbuf, size_t ksiz, const char* vbuf, size_t vsiz) {
        _assert_(db && kbuf && ksiz <= MEMMAXSIZ && vbuf && vsiz <= MEMMAXSIZ);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->append(kbuf, ksiz, vbuf, vsiz);
    }
    
    /**
     * Add a number to the numeric value of a record.
     */
    int64_t ktdbincrint(KTRDB* db, const char* kbuf, size_t ksiz, int64_t num, int64_t orig) {
        _assert_(db && kbuf && ksiz <= MEMMAXSIZ);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->increment(kbuf, ksiz, num, orig);
    }
    
    char* ktdbget(KTRDB* db, const char* kbuf, size_t ksiz, size_t* sp) {
        _assert_(db && kbuf && ksiz <= MEMMAXSIZ && sp);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->get(kbuf, ksiz, sp);
    }

    /**
     * Remove a record.
     */
    int32_t ktdbremove(KTRDB* db, const char* kbuf, size_t ksiz) {
        _assert_(db && kbuf && ksiz <= MEMMAXSIZ);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->remove(kbuf, ksiz);
    }

    int32_t ktdbclear(KTRDB* db) {
        _assert_(db);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->clear();
    }

    int64_t ktdbmatchprefix(KTRDB* db, const char* prefix, char** strary, size_t max) {
        _assert_(db && prefix && strary && max <= MEMMAXSIZ);
        RemoteDB* pdb = (RemoteDB*)db;
        std::vector<std::string> strvec;
        if (pdb->match_prefix(prefix, &strvec, max) == -1) return -1;
        int64_t cnt = 0;
        std::vector<std::string>::iterator it = strvec.begin();
        std::vector<std::string>::iterator itend = strvec.end();
        while (it != itend) {
            size_t ksiz = it->size();
            char* kbuf = new char[ksiz+1];
            std::memcpy(kbuf, it->data(), ksiz);
            kbuf[ksiz] = '\0';
            strary[cnt++] = kbuf;
            ++it;
        }
        return cnt;
    }

    int64_t ktdbgetbulkbinary(KTRDB* db, const char** keys, size_t ksiz, char** strary, size_t* sizear) {
        _assert_(db && strary && sizear);
        RemoteDB* pdb = (RemoteDB*)db;

        std::vector< RemoteDB::BulkRecord > bulk_recs;
        for (size_t i=0; i < ksiz; i++) {
            RemoteDB::BulkRecord rec = { 0, keys[i], "", 0 };
            bulk_recs.push_back(rec);
        }

        if (pdb->get_bulk_binary(&bulk_recs) == -1) return -1;
        int64_t cnt = 0;
        std::vector<RemoteDB::BulkRecord>::iterator it = bulk_recs.begin();
        std::vector<RemoteDB::BulkRecord>::iterator itend = bulk_recs.end();
        while (it != itend) {
            if (it->xt == -1) {
                strary[cnt++] = '\0';
            } else {
                size_t vsiz = it->value.size();
                char* vbuf = new char[vsiz+1];
                std::memcpy(vbuf, it->value.data(), vsiz);
                vbuf[vsiz] = '\0';
                strary[cnt] = vbuf;
                sizear[cnt] = vsiz;
                cnt++;
            }
            ++it;
        }
        return cnt;
    }

    int64_t ktdbremovebulkbinary(KTRDB* db, const char** keys, size_t ksiz) {
        _assert_(db && ksiz);
        RemoteDB* pdb = (RemoteDB*)db;

        std::vector< RemoteDB::BulkRecord > bulk_recs;
        for (size_t i=0; i < ksiz; i++) {
            RemoteDB::BulkRecord rec = { 0, keys[i], "", 0 };
            bulk_recs.push_back(rec);
        }

        return pdb->remove_bulk_binary(bulk_recs);
    }

    int64_t ktdbsetbulkbinary(KTRDB* db, const char** keys, size_t ksiz, char** vals, size_t vsiz) {
        _assert_(db && ksiz && vsiz && ksiz == vsiz);
        RemoteDB* pdb = (RemoteDB*)db;

        std::vector< RemoteDB::BulkRecord > bulk_recs;
        for (size_t i=0; i < ksiz; i++) {
            RemoteDB::BulkRecord rec = { 0, keys[i], vals[i], 0 };
            bulk_recs.push_back(rec);
        }

        return pdb->set_bulk_binary(bulk_recs);
    }

    int32_t ktdbplayscript(KTRDB* db, const char* name, const char** params, size_t psiz, char** strary) {
        _assert_(db && strary && params);
        RemoteDB* pdb = (RemoteDB*)db;

        std::map< std::string, std::string > paramsIn;
        std::map< std::string, std::string > result;
        for (size_t i=0; i < psiz; i++) {
            paramsIn[params[i]] = params[i+1];
            i++;
        }

        if (pdb->play_script(name, paramsIn, &result) == false) return -1;
        int64_t cnt = 0;
        std::map<std::string,std::string>::iterator it = result.begin();
        std::map<std::string,std::string>::iterator itend = result.end();
        while (it != itend) {
            size_t ksiz = it->first.size();
            char* kbuf = new char[ksiz+1];
            size_t vsiz = it->second.size();
            char* vbuf = new char[vsiz+1];
            std::memcpy(kbuf, it->first.data(), ksiz);
            std::memcpy(vbuf, it->second.data(), vsiz);
            kbuf[ksiz] = '\0';
            vbuf[vsiz] = '\0';
            strary[cnt++] = kbuf;
            strary[cnt++] = vbuf;
            ++it;
        }
        return cnt;
    }

    int32_t ktdbecode(KTRDB* db) {
        _assert_(db);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->error().code();
    }

    const char* ktecodename(int32_t code) {
        _assert_(true);
        return RemoteDB::Error::codename((RemoteDB::Error::Code)code);
    }

    int64_t ktdbcount(KTRDB* db) {
        _assert_(db);
        RemoteDB* pdb = (RemoteDB*)db;
        return pdb->count();
    }
}
