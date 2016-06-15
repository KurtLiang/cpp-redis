
#include "RotImp.h"
#include "RotServer.h"
#include "server.h" //redis server
#include "ErrorCode.h"
#include <boost/multi_index/detail/scope_guard.hpp>

#define SHR_KEY_SIZE        512
#define HT_FIELD_SIZE       256

//TOHACK need these?
//signalModifiedKey(db, key);
//notifyKeyspaceEvent();

#define _SCOPE_GUARD_BEGIN {
#define _SCOPE_GUARD_END   }

#define GetDb(db, appId, iret) \
    auto db = g_app.lookforDb(appId);   \
    if (db == nullptr)                  \
    {                                   \
        LOG->error() << __FUNCTION__  << "|" << " failed to locate db for app id:" << appId << endl; \
        iret = ERR_INVAILDINPUT;        \
        PROC_BREAK                      \
    }

#define updateSharedKeyObj(key, sk, iret) \
    robj *key = copyGetSharedKey(sK);   \
    if (key == nullptr)                 \
    {                                   \
        iret = ERR_INVAILDINPUT;        \
        PROC_BREAK                      \
    }

#define verifyRobjType(obj, type_, iret) \
    if ((obj)->type != type_)            \
    {                                   \
        LOG->error() << __FUNCTION__ << "|" << " wrong object type :" << (obj)->type << ", expecting:" << type_ << endl; \
        iret = ERR_UNKNOWN;             \
        PROC_BREAK                      \
    }

#define verifyRobjEncoding(obj, encoding_, iret) \
    if (obj->encoding != encoding_)         \
    {                                       \
        LOG->error() << __FUNCTION__ << "| object encoding not matched, expecting:" \
            << encoding_ << " got:" << obj->encoding << endl;                      \
        iret = ERR_UNKNOWN;                 \
        PROC_BREAK                          \
    }

using boost::multi_index::detail::make_guard;

robj *allocateFieldObj()
{
    return createStringObject(NULL, HT_FIELD_SIZE);
}

int fillFieldObj(robj *o, const string &s)
{

    if (s.size() >= HT_FIELD_SIZE)
    {
        LOG->error() << __FUNCTION__ << "field string is too large:" << s.size()
            << ", part of it:" << s.substr(0, 64)<< endl;
        return 1;
    }

    o->ptr = sdscpylen((sds)o->ptr, s.data(), s.length());
    return 0;
}

void releaseFieldObj(robj *o)
{
    if (o)
    {
        assert (o->refcount == 1);
        int cnt = o->refcount;
        while (cnt-- > 0)
        {
            decrRefCount(o);
        }
    }
}


void RotImp::initialize()
{
    auto_del_key_if_empty_ = 1;

    shr_key_ = createRawStringObject(NULL, SHR_KEY_SIZE);
    assert (shr_key_->refcount == 1);
}

void RotImp::destroy()
{
    assert (shr_key_->refcount == 1);
    if (shr_key_->refcount != 1)
    {
        LOG->error() << "|" << __FUNCTION__ << "|" << " incorrect sh" << endl;
    }
    else
    {
        decrRefCount(shr_key_);
        shr_key_ = NULL;
    }
}

int RotImp::onDispatch(taf::JceCurrentPtr _current, vector<char> &_sResponseBuffer)
{
    freeMemoryIfNeeded();
    return Rot::onDispatch(_current, _sResponseBuffer);
}

/*
 * @return 0 on success, otherwise -1
 */
int robj2string(robj *obj, string &str, int *iret)
{
    if (obj->type != OBJ_STRING)
    {
        LOG->error() << __FUNCTION__ << "|"
            << " wrong object type :" << obj->type << ", expecting:" << OBJ_STRING << endl;
        *iret = ERR_UNKNOWN;
        return -1;
    }

    if (sdsEncodedObject(obj))
    {
        const char *buf = (const char *)obj->ptr;
        size_t len = sdslen((sds)obj->ptr);
        str.assign(buf, len);
    }
    else if (obj->encoding == OBJ_ENCODING_INT)
    {
        char buf[32] = "";
        int len = ll2string(buf, sizeof(buf), (long)obj->ptr);
        str.assign(buf, len);

    }
    else
    {
        LOG->error() << __FUNCTION__ << "|string type, wrong obj->encoding:"
            << obj->encoding << endl;
        *iret = ERR_UNKNOWN;
        return -1;
    }

    return 0;
}

string robj2string(robj *obj)
{
    if (obj->type == OBJ_STRING)
    {
        if (sdsEncodedObject(obj))
        {
            const char *buf = (const char *)obj->ptr;
            size_t len = sdslen((sds)obj->ptr);
            return string(buf, len);
        }
        else if (obj->encoding == OBJ_ENCODING_INT)
        {
            return std::to_string((long)obj->ptr);
        }
    }
    else
    {
        LOG->error() << __FUNCTION__ << "|"
            << " wrong object type :" << obj->type << ", expecting:" << OBJ_STRING << endl;
    }

    return "";
}


redisObject * RotImp::copyGetSharedKey(const string &s)
{
    if (s.size() >= SHR_KEY_SIZE)
    {
        LOG->error() << "key string is too large:" << s.size()
            << ", part of it:" << s.substr(0, 64)<< endl;
        return NULL;
    }

    assert (shr_key_->type == OBJ_STRING);
    assert (shr_key_->encoding == OBJ_ENCODING_RAW);

    shr_key_->ptr = sdscpylen((sds)shr_key_->ptr, s.data(), s.length());
    return shr_key_;
}

taf::Int32 RotImp::getAppName(taf::Int32 appId,std::string &appName,taf::JceCurrentPtr current)
{
    appName = g_app.lookforAppName(appId);
    if (appName.empty())
        return ERR_NOEXISTS;

    return 0;
}


/*
 * refer to setGenericCommand
 */
taf::Int32 RotImp::set(taf::Int32 appId,const std::string & sK,const std::string & sV, const Comm::StringRobjOption &opt, taf::JceCurrentPtr current)
{
    //TODO opt

    int iret = -1;

    PROC_BEGIN
    __TRY__

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret);

    robj *val = tryObjectEncoding(createStringObject(sV.data(), sV.length()));
    setKey(db, key, val);
    decrRefCount(val);
    assert (val->refcount == 1 || val->encoding == OBJ_ENCODING_INT);//maybe shared integers
    ++server.dirty;

    iret = 0;

    __CATCH__
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}


taf::Int32 RotImp::mset(taf::Int32 appId,const map<std::string, std::string> & mKVs, const Comm::StringRobjOption &opt, taf::JceCurrentPtr current)
{
    //TODO opt

    int iret = -1;

    PROC_BEGIN
    __TRY__

    GetDb(db, appId, iret);

    for (auto &KV : mKVs)
    {
        auto &sK = KV.first;
        auto &sV = KV.second;

        robj *key = copyGetSharedKey(sK);
        if (key == nullptr)
        {
            continue;
        }

        robj *val = tryObjectEncoding(createStringObject(sV.data(), sV.length()));
        setKey(db, key, val);
        decrRefCount(val);
        assert (val->refcount == 1 || val->encoding == OBJ_ENCODING_INT);//maybe shared integers
        ++server.dirty;
    }

    iret =0;

    __CATCH__
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::get(taf::Int32 appId,const std::string & sK,std::string &sV,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN
    __TRY__

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret)

    robj *val = lookupKeyRead(db, key);
    if (val == nullptr)
    {
        iret = 0; //or ERR_NOEXISTS?
        PROC_BREAK
    }

    if (robj2string(val, sV, &iret))
    {
        PROC_BREAK
    }

    iret = 0;

    __CATCH__
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::mget(taf::Int32 appId,const vector<std::string> & vKs, map<std::string, std::string> &mKVs, taf::JceCurrentPtr current)
{
    int iret = -1;
    int failed_n = 0;

    PROC_BEGIN
    __TRY__

    GetDb(db, appId, iret);

    for (auto &sK : vKs)
    {
        robj *key = copyGetSharedKey(sK);
        if (key == nullptr)
        {
            ++failed_n;
            continue;
        }


        robj *val = lookupKeyRead(db, key);
        if (val == NULL)
        {
            ++failed_n;
            continue;
        }

        string sV = robj2string(val);
        if (sV.empty())
        {
            ++failed_n;
            continue;
        }

        mKVs[sK] = sV;
    }

    iret = 0;

    __CATCH__
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << "| failed get number:" << failed_n << endl;
    return iret;
}

/*
 * @brief if key doesn't exist, then create one
 */
taf::Int32 RotImp::incrby(taf::Int32 appId, const std::string & sK, taf::Int64 incr, taf::Int64 &result, taf::JceCurrentPtr current)
{
    result = 0;
    int iret = -1;

    PROC_BEGIN
    __TRY__

    long long value = 0;

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret)

    robj *vobj = lookupKeyWrite(db, key);

    if (vobj)
    {
        verifyRobjType(vobj, OBJ_STRING, iret);

        if (getLongLongFromObject(vobj, &value) != C_OK)
        {
            LOG->error() << "value is not an integer or out of range. app-id:"<<appId << " key:" << sK << endl;
            iret = ERR_UNKNOWN;
            PROC_BREAK
        }
    }

    long long oldvalue = value;
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue)))
    {
        LOG->error() << __FUNCTION__ << "|increment or decrement would overflow" << endl;
        iret = ERR_OUTOFRANGE;
        PROC_BREAK
    }
    value += incr;

    if (vobj && vobj->refcount == 1 && vobj->encoding == OBJ_ENCODING_INT &&
        (value < 0 || value >= OBJ_SHARED_INTEGERS) &&
        value >= LONG_MIN && value <= LONG_MAX)
    {
        vobj->ptr = (void*)((long)value);
    }
    else
    {
        robj *new_vobj = createStringObjectFromLongLong(value);
        if (vobj)
            dbOverwrite(db, key,new_vobj);
        else
            dbAdd(db, key, new_vobj);
    }

    ++server.dirty;
    result = value;
    iret = 0;

    __CATCH__
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}


taf::Int32 RotImp::incrbyfloat(taf::Int32 appId,const std::string & sK,taf::Double incr,taf::Double &result,taf::JceCurrentPtr current)
{
    result = 0;
    int iret = -1;

    PROC_BEGIN

    long double value = 0;

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret)

    robj *vobj = lookupKeyWrite(db, key);
    if (vobj)
    {
        verifyRobjType(vobj, OBJ_STRING, iret);
        if (getLongDoubleFromObject(vobj, &value) != C_OK)
        {
            LOG->error() << "value is not an valid float. app-id:" << appId << "  key:" << sK << endl;
            iret = ERR_UNKNOWN;
            PROC_BREAK
        }
    }

    value += incr;
    if (isnan(value) || isinf(value))
    {
        LOG->error() << "increment would produce NaN or Infinity. app-id:" << appId << "  key:" << sK << endl;
        iret = ERR_UNKNOWN;
        PROC_BREAK
    }

    robj *new_o = createStringObjectFromLongDouble(value,1);
    if (vobj)
        dbOverwrite(db, key, new_o);
    else
        dbAdd(db, key,new_o);

    server.dirty++;
    result = value;
    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::append(taf::Int32 appId,const std::string & sK,const std::string & sV,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN
    __TRY__

    if (sV.empty())
    {
        iret = 0;
        PROC_BREAK
    }

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret)

    robj *vobj = lookupKeyWrite(db, key);
    if (vobj == nullptr)
    {
        vobj = createRawStringObject(sV.data(), sV.length());
        if (vobj == nullptr)
        {
            PROC_BREAK
        }

        /* create the key */
        dbAdd(db, key, vobj);
    }
    else
    {
        verifyRobjType(vobj, OBJ_STRING, iret);

        size_t tolen = stringObjectLen(vobj) + sV.length();
        if (tolen > 512*1024*1024)
        {
            LOG->error() << __FUNCTION__ << "| string exceeds maximum allowed size(512MB)" << endl;
            PROC_BREAK
        }

        /* append the value */
        vobj = dbUnshareStringValue(db, key, vobj);
        vobj->ptr = sdscatlen((sds)vobj->ptr, (const void*)sV.data(), sV.length());
    }

    server.dirty++;

    iret = 0;
    __CATCH__
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::push(taf::Int32 appId,const std::string & sK,const vector<std::string> & vItems,Comm::EListDirection dir,const Comm::ListRobjOption & opt, int &length, taf::JceCurrentPtr current)
{
    length = 0;
    int iret = -1;

    PROC_BEGIN
    __TRY__

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret);

    robj *lobj = lookupKeyWrite(db, key);

    if (vItems.empty() || std::count_if(vItems.begin(), vItems.end(), [](const string &s){ return s.empty();}) == (int)vItems.size())
    {
        if (lobj && lobj->type == OBJ_LIST) length = listTypeLength(lobj);

        iret = 0;
        PROC_BREAK
    }

    if (lobj == NULL)
    {
        if (opt.set_if_exist) //bye bye
        {
            iret = 0;
            PROC_BREAK
        }

        lobj = createQuicklistObject();
        quicklistSetOptions((quicklist*)lobj->ptr, server.list_max_ziplist_size, server.list_compress_depth);
        dbAdd(db, key, lobj);
    }
    else
    {
        verifyRobjType(lobj, OBJ_LIST, iret);
    }

    int pushed = 0;
    int where = (dir== Comm::ELIST_HEAD?LIST_HEAD:LIST_TAIL);
    for (auto &sItem : vItems)
    {
        robj *val = createStringObject(sItem.data(), sItem.length());
        listTypePush(lobj, val, where);
        decrRefCount(val);
        assert (val->refcount == 1 || val->encoding == OBJ_ENCODING_INT);//maybe shared integers
        ++pushed;
    }

    server.dirty += pushed;
    length = listTypeLength(lobj);
    iret = 0;

    __CATCH__
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::pop(taf::Int32 appId,const std::string & sK,Comm::EListDirection dir,std::string &sItem,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret);

    robj *lobj = lookupKeyWrite(db, key);
    if (lobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(lobj, OBJ_LIST, iret);

    int where = (dir == Comm::ELIST_HEAD?LIST_HEAD:LIST_TAIL);
    robj* val = listTypePop(lobj, where); //TOHACK unwind
    if (val == NULL)
    {
        iret = ERR_NOEXISTS;
        PROC_BREAK
    }

    if (robj2string(val, sItem, &iret))
    {
        sItem.clear();
        decrRefCount(val); //don't forget to release object!!!
        PROC_BREAK
    }

    decrRefCount(val);
    ++server.dirty;

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}


taf::Int32 RotImp::lindex(taf::Int32 appId,const std::string & sK,taf::Int32 index,std::string &sV,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret);

    robj *lobj = lookupKeyRead(db, key);
    if (lobj == NULL) //no such list
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(lobj, OBJ_LIST, iret);
    verifyRobjEncoding(lobj, OBJ_ENCODING_QUICKLIST, iret);

    quicklistEntry qe;
    if (!quicklistIndex((quicklist*)lobj->ptr, index, &qe))
    {
        //0-not found
        iret = ERR_NOEXISTS;
        PROC_BREAK
    }

    if (qe.value)
    {
        sV.assign((char*)qe.value, qe.sz);
    }
    else
    {
        sV = std::to_string(qe.longval);
    }

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::lset(taf::Int32 appId,const std::string & sK,taf::Int32 index,const std::string & sV,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret);

    robj *lobj = lookupKeyWrite(db, key);
    if (lobj == NULL)
    {
        PROC_BREAK
    }

    verifyRobjType(lobj, OBJ_LIST, iret);
    verifyRobjEncoding(lobj, OBJ_ENCODING_QUICKLIST, iret);

    quicklist *ql = (quicklist*)lobj->ptr;
    int replaced =  quicklistReplaceAtIndex(ql, index, sV.data(), sV.length());
    if (!replaced)
    {
        LOG->error() << __FUNCTION__ << "|out of range" << endl;
        iret = ERR_OUTOFRANGE;
        PROC_BREAK
    }

    ++server.dirty;
    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::llen(taf::Int32 appId,const std::string & sK,taf::Int64 &length,taf::JceCurrentPtr current)
{
    length = 0;
    int iret = -1;

    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret);

    robj *lobj = lookupKeyRead(db, key);
    if (lobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(lobj, OBJ_LIST, iret);
    length =listTypeLength(lobj);

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::lrem(taf::Int32 appId,const std::string & sK,taf::Int64 toremove,const std::string & sV,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    long removed = 0;

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret);

    robj *lobj = lookupKeyWrite(db, key);
    if (lobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(lobj, OBJ_LIST, iret);
    verifyRobjEncoding(lobj, OBJ_ENCODING_QUICKLIST, iret);

    listTypeIterator *li=nullptr;
    _SCOPE_GUARD_BEGIN

    if (toremove < 0)
    {
        toremove = -toremove;
        li = listTypeInitIterator(lobj,-1,LIST_HEAD);
    } else
    {
        li = listTypeInitIterator(lobj,0,LIST_TAIL);
    }

    listTypeEntry entry;
    while (listTypeNext(li,&entry))
    {
        if (quicklistCompare(entry.entry.zi, (const unsigned char*)sV.data(), sV.length())) //return 1 if equal
        {
            listTypeDelete(li, &entry);
            server.dirty++;
            removed++;
            if (toremove && removed == toremove)
                break;
        }
    }

    _SCOPE_GUARD_END
    listTypeReleaseIterator(li);

    int keyremoved = 0;
    if (removed > 0 && listTypeLength(lobj) == 0)
    {
        if (auto_del_key_if_empty_)
        {
            dbDelete(db, key);
            keyremoved = 1;
        }
    }

    if (keyremoved)
    {
        LOG->debug() << __FUNCTION__ << "| list object removed since all elements deleted :" << sK << endl;
    }
    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::lrange(taf::Int32 appId,const std::string & sK,taf::Int32 istart,taf::Int32 iend,vector<std::string> &vItems,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret);

    robj *lobj = lookupKeyRead(db, key);
    if (lobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(lobj, OBJ_LIST, iret);
    verifyRobjEncoding(lobj, OBJ_ENCODING_QUICKLIST, iret);

    long start = istart;
    long end   = iend;
    long llen  = listTypeLength(lobj);

    /* adjust negative indexes */
    if (start < 0) start = llen+start;
    if (end < 0)   end   = llen+end;
    if (start < 0) start = 0;

    if (start > end || start >= llen)
    {
        iret = 0;
        PROC_BREAK
    }

    if (end >= llen) end = llen-1;
    long rangelen = (end-start)+1;

    listTypeIterator *iter = listTypeInitIterator(lobj, start, LIST_TAIL);

    while (rangelen--)
    {
        listTypeEntry entry;
        listTypeNext(iter, &entry);
        quicklistEntry *qe = &entry.entry;
        if (qe->value)
        {
           vItems.push_back(std::string((char*)qe->value, qe->sz));
        }
        else
        {
            vItems.push_back(std::to_string(qe->longval));
        }
    }

    listTypeReleaseIterator(iter);

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::hmset(taf::Int32 appId,const std::string & sK,const map<std::string, std::string> & mFV,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN

    if (mFV.empty())
    {
        iret = 0;
        PROC_BREAK
    }

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    robj *hobj = lookupKeyWrite(db, key);
    if (hobj == NULL)
    {
        hobj = createHashObject();
        dbAdd(db, key, hobj);
    }
    else
    {
        verifyRobjType(hobj, OBJ_HASH, iret);
    }

    for (auto &fv : mFV)
    {
        auto &field = fv.first;
        auto &value = fv.second;

        UNUSED(field);
        UNUSED(value);

        //TODO



    }

    ++server.dirty;
    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::hmget(taf::Int32 appId,const std::string & sK,const vector<std::string> & vFields,map<std::string, std::string> &mFV,taf::JceCurrentPtr current)
{
    int iret = -1;
    robj *fldobj = NULL;

    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    robj *hobj = lookupKeyRead(db, key);
    if (hobj == NULL)
    {
        iret = ERR_NOEXISTS;
        PROC_BREAK
    }

    verifyRobjType(hobj, OBJ_HASH, iret);

    fldobj = allocateFieldObj();

    int err_encoding = 0;
    string sval;
    for (auto &field : vFields)
    {
        sval.clear();

        if (fillFieldObj(fldobj, field))
            continue;

        if (hobj->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *vstr = NULL;
            unsigned int vlen = UINT_MAX;
            long long vll = LLONG_MAX;

            if (hashTypeGetFromZiplist(hobj, fldobj, &vstr, &vlen, &vll))
                continue;

            if (vstr)
                mFV[field] = string((char*)vstr, vlen);
            else
                mFV[field] = std::to_string(vll);
        }
        else if (hobj->encoding == OBJ_ENCODING_HT)
        {
            robj *value;
            if (hashTypeGetFromHashTable(hobj, fldobj, &value))
                continue;

            sval = robj2string(value);
            if (sval.empty())
                continue;

            mFV[field] = sval;
        }
        else
        {
            err_encoding = 1;
            LOG->error() << __FUNCTION__ << "| incorrect encoding for key:" << sK << endl;
            break;
        }
    }

    if (err_encoding)
    {
        mFV.clear();
        PROC_BREAK
    }

    iret = 0;
    PROC_END

    releaseFieldObj(fldobj);

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::hgetall(taf::Int32 appId,const std::string & sK,map<std::string, std::string> &mFV,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    robj *hobj = lookupKeyRead(db, key);
    if (hobj == NULL)
    {
        iret = ERR_NOEXISTS;
        PROC_BREAK
    }

    verifyRobjType(hobj, OBJ_HASH, iret);

    hashTypeIterator *hi = hashTypeInitIterator(hobj);
    _SCOPE_GUARD_BEGIN

    while (hashTypeNext(hi) != C_ERR)
    {
        if (hi->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *vstr = NULL;
            unsigned int vlen = UINT_MAX;
            long long vll = LLONG_MAX;

            string ssk, ssv;
            if (!ziplistGet(hi->fptr, &vstr, &vlen, &vll))//key
                continue;

            if (vstr) ssk.assign((char*)vstr, vlen);
            else      ssk = std::to_string(vll);

            vstr = NULL;
            vlen = UINT_MAX;
            vll = LLONG_MAX;
            if (!ziplistGet(hi->vptr, &vstr, &vlen, &vll)) //value
                continue;

            if (vstr) ssv.assign((char*)vstr, vlen);
            else      ssv = std::to_string(vll);

            mFV[ssk] = ssv;
        }
        else if (hi->encoding == OBJ_ENCODING_HT)
        {
            robj *k = (robj*)dictGetKey(hi->de);
            robj *v = (robj*)dictGetVal(hi->de);
            if (!k || !v) continue;

            string ssk = robj2string(k);
            string ssv = robj2string(v);
            if (ssk.empty() || ssv.empty()) continue;
            mFV[ssk] = ssv;
        }
        else
        {
            LOG->error() << __FUNCTION__ << "|Unknown hash encoding" << endl;
            continue;
        }
    }

    _SCOPE_GUARD_END
    hashTypeReleaseIterator(hi);

    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::hexists(taf::Int32 appId,const std::string & sK,const std::string & sField,taf::Int32 &existed,taf::JceCurrentPtr current)
{
    existed = 0;

    int iret = -1;
    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    robj *hobj = lookupKeyRead(db, key);
    if (hobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(hobj, OBJ_HASH, iret);

    auto fldobj = allocateFieldObj();
    if (fillFieldObj(fldobj, sField))
    {
        PROC_BREAK
    }

    if (hashTypeExists(hobj, fldobj))
        existed = 1;
    releaseFieldObj(fldobj);

    iret =0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::hdel(taf::Int32 appId,const std::string & sK,const vector<std::string> & vFields,taf::JceCurrentPtr current)
{
    int iret = -1;
    robj *fldobj = NULL;
    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    robj *hobj = lookupKeyWrite(db, key);
    if (hobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(hobj, OBJ_HASH, iret);

    int deleted = 0;
    int keyremoved = 0;
    fldobj = allocateFieldObj();
    for (auto &sF : vFields)
    {
        if (fillFieldObj(fldobj, sF))
            continue;

        if (hashTypeDelete(hobj, fldobj))
            continue;

        ++deleted;
        if (hashTypeLength(hobj) == 0)
        {
            if (auto_del_key_if_empty_)
            {
                dbDelete(db, key);
                keyremoved = 1;
            }
            break;
        }
    }

    server.dirty += deleted;
    if (keyremoved)
    {
        LOG->debug() << __FUNCTION__ << "| hash object removed since all fields deleted :" << sK << endl;
    }

    iret = 0;
    PROC_END

    releaseFieldObj(fldobj);

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::sadd(taf::Int32 appId,const std::string & sK,const vector<std::string> & vMembers,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN

    if (vMembers.empty())
    {
        iret = 0;
        PROC_BREAK
    }

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    robj *setobj = lookupKeyWrite(db, key);
    if (setobj == NULL)
    {
        /* encode as an intset if the first item is an integer */
        long long ll;
        const string &s = vMembers[0];

        if (string2ll(s.data(), s.length(), &ll))
            setobj = createIntsetObject();
        else
            setobj = createSetObject();

        dbAdd(db, key, setobj);
    }
    else
    {
        verifyRobjType(setobj, OBJ_SET, iret);
    }

    int added = 0;
    for (auto &s : vMembers)
    {
        robj *val = tryObjectEncoding(createEmbeddedStringObject(s.data(), s.length()));
        if (setTypeAdd(setobj, val)) ++added;
        decrRefCount(val);
    }

    server.dirty += added;
    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::srem(taf::Int32 appId,const std::string & sK,const vector<std::string> & vMembers,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN

    if (vMembers.empty())
    {
        iret = 0;
        PROC_BREAK
    }

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    robj *setobj = lookupKeyWrite(db, key);
    if (setobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(setobj, OBJ_SET, iret);

    int deleted = 0;
    for (auto &s : vMembers)
    {

        robj *val = tryObjectEncoding(createEmbeddedStringObject(s.data(), s.length()));
        if (setTypeRemove(setobj, val)) ++deleted;
        decrRefCount(val);

        if (setTypeSize(setobj) == 0)
        {
            if (auto_del_key_if_empty_)
            {
                dbDelete(db, key);
                LOG->debug() << __FUNCTION__ << "| set object removed since all members deleted :" << sK << endl;
            }
            break;
        }
    }

    server.dirty += deleted;

    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}


taf::Int32 RotImp::spop(taf::Int32 appId,const std::string & sK,taf::Int32 icount,vector<std::string> &vMembers,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    if (icount<=0)
    {
        PROC_BREAK
    }

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    /* TOHACK @ly optimize accoding to count value */
    robj *setobj = lookupKeyWrite(db, key);
    if (setobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(setobj, OBJ_SET, iret);

    long size = setTypeSize(setobj);
    long count = std::min<long>(size, icount);

    while (count-- > 0)
    {
        int64_t llele;
        robj *ele;

        /* Get a random element from the set */
        int encoding = setTypeRandomElement(setobj, &ele, &llele);

        /* Remove the element from the set */
        if (encoding == OBJ_ENCODING_INTSET)
        {
            setobj->ptr = intsetRemove((intset*)setobj->ptr,llele,NULL);
            vMembers.push_back(std::to_string(llele));

        }
        else
        {
            string s = robj2string(ele);
            if (!s.empty())
                vMembers.push_back(s);

            setTypeRemove(setobj,ele);
        }

        ++server.dirty;
    }

    if (setTypeSize(setobj) == 0)
    {
        if (auto_del_key_if_empty_)
        {
            dbDelete(db, key);
            LOG->debug() << __FUNCTION__ << "| set object removed since all members deleted :" << sK << endl;
        }
    }

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::sismember(taf::Int32 appId,const std::string & sK,const std::string & sMember,taf::Int32 &is_mem,taf::JceCurrentPtr current)
{
    is_mem = 0;
    int iret = -1;

    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    robj *setobj = lookupKeyRead(db, key);
    if (setobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(setobj, OBJ_SET, iret);

    robj *mem_o = tryObjectEncoding(createStringObject(sMember.data(), sMember.length()));
    _SCOPE_GUARD_BEGIN

    if (setTypeIsMember(setobj, mem_o))
        is_mem = 1;

    _SCOPE_GUARD_END
    decrRefCount(mem_o);

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

int sinterGenericProc(redisDb *db, const vector<std::string> & vK, const std::string & storeKey, vector<std::string> &vResults)
{
    vector<robj *> setkeys;
    robj *dstkey = NULL;

    for (auto &k : vK)
    {
        auto o = createStringObject(k.data(), k.length());
        setkeys.push_back(o);
    }

    if (!storeKey.empty())
    {
        dstkey = createStringObject(storeKey.data(), storeKey.length());
    }

    auto releaseGuard = make_guard([&setkeys, &dstkey]()
            {
            for (auto &ok : setkeys) { decrRefCount(ok); }
            if (dstkey) decrRefCount(dstkey);
            });

    vector<robj *> srcset;
    robj *dstset = NULL;

    size_t setnum = setkeys.size();
    for (size_t j = 0; j < setnum; j++)
    {
        robj *setobj = dstkey ? lookupKeyWrite(db, setkeys[j]) : lookupKeyRead(db,setkeys[j]);
        if (!setobj)
        {
            if (dstkey)
            {
                if (dbDelete(db,dstkey))
                {
                    server.dirty++;
                }
            }
            return -1;
        }

        if (setobj->encoding != OBJ_SET)
        {
            return -1;
        }

        srcset.push_back(setobj);
    }

    if (srcset.empty())
        return -1;

    /* sort from smallest to largest */
    std::sort(srcset.begin(), srcset.end(), [](robj *s1, robj *s2) -> bool { return setTypeSize(s1) < setTypeSize(s2);} );

    if (dstkey)
    {
        dstset = createIntsetObject();
    }

    int encoding;
    int64_t intobj;
    robj *eleobj;

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    setTypeIterator *si = setTypeInitIterator(srcset[0]);
    while((encoding = setTypeNext(si, &eleobj, &intobj)) != -1)
    {
        size_t j;
        for (j = 1; j < setnum; j++)
        {
            if (srcset[j] == srcset[0])
                continue;

            if (encoding == OBJ_ENCODING_INTSET)
            {
                /* intset with intset is simple... and fast */
                if (srcset[j]->encoding == OBJ_ENCODING_INTSET && !intsetFind((intset*)srcset[j]->ptr,intobj))
                {
                    break;
                }
                else if (srcset[j]->encoding == OBJ_ENCODING_HT)
                {
                    /* in order to compare an integer with an object we
                     * have to use the generic function, creating an object
                     * for this */
                    eleobj = createStringObjectFromLongLong(intobj);
                    if (!setTypeIsMember(srcset[j],eleobj))
                    {
                        decrRefCount(eleobj);
                        break;
                    }
                    decrRefCount(eleobj);
                }
            }
            else if (encoding == OBJ_ENCODING_HT)
            {
                /* Optimization... if the source object is integer
                 * encoded AND the target set is an intset, we can get
                 * a much faster path. */
                if (eleobj->encoding == OBJ_ENCODING_INT
                    && srcset[j]->encoding == OBJ_ENCODING_INTSET
                    && !intsetFind((intset*)srcset[j]->ptr,(long)eleobj->ptr))
                {
                    break;
                }
                /* else... object to object check is easy as we use the
                 * type agnostic API here. */
                else if (!setTypeIsMember(srcset[j],eleobj))
                {
                    break;
                }
            }
        }

        /* Only take action when all sets contain the member */
        if (j == setnum)
        {
            if (!dstset)
            {
                if (encoding == OBJ_ENCODING_HT)
                {
                    string s = robj2string(eleobj);
                    if (!s.empty())
                        vResults.push_back(s);
                }
                else
                {
                    vResults.push_back(std::to_string(intobj));
                }
            }
            else
            {
                if (encoding == OBJ_ENCODING_INTSET)
                {
                    eleobj = createStringObjectFromLongLong(intobj);
                    setTypeAdd(dstset,eleobj);
                    decrRefCount(eleobj);
                } else
                {
                    setTypeAdd(dstset,eleobj);
                }
            }
        }
    }
    setTypeReleaseIterator(si);

    if (dstset)
    {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */
        int deleted = dbDelete(db, dstkey);
        if (setTypeSize(dstset) > 0)
        {
            dbAdd(db, dstkey, dstset);
        }
        else
        {
            decrRefCount(dstset);
        }

        if (deleted)
        {
            LOG->debug() << __FUNCTION__ << "|delete previous key's value before storing :" << storeKey << endl;
        }
        server.dirty++;
    }

    return 0;
}

taf::Int32 RotImp::smembers(taf::Int32 appId,const std::string & sK,vector<std::string> &vMembers,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN
    __TRY__

    GetDb(db, appId, iret);

    vector<string> vK;
    vK.push_back(sK);
    iret = sinterGenericProc(db, vK, "", vMembers);

    __CATCH__
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::sinter(taf::Int32 appId, const vector<std::string> & vK, const std::string & storeKey, vector<std::string> &vResults,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN
    __TRY__

    if (vK.empty())
    {
        PROC_BREAK
    }

    GetDb(db, appId, iret);
    iret = sinterGenericProc(db, vK, storeKey, vResults);

    __CATCH__
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

int suniondiffGenericProc(redisDb *db, const vector<std::string> & vK, const std::string & storeKey, vector<std::string> &vResults, int op)
{
    vector<robj*> setkeys;
    robj* dstkey = NULL;

    for (auto &k : vK)
    {
        auto o = createStringObject(k.data(), k.length());
        setkeys.push_back(o);
    }

    if (!storeKey.empty())
    {
        dstkey = createStringObject(storeKey.data(), storeKey.length());
    }

    auto releaseGuard = make_guard([&setkeys, &dstkey]()
            {
            for (auto &ok : setkeys) { decrRefCount(ok); }
            if (dstkey) decrRefCount(dstkey);
            });

    vector<robj*> srcset;
    robj* dstset = NULL;

    int tmpidx=0;
    for (const auto &key : setkeys)
    {
        ++tmpidx;
        robj *setobj = dstkey ? lookupKeyWrite(db, key) : lookupKeyRead(db, key);
        if (!setobj)
        {
            if (op == SET_OP_DIFF && tmpidx == 1)
            {
                /* if diff and first  set can't be found then return */
                return -1;
            }

            continue;
        }
        if (setobj->type != OBJ_SET)
        {
            return -1;
        }
        srcset.push_back(setobj);
    }

    if (srcset.empty())
    {
        return -1;
    }

    size_t setnum = srcset.size();
    int diff_algo = 1;

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * We compute what is the best bet with the current input here. */
    if (op == SET_OP_DIFF && srcset[0])
    {
        long long algo_one_work = 0, algo_two_work = 0;

        for (size_t j = 0; j < setnum; j++)
        {
            algo_one_work += setTypeSize(srcset[0]);
            algo_two_work += setTypeSize(srcset[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1)
        {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            std::sort(srcset.begin()+1, srcset.begin(), [](robj *s1, robj *s2)->bool { return setTypeSize(s1) > setTypeSize(s2) ;});
        }
    }

    /* buffer to store */
    dstset = createIntsetObject();

    if (op == SET_OP_UNION)
    {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        for (size_t j = 0; j < setnum; ++j)
        {
            robj *ele;
            setTypeIterator *si = setTypeInitIterator(srcset[j]);
            while((ele = setTypeNextObject(si)) != NULL)
            {
                setTypeAdd(dstset,ele);
                decrRefCount(ele);
            }
            setTypeReleaseIterator(si);
        }
    }
    else if (op == SET_OP_DIFF && diff_algo == 1)
    {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
        robj *ele;
        setTypeIterator *si = setTypeInitIterator(srcset[0]);
        while((ele = setTypeNextObject(si)) != NULL)
        {
            size_t j;
            for (j = 1; j < setnum; j++)
            {
                if (srcset[j] == srcset[0]) break; /* same set! */
                if (setTypeIsMember(srcset[j],ele)) break;
            }

            if (j == setnum)
            {
                /* There is no other set with this element. Add it. */
                setTypeAdd(dstset,ele);
            }
            decrRefCount(ele);
        }
        setTypeReleaseIterator(si);
    }
    else if (op == SET_OP_DIFF && diff_algo == 2)
    {
        /* DIFF Algorithm 2:
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * This is O(N) where N is the sum of all the elements in every
         * set. */
        for (size_t j = 0; j < setnum; j++)
        {
            int cardinality = 0;
            robj *ele;
            setTypeIterator *si = setTypeInitIterator(srcset[j]);
            while((ele = setTypeNextObject(si)) != NULL)
            {
                if (j == 0)
                {
                    if (setTypeAdd(dstset,ele)) cardinality++;
                }
                else
                {
                    if (setTypeRemove(dstset,ele)) cardinality--;
                }
                decrRefCount(ele);
            }
            setTypeReleaseIterator(si);

            /* Exit if result set is empty as any additional removal
             * of elements will have no effect. */
            if (cardinality == 0) break;
        }
    }

    /* Output the content of the resulting set, if not in STORE mode */
    if (!dstkey)
    {
        robj *ele;
        setTypeIterator *si = setTypeInitIterator(dstset);
        while((ele = setTypeNextObject(si)) != NULL)
        {
            string s = robj2string(ele);
            if (!s.empty())
            {
                vResults.push_back(s);
            }
            decrRefCount(ele);
        }
        setTypeReleaseIterator(si);
        decrRefCount(dstset);
    }
    else
    {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        int deleted = dbDelete(db, dstkey);

        if (setTypeSize(dstset) > 0)
        {
            dbAdd(db,dstkey,dstset);
        }
        else
        {
            decrRefCount(dstset);
        }

        if (deleted)
        {
            LOG->debug() << __FUNCTION__ << "|delete previous key's value before storing :" << storeKey << endl;
        }
        server.dirty++;
    }

    return 0;
}

taf::Int32 RotImp::sdiff(taf::Int32 appId,const vector<std::string> & vK, const std::string & storeKey, vector<std::string> &vResults,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    if (vK.empty())
    {
        PROC_BREAK
    }

    GetDb(db, appId, iret);
    iret = suniondiffGenericProc(db, vK, storeKey, vResults, SET_OP_DIFF);

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::sunion(taf::Int32 appId,const vector<std::string> & vK, const std::string & storeKey, vector<std::string> &vResults,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    if (vK.empty())
    {
        PROC_BREAK
    }

    GetDb(db, appId, iret);
    iret = suniondiffGenericProc(db, vK, storeKey, vResults, SET_OP_UNION);

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}


taf::Int32 RotImp::zadd(taf::Int32 appId,const std::string & sK,const vector<Comm::ZsetScoreMember> & vScoreMember,const Comm::ZsetRobjOption & option,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    //TODO implement me
    //

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::zrem(taf::Int32 appId,const std::string & sK,const vector<std::string> & vMembers,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    //TODO implement me
    //

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::zrank(taf::Int32 appId,const std::string & sK,const std::string & sMember,taf::Int32 reverse,taf::Int32 &rank,taf::JceCurrentPtr current)
{
    rank = -1;

    int iret = -1;
    PROC_BEGIN

    if (sMember.empty())
    {
        iret = 0;
        PROC_BREAK
    }

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    robj *zobj = lookupKeyRead(db,key);
    if (zobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(zobj, OBJ_ZSET, iret);
    unsigned long llen = zsetLength(zobj);

    if (llen == 0 || llen  > 0xfffffffe)
    {
        iret = 0;
        PROC_BREAK
    }

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *zl = (unsigned char*)zobj->ptr;
        unsigned char *eptr, *sptr;

        eptr = ziplistIndex(zl,0);
        sptr = ziplistNext(zl,eptr);

        rank = 1;
        while(eptr != NULL)
        {
            if (ziplistCompare(eptr, (const unsigned char*)sMember.data(), sMember.length()))  //equal
                break;

            rank++;
            zzlNext(zl,&eptr,&sptr);
        }

        if (eptr != NULL)
        {
            if (reverse)
                rank = llen-rank;
            else
                rank = rank-1;
        }
        else
        {
            iret = 0;
            PROC_BREAK;
        }
    }
    else if (zobj->encoding == OBJ_ENCODING_SKIPLIST)
    {
        zset *zs = (zset*)zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        robj *member_o = createStringObject(sMember.data(), sMember.length());
        de = dictFind(zs->dict_, member_o);
        if (de != NULL)
        {
            score = *(double*)dictGetVal(de);
            rank = zslGetRank(zsl,score, member_o);
            if (reverse)
                rank = llen - rank;
            else
                rank = rank-1;
        }
        else
        {
            decrRefCount(member_o);
            iret = 0;
            PROC_BREAK
        }
        decrRefCount(member_o);
    }
    else
    {
        serverPanic("Unknown sorted set encoding");
        PROC_BREAK
    }

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::zincrby(taf::Int32 appId,const std::string & sK,taf::Double increment,const std::string & sMember,taf::Double &new_score,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    //TODO implement me
    //

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::zrange(taf::Int32 appId,const std::string & sK,taf::Int32 start,taf::Int32 end,const Comm::ZsetRangeOption & opt,vector<Comm::ZsetScoreMember> &vScoreMembers,taf::JceCurrentPtr current)
{
    int iret = -1;
    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    robj *zobj = lookupKeyRead(db, key);
    if (zobj == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    verifyRobjType(zobj, OBJ_ZSET, iret);

    /* Sanitize indexes. */
    int llen = zsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0)   end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen)
    {
        iret = 0;
        PROC_BREAK
    }

    if (end >= llen) end = llen-1;
    int rangelen = (end-start)+1;
    Comm::ZsetScoreMember sm;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *zl = (unsigned char*)zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (opt.reverse)
            eptr = ziplistIndex(zl,-2-(2*start));
        else
            eptr = ziplistIndex(zl,2*start);

        sptr = ziplistNext(zl,eptr);

        while (rangelen--)
        {
            if (eptr == NULL || sptr == NULL)
                break;

            if (ziplistGet(eptr,&vstr,&vlen,&vlong) == 0)
            {
                break;
            }

            sm.resetDefautlt();
            if (vstr == NULL)
            {
                sm.member = std::to_string(vlong);
            }
            else
            {
                sm.member.assign((const char*)vstr, vlen);
            }

            if (opt.with_scores)
            {
                sm.score = zzlGetScore(sptr);
            }

            vScoreMembers.push_back(sm);

            if (opt.reverse)
                zzlPrev(zl,&eptr,&sptr);
            else
                zzlNext(zl,&eptr,&sptr);
        }
    }
    else if (zobj->encoding == OBJ_ENCODING_SKIPLIST)
    {
        zset *zs = (zset*)zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        robj *ele;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        if (opt.reverse)
        {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,llen-start);
        }
        else
        {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

        while(rangelen--)
        {
            sm.resetDefautlt();
            if (ln == NULL)
                break;

            ele = ln->obj;

            sm.member = robj2string(ele);
            if (opt.with_scores)
            {
                sm.score = ln->score;
            }
            vScoreMembers.push_back(sm);

            ln = opt.reverse ? ln->backward : ln->level[0].forward;
        }
    }
    else
    {
        LOG->error() << __FUNCTION__ << "|Unknown sorted set encoding" << endl;
        PROC_BREAK
    }

    if (rangelen > 0)
    {
        LOG->error() << __FUNCTION__  <<  "|failed to get enough member from zset" << endl;
    }

    iret = 0;
    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}


taf::Int32 RotImp::del(taf::Int32 appId,const vector<std::string> & vKs,taf::Int32 &deleted, taf::JceCurrentPtr current)
{
    deleted=0;
    int iret = -1;

    PROC_BEGIN

    GetDb(db, appId, iret);

    for (auto &sK : vKs)
    {
        robj *key = copyGetSharedKey(sK);
        if (key == NULL)
            continue;

        expireIfNeeded(db, key);
        if (dbDelete(db, key))
        {
            ++server.dirty;
            ++deleted;
        }
    }

    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::exists(taf::Int32 appId,const vector<std::string> & vKs,taf::Int32 &existed,taf::JceCurrentPtr current)
{
    int iret = -1;
    existed = 0;

    PROC_BEGIN

    GetDb(db, appId, iret);

    for (auto &sK : vKs)
    {
        robj *key = copyGetSharedKey(sK);
        if (key == NULL)
            continue;

        expireIfNeeded(db, key);
        if (dbExists(db, key))
            ++existed;
    }

    iret = 0 ;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::expire(taf::Int32 appId, const std::string & sK, taf::Int64 seconds,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN

    int is_persistent = (seconds < 0);

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    /* return if no key */
    if (lookupKeyWrite(db, key) == NULL)
    {
        iret = 0;
        PROC_BREAK
    }

    if (is_persistent)
    {
        removeExpire(db, key);
    }
    else
    {
        long long when = mstime() + seconds*1000;
        setExpire(db, key, when);
    }

    ++server.dirty;
    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::ttl(taf::Int32 appId, const std::string & sK, /*out */taf::Int64 &seconds, taf::JceCurrentPtr current)
{
    seconds = -2; /* not existed or expired by default */

    int iret = -1;

    PROC_BEGIN

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sK, iret);

    if (lookupKeyRead(db, key) == NULL) //bye bye
    {
        iret = 0;
        PROC_BREAK
    }

    auto expire = getExpire(db, key);
    if (expire == -1) //persistent
    {
        seconds = -1;
    }
    else
    {
        auto ttl = expire - mstime();
        if (ttl < 0) ttl = 0;
        seconds = ttl/1000;
    }

    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

