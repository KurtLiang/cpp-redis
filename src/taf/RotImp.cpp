
#include "RotImp.h"
#include "RotServer.h"
#include "server.h" //redis server
#include "ErrorCode.h"

#define SHR_KEY_SIZE        512
#define HT_FIELD_SIZE       256

//TOHACK need these?
//signalModifiedKey(db, key);
//notifyKeyspaceEvent();

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
    //for  test TODO
    LOG->debug()<< __FUNCTION__ << endl;


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
        LOG->error() << __FUNCTION__ << "|" << " wrong obj->encoding:"
            << obj->encoding << endl;
        *iret = ERR_UNKNOWN;
        return -1;
    }

    return 0;
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


taf::Int32 RotImp::mset(taf::Int32 appId,const map<std::string, std::string> & mKVs, const Comm::StringRobjOption &opt,  taf::JceCurrentPtr current)
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

        string sV;
        int  tmpret;
        if (robj2string(val, sV, &tmpret))
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
    int iret = -1;

    PROC_BEGIN
    __TRY__

    GetDb(db, appId, iret);

    result = 0;
    long long value = 0;

    updateSharedKeyObj(key, sk, iret)

    robj *vobj = lookupKeyWrite(db, key);

    if (vobj)
    {
        verifyRobjType(vobj, OBJ_STRING, iret);

        if (getLongLongFromObject(vobj, &value) != C_OK)
        {
            LOG->error() << " value is not an integer or out of range "<< endl;
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
        {
            dbOverwrite(db, key,new_vobj);
        }
        else
        {
            dbAdd(db, key, new_vobj);
        }
    }

    ++server.dirty;
    result = value;

    iret = 0;

    __CATCH__
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


taf::Int32 RotImp::push(taf::Int32 appId,const std::string & sK,const vector<std::string> & vItems,Comm::EListDirection dir,const Comm::ListRobjOption & opt,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN
    __TRY__

    GetDb(db, appId, iret);
    updateSharedKeyObj(key, sk, iret);

    robj *lobj = lookupKeyWrite(db, key);
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
    robj* val = listTypePop(lobj, where);
    if (val == NULL)
    {
        iret = 0;// or ERR_NOEXISTS?
        PROC_BREAK
    }

    if (robj2string(val, sItem, &iret))
    {
        decrRefCount(val); //don't forget to release object
        PROC_BREAK
    }

    decrRefCount(val);
    ++server.dirty;

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

    if (lobj->encoding != OBJ_ENCODING_QUICKLIST)
    {
        LOG->error() << __FUNCTION__ << "| object encoding not matched, expecting:"
            << OBJ_ENCODING_QUICKLIST << ", got:" << lobj->encoding << endl;

        PROC_BREAK
    }

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

            int t;
            if (robj2string(value, sval, &t))
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
            dbDelete(db, key);
            keyremoved = 1;
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
    int keyremoved = 0;
    for (auto &s : vMembers)
    {

        robj *val = tryObjectEncoding(createEmbeddedStringObject(s.data(), s.length()));
        if (setTypeRemove(setobj, val)) ++deleted;
        decrRefCount(val);

        if (setTypeSize(setobj) == 0)
        {
            dbDelete(db, key);
            keyremoved = 1;
            break;
        }
    }

    server.dirty += deleted;
    if (keyremoved)
    {
        LOG->debug() << __FUNCTION__ << "| set object removed since all members deleted :" << sK << endl;
    }

    iret = 0;

    PROC_END

    FDLOG() << iret << "|" << __FUNCTION__ << "|" << appId << endl;
    return iret;
}

taf::Int32 RotImp::smembers(taf::Int32 appId,const std::string & sK,vector<std::string> &vMembers,taf::JceCurrentPtr current)
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

taf::Int32 RotImp::sinter(taf::Int32 appId,const vector<std::string> & vK,vector<std::string> &vResults,taf::JceCurrentPtr current)
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

taf::Int32 RotImp::sdiff(taf::Int32 appId,const vector<std::string> & vK,vector<std::string> &vResults,taf::JceCurrentPtr current)
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

taf::Int32 RotImp::sunion(taf::Int32 appId,const vector<std::string> & vK,vector<std::string> &vResults,taf::JceCurrentPtr current)
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

taf::Int32 RotImp::zrank(taf::Int32 appId,const std::string & sK,const std::string & sMember,taf::Int32 rev,taf::Int32 &rank,taf::JceCurrentPtr current)
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

taf::Int32 RotImp::zrange(taf::Int32 appId,const std::string & sK,taf::Int32 start,taf::Int32 stop,const Comm::ZsetRangeOption & opt,vector<Comm::ZsetScoreMember> &vScoreMembers,taf::JceCurrentPtr current)
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

