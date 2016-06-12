
#include "RotImp.h"
#include "RotServer.h"
#include "server.h" //redis server
#include "ErrorCode.h"

#define SHR_KEY_SIZE        512

//TODO need this?
//signalModifiedKey(db, key);

#define GetDb(db, appId, iret) \
    auto db = g_app.lookforDb(appId);  \
    if (db == nullptr)                  \
    {                                   \
        LOG->error() << __FUNCTION__  << "|" << " failed to locate db for app id:" << appId << endl; \
        iret = ERR_INVAILDINPUT;        \
        PROC_BREAK;                     \
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


redisObject * RotImp::copyGetSharedKey(const string &s)
{
    if (s.size() >= SHR_KEY_SIZE)
    {
        LOG->error() << __FUNCTION__ << "| key string is too large:" << s.size()
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
taf::Int32 RotImp::set(taf::Int32 appId,const std::string & sK,const std::string & sV,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN
    __TRY__

    GetDb(db, appId, iret);

    robj *key = copyGetSharedKey(sK);
    if (key == nullptr)
    {
        iret = ERR_INVAILDINPUT;
        PROC_BREAK
    }
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


taf::Int32 RotImp::mset(taf::Int32 appId,const map<std::string, std::string> & mKVs,taf::JceCurrentPtr current)
{
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

/*
 * @return 0 on success, otherwise -1
 */
int robj2string(robj *obj, string &str, int *iret)
{
    if (obj == nullptr)
    {
        *iret = ERR_NOEXISTS;
        return -1;
    }

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

/*
 * @return 0 if it is matched, else 1.
 */
int checkRobjType(robj *obj, int type)
{
    return obj->type == type?0:1;
}

taf::Int32 RotImp::get(taf::Int32 appId,const std::string & sK,std::string &sV,taf::JceCurrentPtr current)
{
    int iret = -1;

    PROC_BEGIN
    __TRY__

    GetDb(db, appId, iret);

    robj *key = copyGetSharedKey(sK);
    if (key == nullptr)
    {
        iret = ERR_INVAILDINPUT;
        PROC_BREAK
    }

    robj *val = lookupKeyRead(db, key);

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

    robj *key  = copyGetSharedKey(sK);
    if (key == nullptr)
    {
        iret = ERR_INVAILDINPUT;
        PROC_BREAK
    }

    robj *vobj = lookupKeyWrite(db, key);

    if (vobj)
    {
        if (checkRobjType(vobj, OBJ_STRING))
        {
            LOG->error() << __FUNCTION__ << "|"
                << " wrong object type :" << vobj->type << ", expecting:" << OBJ_STRING << endl;
            iret = ERR_UNKNOWN;
            PROC_BREAK
        }

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

    robj *key = copyGetSharedKey(sK);
    if (key == nullptr)
    {
        iret = ERR_INVAILDINPUT;
        PROC_BREAK
    }

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
        if (checkRobjType(vobj, OBJ_STRING))
        {
            LOG->error() << __FUNCTION__ << "| wrong object type :"
                << vobj->type << ", expecting:" << OBJ_STRING << endl;
            PROC_BREAK
        }

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

taf::Int32 RotImp::del(taf::Int32 appId,const vector<std::string> & vKs,taf::Int32 &deleted, taf::JceCurrentPtr current)
{
    int iret = -1;
    deleted=0;

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
    return 0;
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

