
#include "RotImp.h"
#include "RotServer.h"
#include "server.h" //redis server
#include "ErrorCode.h"

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
    //TODO
}

void RotImp::destroy()
{
    //TODO
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

    robj *key=nullptr, *val=nullptr; //TODO
    setKey(db, key, val);
    ++server.dirty;

    iret = 0;
    __CATCH__
    PROC_END

    FDLOG() << __FUNCTION__ << "|" << iret << "|" << appId << "|" << sK << "|" << sV << endl;
    return iret;
}


taf::Int32 RotImp::mset(taf::Int32 appId,const map<std::string, std::string> & mKVs,taf::JceCurrentPtr current)
{
    //TODO
    return 0;
}

taf::Int32 RotImp::get(taf::Int32 appId,const std::string & sK,std::string &sV,taf::JceCurrentPtr current)
{
    //TODO

    return 0;
}

taf::Int32 RotImp::mget(taf::Int32 appId,const vector<std::string> & vKs,map<std::string, std::string> &mKVs,taf::JceCurrentPtr current)
{
    //TODO

    return 0;
}

taf::Int32 RotImp::incrby(taf::Int32 appId,const std::string & sK,taf::Int64 incr,taf::JceCurrentPtr current)
{
    //TODO

    return 0;
}

taf::Int32 RotImp::append(taf::Int32 appId,const std::string & sK,const std::string & sV,taf::JceCurrentPtr current)
{
    //TODO

    return 0;
}

taf::Int32 RotImp::del(taf::Int32 appId,const vector<std::string> & vKs,taf::Int32 &number,taf::JceCurrentPtr current)
{
    //TODO
    return 0;
}

taf::Int32 RotImp::exists(taf::Int32 appId,const vector<std::string> & vKs,taf::Int32 &number,taf::JceCurrentPtr current)
{
    //TODO
    return 0;
}

