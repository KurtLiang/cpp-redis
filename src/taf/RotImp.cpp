
#include "RotImp.h"
#include "servant/Application.h"
#include "server.h"



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
    //TODO
    return 0;
}


taf::Int32 RotImp::set(taf::Int32 appId,const std::string & sK,const std::string & sV,taf::JceCurrentPtr current)
{
    //TODO

    return 0;
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

