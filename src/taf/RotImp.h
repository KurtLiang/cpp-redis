#ifndef __ROT_IMP_H__
#define __ROT_IMP_H__

#include "Rot.h"

class RotImp: public Comm::Rot
{
public:
    virtual ~RotImp() {}
    virtual void initialize();
    virtual void destroy();


    virtual taf::Int32 getAppName(taf::Int32 appId,std::string &appName,taf::JceCurrentPtr current);

    virtual taf::Int32 set(taf::Int32 appId,const std::string & sK,const std::string & sV,taf::JceCurrentPtr current);

    virtual taf::Int32 mset(taf::Int32 appId,const map<std::string, std::string> & mKVs,taf::JceCurrentPtr current);

    virtual taf::Int32 get(taf::Int32 appId,const std::string & sK,std::string &sV,taf::JceCurrentPtr current);

    virtual taf::Int32 mget(taf::Int32 appId,const vector<std::string> & vKs,map<std::string, std::string> &mKVs,taf::JceCurrentPtr current);

    virtual taf::Int32 incrby(taf::Int32 appId,const std::string & sK,taf::Int64 incr,taf::JceCurrentPtr current);

    virtual taf::Int32 append(taf::Int32 appId,const std::string & sK,const std::string & sV,taf::JceCurrentPtr current);

    virtual taf::Int32 del(taf::Int32 appId,const vector<std::string> & vKs,taf::Int32 &number,taf::JceCurrentPtr current);

    virtual taf::Int32 exists(taf::Int32 appId,const vector<std::string> & vKs,taf::Int32 &number,taf::JceCurrentPtr current);

};

#endif
