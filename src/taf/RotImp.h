#ifndef __ROT_IMP_H__
#define __ROT_IMP_H__

#include "Rot.h"

struct redisObject;

class RotImp: public Comm::Rot
{
public:
    virtual ~RotImp() {}

    virtual void initialize();

    virtual void destroy();

    virtual taf::Int32 getAppName(taf::Int32 appId,std::string &appName,taf::JceCurrentPtr current);

    virtual taf::Int32 set(taf::Int32 appId,const std::string & sK,const std::string & sV, const Comm::StringRobjOption &opt, taf::JceCurrentPtr current);

    virtual taf::Int32 mset(taf::Int32 appId,const map<std::string, std::string> & mKVs, const Comm::StringRobjOption &opt, taf::JceCurrentPtr current);

    virtual taf::Int32 get(taf::Int32 appId,const std::string & sK,std::string &sV,taf::JceCurrentPtr current);

    virtual taf::Int32 mget(taf::Int32 appId,const vector<std::string> & vKs,map<std::string, std::string> &mKVs,taf::JceCurrentPtr current);

    virtual taf::Int32 incrby(taf::Int32 appId,const std::string & sK,taf::Int64 incr, taf::Int64 &result, taf::JceCurrentPtr current);

    virtual taf::Int32 append(taf::Int32 appId,const std::string & sK,const std::string & sV,taf::JceCurrentPtr current);

    virtual taf::Int32 push(taf::Int32 appId,const std::string & sK,const vector<std::string> & vItems,Comm::EListDirection dir,const Comm::ListRobjOption & opt,taf::JceCurrentPtr current);

    virtual taf::Int32 pop(taf::Int32 appId,const std::string & sK,Comm::EListDirection dir,std::string &sItem,taf::JceCurrentPtr current);

    virtual taf::Int32 lrange(taf::Int32 appId,const std::string & sK,taf::Int32 start,taf::Int32 end,vector<std::string> &vItems,taf::JceCurrentPtr current);

    virtual taf::Int32 hmset(taf::Int32 appId,const std::string & sK,const map<std::string, std::string> & mFV,taf::JceCurrentPtr current);

    virtual taf::Int32 hmget(taf::Int32 appId,const std::string & sK,const vector<std::string> & vFields,map<std::string, std::string> &mFV,taf::JceCurrentPtr current);

    virtual taf::Int32 hexists(taf::Int32 appId,const std::string & sK,const std::string & sField,taf::Int32 &exist_res,taf::JceCurrentPtr current);

    virtual taf::Int32 hdel(taf::Int32 appId,const std::string & sK,const vector<std::string> & vFields,taf::JceCurrentPtr current);

    virtual taf::Int32 sadd(taf::Int32 appId,const std::string & sK,const vector<std::string> & vMembers,taf::JceCurrentPtr current);

    virtual taf::Int32 srem(taf::Int32 appId,const std::string & sK,const vector<std::string> & vMembers,taf::JceCurrentPtr current);

    virtual taf::Int32 smembers(taf::Int32 appId,const std::string & sK,vector<std::string> &vMembers,taf::JceCurrentPtr current);

    virtual taf::Int32 sinter(taf::Int32 appId,const vector<std::string> & vK,vector<std::string> &vResults,taf::JceCurrentPtr current);

    virtual taf::Int32 sdiff(taf::Int32 appId,const vector<std::string> & vK,vector<std::string> &vResults,taf::JceCurrentPtr current);

    virtual taf::Int32 sunion(taf::Int32 appId,const vector<std::string> & vK,vector<std::string> &vResults,taf::JceCurrentPtr current);

    virtual taf::Int32 zadd(taf::Int32 appId,const std::string & sK,const vector<Comm::ZsetScoreMember> & vScoreMember,const Comm::ZsetRobjOption & option,taf::JceCurrentPtr current);

    virtual taf::Int32 zrem(taf::Int32 appId,const std::string & sK,const vector<std::string> & vMembers,taf::JceCurrentPtr current);

    virtual taf::Int32 zrank(taf::Int32 appId,const std::string & sK,const std::string & sMember,taf::Int32 rev,taf::Int32 &rank,taf::JceCurrentPtr current);

    virtual taf::Int32 zincrby(taf::Int32 appId,const std::string & sK,taf::Double increment,const std::string & sMember,taf::Double &new_score,taf::JceCurrentPtr current);

    virtual taf::Int32 zrange(taf::Int32 appId,const std::string & sK,taf::Int32 start,taf::Int32 stop,const Comm::ZsetRangeOption & opt,vector<Comm::ZsetScoreMember> &vScoreMembers,taf::JceCurrentPtr current);

    virtual taf::Int32 del(taf::Int32 appId,const vector<std::string> & vKs,taf::Int32 &number,taf::JceCurrentPtr current);

    virtual taf::Int32 exists(taf::Int32 appId,const vector<std::string> & vKs,taf::Int32 &number,taf::JceCurrentPtr current);

    virtual taf::Int32 expire(taf::Int32 appId,const std::string & sK,taf::Int64 seconds,taf::JceCurrentPtr current);

    virtual taf::Int32 ttl(taf::Int32 appId,const std::string & sK,taf::Int64 &seconds,taf::JceCurrentPtr current);

protected:
    virtual int onDispatch(taf::JceCurrentPtr _current, vector<char> &_sResponseBuffer);

private:
    redisObject* copyGetSharedKey(const string &s); /* return NULL if error */

private:
    redisObject* shr_key_;
};

#endif
