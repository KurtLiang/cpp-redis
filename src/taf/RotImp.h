#ifndef __ROT_IMP_H__
#define __ROT_IMP_H__

#include "Rot.h"

class RotImp: public Comm::Rot
{
public:
    virtual ~RotImp() {}
    virtual void initialize();
    virtual void destroy();

    virtual taf::Int32 setItem(const std::string & sApp,const std::string & sK,const std::string & sV,taf::JceCurrentPtr current) = 0;
    virtual taf::Int32 getItem(const std::string & sApp,const std::string & sK,std::string &sV,taf::JceCurrentPtr current) = 0;
    virtual taf::Int32 delItem(const std::string & sApp,const std::string & sK,taf::JceCurrentPtr current) = 0;
    virtual taf::Int32 setItemBatch(const std::string & sApp,const map<std::string, std::string> & mKVs,taf::JceCurrentPtr current) = 0;
    virtual taf::Int32 getItemBatch(const std::string & sApp,const vector<std::string> & vKs,map<std::string, std::string> &mKVs,taf::JceCurrentPtr current) = 0;
    virtual taf::Int32 delItemBatch(const std::string & sApp,const vector<std::string> & vKs,map<std::string, taf::Int32> &mKVs,taf::JceCurrentPtr current) = 0;
};

#endif
