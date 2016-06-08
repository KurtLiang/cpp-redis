/*
 * Redis on Taf, aka 'Rot'
 */
#ifndef __ROT_SERVER_H__
#define __ROT_SERVER_H__

#include "servant/Application.h"
#include <map>

#ifndef PROC_BEGIN
#define PROC_BEGIN do{
#define PROC_END   }while(0);
#define PROC_BREAK break;
#endif


struct redisDb;

class RotServer: public taf::Application
{
public:
    virtual ~RotServer() {};

    virtual void initialize();
    virtual void destroyApp();

    std::string lookforAppName(taf::Int32 appId)
    {
        auto it = appid2name_.find(appId);
        if (it == appid2name_.end())
            return  "";

        return it->second;
    }

    redisDb* lookforDb(taf::Int32 appId);

private:
    int initRedis(TC_Config &tConf);

private:
    std::map<int, std::string> appid2name_;
};

extern RotServer g_app;

#endif

