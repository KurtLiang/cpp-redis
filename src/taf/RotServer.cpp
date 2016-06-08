/*
 * Redis on Taf, aka 'Rot'
 */
#include "RotServer.h"
#include "RedisOnTaf.h"
#include "RotImp.h"
#include <iostream>

RotServer g_app;

/**
 * @return 0 on success otherwise -1.
 */
int initRedis(TC_Config &tConf)
{
    //TODO


    return 0;
}

void RotServer::initialize()
{
    //initialize application here

    string sConf = ServerConfig::ServerName + ".conf";
    addConfig(sConf);

    TC_Config tConf;
    tConf.parseFile(ServerConfig::BasePath + sConf);

    int iret = initRedis(tConf);
    if (iret)
    {
        LOG->error() << "failed to init redis instance" << endl;
    }
}

void RotServer::destroyApp()
{
    //TODO
}




int main(int argc, char *argv[])
{
    try
    {
        g_app.main(argc, argv);
        g_app.waitForShutdown();
    }
    catch(std::exception &e)
    {
        std::cerr << "std::exception:" << e.what() << std::endl;
    }
    catch(...)
    {
        std::cerr << "unknown exception." << std::endl;
    }

    return -1;
}
