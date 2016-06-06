/*
 * Redis on Taf, aka 'Rot'
 */
#include "RotServer.h"
#include "RedisOnTaf.h"
#include "RotImp.h"
#include <iostream>

RotServer g_app;

void RotServer::initialize()
{
    //TODO
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
