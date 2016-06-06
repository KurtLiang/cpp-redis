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




/* portal for redis node integrated with TAF */
int taf_main(int argc, char *argv[])
{
    std::cout << "hello world" << std::endl;
    return 0;
}
