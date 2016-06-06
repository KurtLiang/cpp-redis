/*
 * Redis on Taf, aka 'Rot'
 */
#ifndef __ROT_SERVER_H__
#define __ROT_SERVER_H__

#include "servant/Application.h"

class RotServer: public taf::Application
{
public:
    virtual ~RotServer() {};

    virtual void initialize();
    virtual void destroyApp();

};

extern RotServer g_app;

#endif

