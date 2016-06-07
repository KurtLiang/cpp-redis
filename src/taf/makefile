
#-----------------------------------------------------------------------

APP       := Comm
TARGET    := RotServer
MFLAGS    := 64
CONFIG    :=
STRIP_FLAG:= N
J2CPP_FLAG:=

REDIS_PATH    := /home/liangyang/work/redis/redis-3.2.0/src
#REDIS_OBJS    := $(patsubst %.o, $(REDIS_PATH)/%.o,$(REDIS_BASEOBJS))
REDIS_LIB     :=  $(REDIS_PATH)/libredis.a
REDIS_DEP_LIB := ../../deps/jemalloc/lib/libjemalloc.a -ldl


#.PHONY: all

ifdef REDIS_LIB
LIB       += -lrt $(REDIS_LIB)  $(REDIS_DEP_LIB)
else
LIB       += -lrt $(REDIS_OBJS) $(REDIS_DEP_LIB)
endif

INCLUDE   += -I$(REDIS_PATH)

#-----------------------------------------------------------------------

include /usr/local/taf/makefile.taf

#-----------------------------------------------------------------------
