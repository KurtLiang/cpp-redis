#ifndef CRC64_H
#define CRC64_H

#ifdef __cplusplus
#include <cstdint>
#else
#include <stdint.h>
#endif

uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);

#ifdef REDIS_TEST
int crc64Test(int argc, char *argv[]);
#endif

#endif
