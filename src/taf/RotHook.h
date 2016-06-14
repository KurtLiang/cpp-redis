#ifndef __TAF_HOOK_H__
#define __TAF_HOOK_H__

#define TAF_API extern

#ifdef __cplusplus
extern "C" {
#endif


#define TAF_HOOK_DONE       0
#define TAF_HOOK_CONTINUE   1

/*
 *
 */
typedef int (*logSinkerHook)(int, const char *);


#ifdef __cplusplus
}
#endif

#endif
