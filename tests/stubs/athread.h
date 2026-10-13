#ifndef SWQC_TEST_ATHREAD_STUB_H
#define SWQC_TEST_ATHREAD_STUB_H
int __real_athread_spawn(void *, void *, int);
int athread_join(void);
#endif
