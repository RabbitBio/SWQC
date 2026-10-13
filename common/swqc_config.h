#ifndef SWQC_CONFIG_H
#define SWQC_CONFIG_H

#define SWQC_SINGLE 0
#define SWQC_CGS 1
#define SWQC_CROSS 2
#ifndef SWQC_EXEC_MODE
#define SWQC_EXEC_MODE SWQC_SINGLE
#endif
#ifndef SWQC_POOL
#define SWQC_POOL 0
#endif
#ifndef SWQC_LDM_POOL
#define SWQC_LDM_POOL 0
#endif
#ifndef SWQC_MANUAL_LDM
#define SWQC_MANUAL_LDM 1
#endif
#ifndef SWQC_LDM_POOL_BYTES
#define SWQC_LDM_POOL_BYTES (32 << 10)
#endif
#if SWQC_LDM_POOL && !SWQC_POOL
#error "CPE_LDM_POOL requires CPE_ALLOCATOR=pool"
#endif
#if SWQC_LDM_POOL_BYTES < 256 || SWQC_LDM_POOL_BYTES > (56 << 10) || SWQC_LDM_POOL_BYTES % 64
#error "LDM_POOL_BYTES must be a multiple of 64 in [256, 57344]"
#endif
#if SWQC_EXEC_MODE == SWQC_SINGLE
#define SWQC_CPE_COUNT 64
#else
#define SWQC_CPE_COUNT 384
#endif

#endif
