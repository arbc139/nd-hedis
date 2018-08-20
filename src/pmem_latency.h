/*
 * Copyright (c) 2017, Andreas Bluemle <andreas dot bluemle at itxperts dot de>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __PMEM_LATENCY_H
#define __PMEM_LATENCY_H

#include "obj.h"
#include "libpmemobj.h"

#ifdef USE_ND
void emulateReadLatency(void);
void emulateWriteLatency(void);

void *pmemobj_direct_latency(PMEMoid oid);
PMEMoid pmemobj_tx_zalloc_latency(size_t size, uint64_t type_num);
int pmemobj_tx_free_latency(PMEMoid oid);

#define D_RW_LATENCY(o) ({\
    emulateReadLatency();\
    D_RW(o);\
})
#define D_RO_LATENCY(o) ({\
    emulateReadLatency();\
    D_RO(o);\
})
#define TX_ADD_DIRECT_LATENCY(o) ({\
    emulateWriteLatency();\
    TX_ADD_DIRECT(o);\
})
#define TX_FREE_LATENCY(o) ({\
    emulateWriteLatency();\
    TX_FREE(o);\
})
#define TX_ADD_FIELD_DIRECT_LATENCY(o, field) ({\
    emulateWriteLatency();\
    TX_ADD_FIELD_DIRECT(o, field);\
})
#endif

#endif