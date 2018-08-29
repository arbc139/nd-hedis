#ifdef TODIS
#include "server.h"
#include "obj.h"
#include "libpmemobj.h"
#include "util.h"

#include <time.h>

void emulateReadLatency(void) {
    struct timespec base = nstimespec();
    base.tv_nsec += server.pm_read_latency;
    int count = 0;
    while (nstimeCompare(base, nstimespec()) == 1) {
        count++;
    }
    // serverLog(LL_NOTICE, "read latency loop: %d", count);
}

void emulateWriteLatency(void) {
    struct timespec base = nstimespec();
    base.tv_nsec += server.pm_write_latency;
    int count = 0;
    while (nstimeCompare(base, nstimespec()) == 1) {
        count++;
    }
    // serverLog(LL_NOTICE, "write latency loop: %d", count);
}

void *pmemobj_direct_latency(PMEMoid oid) {
    emulateReadLatency();
    return pmemobj_direct(oid);
}

PMEMoid pmemobj_tx_zalloc_latency(size_t size, uint64_t type_num) {
    emulateWriteLatency();
    return pmemobj_tx_zalloc(size, type_num);
}

int pmemobj_tx_free_latency(PMEMoid oid) {
    emulateWriteLatency();
    return pmemobj_tx_free(oid);
}
#endif