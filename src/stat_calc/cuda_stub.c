#include "kernel.h"

#ifndef ENABLE_CUDA_BACKEND

static const char* k_err = "CUDA toolkit/runtime unavailable; CPU fallback active";

int cuda_get_device_count(int* count) {
    if (count) *count = 0;
    return 0;
}

int cuda_select_device(int device_id) {
    (void)device_id;
    return -1;
}

int cuda_init_context(void) {
    return -1;
}

int cuda_run_percentage_change_batch(
    const double* open_prices,
    const double* current_prices,
    double* out_pct,
    int n
) {
    (void)open_prices;
    (void)current_prices;
    (void)out_pct;
    (void)n;
    return -1;
}

const char* cuda_last_error_string(void) {
    return k_err;
}

void cuda_cleanup(void) {}

#endif
