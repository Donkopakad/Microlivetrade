// C:\Users\User\bolt-changes\src\stubs.c
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Pretend CUDA is present & working (CPU-backed stubs).

__declspec(dllexport) int cuda_wrapper_select_best_device(void) {
    return 0; // device 0 "selected"
}

__declspec(dllexport) int cuda_wrapper_init_device(int device_id) {
    (void)device_id;
    return 0; // success
}

__declspec(dllexport) int cuda_wrapper_get_device_info(char* buf, int len) {
    if (buf && len > 0) {
        const char* info = "Stub CUDA (CPU), CC=0.0, RAM=0 MB";
        // ensure NUL-termination
        strncpy(buf, info, (size_t)len - 1);
        buf[len - 1] = '\0';
    }
    return 0; // success
}

__declspec(dllexport) void* cuda_wrapper_allocate_memory(size_t bytes) {
    // just allocate CPU memory; caller treats it like device ptr
    if (bytes == 0) return NULL;
    void* p = malloc(bytes);
    return p;
}

__declspec(dllexport) int cuda_wrapper_run_percentage_change_batch(void) {
    // no-op; pretend kernel ran OK
    return 0;
}

__declspec(dllexport) void cuda_wrapper_free_memory(void* p) {
    if (p) free(p);
}

__declspec(dllexport) void cuda_wrapper_reset_device(void) {
    // no-op
}

// SIMD analyzer stub: pretend success
__declspec(dllexport) int analyze_trading_signals_with_liquidity_simd(void) {
    return 0;
}