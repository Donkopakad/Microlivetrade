#ifndef KERNEL_H
#define KERNEL_H

#ifdef __cplusplus
extern "C" {
#endif

int cuda_get_device_count(int* count);
int cuda_select_device(int device_id);
int cuda_init_context(void);
int cuda_run_percentage_change_batch(
    const double* open_prices,
    const double* current_prices,
    double* out_pct,
    int n
);
const char* cuda_last_error_string(void);
void cuda_cleanup(void);

#ifdef __cplusplus
}
#endif

#endif
