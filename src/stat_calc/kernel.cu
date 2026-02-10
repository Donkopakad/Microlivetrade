#include "kernel.h"

#include <cuda_runtime.h>

#include <cstdio>

namespace {
__global__ void percentage_change_kernel(const double* open_prices, const double* current_prices, double* out_pct, int n) {
    int idx = (blockIdx.x * blockDim.x) + threadIdx.x;
    if (idx >= n) return;

    const double open = open_prices[idx];
    const double current = current_prices[idx];
    out_pct[idx] = (open != 0.0) ? (((current - open) / open) * 100.0) : 0.0;
}

char g_last_error[256] = "Success";
int g_selected_device = 0;
double* g_d_open = nullptr;
double* g_d_current = nullptr;
double* g_d_out = nullptr;
int g_capacity = 0;

int set_last_error(cudaError_t err, const char* context) {
    std::snprintf(g_last_error, sizeof(g_last_error), "%s: %s", context, cudaGetErrorString(err));
    return static_cast<int>(err);
}

int ensure_capacity(int n) {
    if (n <= g_capacity) return 0;

    if (g_d_open) cudaFree(g_d_open);
    if (g_d_current) cudaFree(g_d_current);
    if (g_d_out) cudaFree(g_d_out);

    g_d_open = nullptr;
    g_d_current = nullptr;
    g_d_out = nullptr;
    g_capacity = 0;

    cudaError_t err = cudaMalloc(reinterpret_cast<void**>(&g_d_open), static_cast<size_t>(n) * sizeof(double));
    if (err != cudaSuccess) return set_last_error(err, "cudaMalloc(open)");

    err = cudaMalloc(reinterpret_cast<void**>(&g_d_current), static_cast<size_t>(n) * sizeof(double));
    if (err != cudaSuccess) return set_last_error(err, "cudaMalloc(current)");

    err = cudaMalloc(reinterpret_cast<void**>(&g_d_out), static_cast<size_t>(n) * sizeof(double));
    if (err != cudaSuccess) return set_last_error(err, "cudaMalloc(out)");

    g_capacity = n;
    return 0;
}
} // namespace

extern "C" {
int cuda_get_device_count(int* count) {
    if (!count) {
        std::snprintf(g_last_error, sizeof(g_last_error), "cuda_get_device_count: null count");
        return -1;
    }

    cudaError_t err = cudaGetDeviceCount(count);
    if (err != cudaSuccess) {
        *count = 0;
        return set_last_error(err, "cudaGetDeviceCount");
    }

    std::snprintf(g_last_error, sizeof(g_last_error), "Success");
    return 0;
}

int cuda_select_device(int device_id) {
    cudaError_t err = cudaSetDevice(device_id);
    if (err != cudaSuccess) return set_last_error(err, "cudaSetDevice");

    g_selected_device = device_id;
    std::snprintf(g_last_error, sizeof(g_last_error), "Success");
    return 0;
}

int cuda_init_context(void) {
    cudaError_t err = cudaSetDevice(g_selected_device);
    if (err != cudaSuccess) return set_last_error(err, "cudaSetDevice(init)");

    err = cudaFree(nullptr);
    if (err != cudaSuccess) return set_last_error(err, "cudaFree(nullptr)");

    std::snprintf(g_last_error, sizeof(g_last_error), "Success");
    return 0;
}

int cuda_run_percentage_change_batch(
    const double* open_prices,
    const double* current_prices,
    double* out_pct,
    int n
) {
    if (n <= 0) {
        std::snprintf(g_last_error, sizeof(g_last_error), "Success");
        return 0;
    }

    if (!open_prices || !current_prices || !out_pct) {
        std::snprintf(g_last_error, sizeof(g_last_error), "cuda_run_percentage_change_batch: null buffer");
        return -2;
    }

    int cap_err = ensure_capacity(n);
    if (cap_err != 0) return cap_err;

    cudaError_t err = cudaMemcpy(g_d_open, open_prices, static_cast<size_t>(n) * sizeof(double), cudaMemcpyHostToDevice);
    if (err != cudaSuccess) return set_last_error(err, "cudaMemcpy H2D open");

    err = cudaMemcpy(g_d_current, current_prices, static_cast<size_t>(n) * sizeof(double), cudaMemcpyHostToDevice);
    if (err != cudaSuccess) return set_last_error(err, "cudaMemcpy H2D current");

    constexpr int threads_per_block = 256;
    const int blocks = (n + threads_per_block - 1) / threads_per_block;
    percentage_change_kernel<<<blocks, threads_per_block>>>(g_d_open, g_d_current, g_d_out, n);

    err = cudaGetLastError();
    if (err != cudaSuccess) return set_last_error(err, "kernel launch");

    err = cudaDeviceSynchronize();
    if (err != cudaSuccess) return set_last_error(err, "cudaDeviceSynchronize");

    err = cudaMemcpy(out_pct, g_d_out, static_cast<size_t>(n) * sizeof(double), cudaMemcpyDeviceToHost);
    if (err != cudaSuccess) return set_last_error(err, "cudaMemcpy D2H out");

    std::snprintf(g_last_error, sizeof(g_last_error), "Success");
    return 0;
}

const char* cuda_last_error_string(void) {
    return g_last_error;
}

void cuda_cleanup(void) {
    if (g_d_open) cudaFree(g_d_open);
    if (g_d_current) cudaFree(g_d_current);
    if (g_d_out) cudaFree(g_d_out);

    g_d_open = nullptr;
    g_d_current = nullptr;
    g_d_out = nullptr;
    g_capacity = 0;
}
}
