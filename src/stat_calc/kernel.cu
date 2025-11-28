#include "kernel.h"
#include <cuda_runtime.h>
#include <device_launch_parameters.h>
#include <cmath>
#include <cstdio>
#include <cstring>

__global__ void percentage_change_kernel_batch(const GPUOHLCDataBatch_C *ohlc_batch, int num_symbols, GPUPercentageChangeResultBatch_C *pct_results) {
    int symbol_idx = blockIdx.x;
    if (symbol_idx >= num_symbols) return;

    int count = ohlc_batch->counts[symbol_idx];
    if (count < 2) return;

    if (threadIdx.x == 0) {
        float first_price = ohlc_batch->close_prices[symbol_idx][0];
        float last_price = ohlc_batch->close_prices[symbol_idx][count - 1];

        float pct_change = 0.0f;
        if (first_price > 0.000001f) {
            pct_change = ((last_price - first_price) / first_price) * 100.0f;
        }

        pct_results->percentage_change[symbol_idx] = pct_change;
        pct_results->current_price[symbol_idx] = last_price;
        pct_results->candle_open_price[symbol_idx] = first_price;
        pct_results->candle_timestamp[symbol_idx] = (long long)clock64();
    }
    __syncthreads();
}



static KernelError map_cuda_error(cudaError_t cuda_err, const char* context) {
    if (cuda_err == cudaSuccess) {
        return KERNEL_SUCCESS;
    }
    static char error_msg[256];
    snprintf(error_msg, sizeof(error_msg), "%s: %s", context, cudaGetErrorString(cuda_err));
    return { cuda_err, error_msg };
}

static KernelError launch_percentage_change_kernel_internal(
    const GPUOHLCDataBatch_C *d_ohlc_batch,
    GPUPercentageChangeResultBatch_C *d_pct_results,
    int num_symbols)
{
    const int THREADS_PER_BLOCK = 1;
    if (num_symbols > 0) {
        percentage_change_kernel_batch<<<num_symbols, THREADS_PER_BLOCK>>>(d_ohlc_batch, num_symbols, d_pct_results);
        cudaError_t err = cudaGetLastError();
        if (err != cudaSuccess) {
            return map_cuda_error(err, "CUDA percentage change kernel launch failed");
        }
    }
    return KERNEL_SUCCESS;
}



extern "C" {
    KernelError cuda_wrapper_init_device(int device_id) {
        cudaError_t err = cudaSetDevice(device_id);
        if (err != cudaSuccess) {
            return map_cuda_error(err, "Failed to set CUDA device");
        }
        return KERNEL_SUCCESS;
    }
    
    KernelError cuda_wrapper_reset_device() {
        cudaError_t err = cudaDeviceReset();
        if (err != cudaSuccess) {
            return map_cuda_error(err, "Device reset failed");
        }
        return KERNEL_SUCCESS;
    }
    
    KernelError cuda_wrapper_get_device_count(int* count) {
        cudaError_t err = cudaGetDeviceCount(count);
        if (err != cudaSuccess) {
            return map_cuda_error(err, "Failed to get device count");
        }
        return KERNEL_SUCCESS;
    }
    
    KernelError cuda_wrapper_get_device_properties(int device_id, struct cudaDeviceProp* props) {
        cudaError_t err = cudaGetDeviceProperties(props, device_id);
        if (err != cudaSuccess) {
            return map_cuda_error(err, "Failed to get device properties");
        }
        return KERNEL_SUCCESS;
    }
    
    KernelError cuda_wrapper_select_best_device(int* best_device_id_out) {
        int device_count = 0;
        cudaError_t err = cudaGetDeviceCount(&device_count);
        if (err != cudaSuccess) {
            return map_cuda_error(err, "Failed to get device count");
        }
        if (device_count == 0) {
            return KERNEL_ERROR_NO_DEVICE;
        }
        
        int best_device = 0;
        int max_compute_capability = 0;
        for (int i = 0; i < device_count; i++) {
            cudaDeviceProp props;
            err = cudaGetDeviceProperties(&props, i);
            if (err == cudaSuccess) {
                int current_compute_capability = props.major * 100 + props.minor;
                if (current_compute_capability > max_compute_capability) {
                    max_compute_capability = current_compute_capability;
                    best_device = i;
                }
            } else {
                return map_cuda_error(err, "Failed to get properties for device");
            }
        }
        *best_device_id_out = best_device;
        return KERNEL_SUCCESS;
    }
    
    KernelError cuda_wrapper_allocate_memory(
        GPUOHLCDataBatch_C **d_ohlc_batch,
        GPUPercentageChangeResultBatch_C **d_pct_result
    ) {
        cudaError_t err;

        err = cudaMalloc((void**)d_ohlc_batch, sizeof(GPUOHLCDataBatch_C));
        if (err != cudaSuccess) {
            return map_cuda_error(err, "CUDA Malloc failed for d_ohlc_batch");
        }
        err = cudaMemset(*d_ohlc_batch, 0, sizeof(GPUOHLCDataBatch_C));
        if (err != cudaSuccess) {
            return map_cuda_error(err, "CUDA Memset failed for d_ohlc_batch");
        }

        err = cudaMalloc((void**)d_pct_result, sizeof(GPUPercentageChangeResultBatch_C));
        if (err != cudaSuccess) {
            return map_cuda_error(err, "CUDA Malloc failed for d_pct_result");
        }
        err = cudaMemset(*d_pct_result, 0, sizeof(GPUPercentageChangeResultBatch_C));
        if (err != cudaSuccess) {
            return map_cuda_error(err, "CUDA Memset failed for d_pct_result");
        }

        return KERNEL_SUCCESS;
    }
    
    KernelError cuda_wrapper_free_memory(
        GPUOHLCDataBatch_C *d_ohlc_batch,
        GPUPercentageChangeResultBatch_C *d_pct_result
    ) {
        KernelError last_err = KERNEL_SUCCESS;
        cudaError_t current_err;

        if (d_ohlc_batch) {
            current_err = cudaFree(d_ohlc_batch);
            if (current_err != cudaSuccess) {
                last_err = map_cuda_error(current_err, "CUDA Free failed for d_ohlc_batch");
            }
        }

        if (d_pct_result) {
            current_err = cudaFree(d_pct_result);
            if (current_err != cudaSuccess && last_err.code == 0) {
                last_err = map_cuda_error(current_err, "CUDA Free failed for d_pct_result");
            }
        }

        return last_err;
    }
    
    KernelError cuda_wrapper_run_percentage_change_batch(
        GPUOHLCDataBatch_C *d_ohlc_batch_ptr,
        GPUPercentageChangeResultBatch_C *d_pct_results_ptr,
        const GPUOHLCDataBatch_C *h_ohlc_batch,
        GPUPercentageChangeResultBatch_C *h_pct_results,
        int num_symbols
    ) {
        if (num_symbols == 0) return KERNEL_SUCCESS;

        cudaError_t err;

        err = cudaMemcpy(d_ohlc_batch_ptr, h_ohlc_batch, sizeof(GPUOHLCDataBatch_C), cudaMemcpyHostToDevice);
        if (err != cudaSuccess) {
            return map_cuda_error(err, "CUDA Memcpy H2D failed for percentage change input");
        }

        KernelError kerr = launch_percentage_change_kernel_internal(d_ohlc_batch_ptr, d_pct_results_ptr, num_symbols);
        if (kerr.code != 0) {
            return kerr;
        }

        err = cudaDeviceSynchronize();
        if (err != cudaSuccess) {
            return map_cuda_error(err, "CUDA percentage change kernel execution failed");
        }

        err = cudaMemcpy(h_pct_results, d_pct_results_ptr, sizeof(GPUPercentageChangeResultBatch_C), cudaMemcpyDeviceToHost);
        if (err != cudaSuccess) {
            return map_cuda_error(err, "CUDA Memcpy D2H failed for percentage change results");
        }

        return KERNEL_SUCCESS;
    }
    
    
    KernelError cuda_wrapper_get_device_info(int device_id, DeviceInfo* info) {
        cudaDeviceProp prop;
        cudaError_t err = cudaGetDeviceProperties(&prop, device_id);
        if (err != cudaSuccess) {
            return (KernelError){ .code = 10, .message = "Failed to get device properties" };
        }
        
        strncpy(info->name, prop.name, 256);
        info->major = prop.major;
        info->minor = prop.minor;
        info->totalGlobalMem = prop.totalGlobalMem;
        return (KernelError){ .code = 0, .message = "Success" };
    }
}
