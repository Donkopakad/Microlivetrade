#ifndef KERNEL_H
#define KERNEL_H

#define MAX_SYMBOLS_CUDA 404
#define MAX_ORDERBOOK_SIZE 5

typedef struct {
    int code;
    const char* message;
} KernelError;

static const KernelError KERNEL_SUCCESS = {0, "Success"};
static const KernelError KERNEL_ERROR_INVALID_DEVICE = {1, "Invalid device ID"};
static const KernelError KERNEL_ERROR_NO_DEVICE = {2, "No CUDA devices found"};
static const KernelError KERNEL_ERROR_MEMORY_ALLOCATION = {3, "Memory allocation failed"};
static const KernelError KERNEL_ERROR_MEMORY_SET = {4, "Memory set failed"};
static const KernelError KERNEL_ERROR_MEMORY_FREE = {5, "Memory free failed"};
static const KernelError KERNEL_ERROR_MEMCPY = {6, "Memory copy failed"};
static const KernelError KERNEL_ERROR_KERNEL_LAUNCH = {7, "Kernel launch failed"};
static const KernelError KERNEL_ERROR_KERNEL_EXECUTION = {8, "Kernel execution failed"};
static const KernelError KERNEL_ERROR_DEVICE_RESET = {9, "Device reset failed"};
static const KernelError KERNEL_ERROR_GET_PROPERTIES = {10, "Failed to get device properties"};
static const KernelError KERNEL_ERROR_GET_DEVICE_COUNT = {11, "Failed to get device count"};

typedef struct {
    char name[256];
    int major;
    int minor;
    size_t totalGlobalMem;
} DeviceInfo;

struct GPUOHLCDataBatch_C {
    float close_prices[MAX_SYMBOLS_CUDA][15];
    unsigned int counts[MAX_SYMBOLS_CUDA];
};

struct GPUOrderBookDataBatch_C {
    float bid_prices[MAX_SYMBOLS_CUDA][MAX_ORDERBOOK_SIZE];
    float bid_quantities[MAX_SYMBOLS_CUDA][MAX_ORDERBOOK_SIZE];
    float ask_prices[MAX_SYMBOLS_CUDA][MAX_ORDERBOOK_SIZE];
    float ask_quantities[MAX_SYMBOLS_CUDA][MAX_ORDERBOOK_SIZE];
    unsigned int bid_counts[MAX_SYMBOLS_CUDA];
    unsigned int ask_counts[MAX_SYMBOLS_CUDA];
};

struct GPUPercentageChangeResultBatch_C {
    float percentage_change[MAX_SYMBOLS_CUDA];
    float current_price[MAX_SYMBOLS_CUDA];
    float candle_open_price[MAX_SYMBOLS_CUDA];
    long long candle_timestamp[MAX_SYMBOLS_CUDA];
};

extern "C" {
    KernelError cuda_wrapper_init_device(int device_id);
    KernelError cuda_wrapper_reset_device();
    KernelError cuda_wrapper_get_device_count(int* count);
    KernelError cuda_wrapper_get_device_info(int device_id, DeviceInfo* info);
    KernelError cuda_wrapper_select_best_device(int* best_device_id);

    KernelError cuda_wrapper_allocate_memory(
        struct GPUOHLCDataBatch_C **d_ohlc_batch,
        struct GPUPercentageChangeResultBatch_C **d_pct_result
    );

    KernelError cuda_wrapper_free_memory(
        struct GPUOHLCDataBatch_C *d_ohlc_batch,
        struct GPUPercentageChangeResultBatch_C *d_pct_result
    );

    KernelError cuda_wrapper_run_percentage_change_batch(
        struct GPUOHLCDataBatch_C *d_ohlc_batch_ptr,
        struct GPUPercentageChangeResultBatch_C *d_pct_results_ptr,
        const struct GPUOHLCDataBatch_C *h_ohlc_batch,
        struct GPUPercentageChangeResultBatch_C *h_pct_results,
        int num_symbols
    );
}

#endif
