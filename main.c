#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/neutrino.h>

#define NUM_WORKERS 4
#define BUFFER_SIZE (64 * 1024) // 64KB
#define MAX_QUEUE 5

typedef struct {
    float *data_ptr;
    size_t size;
} data_packet_t;

// Context structure to keep global namespace clean
struct {
    data_packet_t queue[MAX_QUEUE];
    int head, tail;
    
    pthread_mutex_t mtx;
    sem_t sem_filled;   // Counts items ready to process
    sem_t sem_empty;    // Counts available slots (prevents "Huge Data" overflow)
    
    float global_result;
} g_aggregator;

void* processing_worker(void* arg) {
    while (1) {
        // 1. Wait for data to be available
        sem_wait(&g_aggregator.sem_filled);
        
        pthread_mutex_lock(&g_aggregator.mtx);
        data_packet_t task = g_aggregator.queue[g_aggregator.tail];
        g_aggregator.tail = (g_aggregator.tail + 1) % MAX_QUEUE;
        pthread_mutex_unlock(&g_aggregator.mtx);

        // 2. Parallel Processing (The "Work")
        float local_sum = 0;
        int count = task.size / sizeof(float);
        for (int i = 0; i < count; i++) {
            local_sum += task.data_ptr[i];
        }

        pthread_mutex_lock(&g_aggregator.mtx);
        g_aggregator.global_result = (g_aggregator.global_result + (local_sum / count)) / 2.0f;
        pthread_mutex_unlock(&g_aggregator.mtx);

        // 3. FREE the huge buffer and signal that the queue slot is empty
        free(task.data_ptr);
        sem_post(&g_aggregator.sem_empty);
    }
    return NULL;
}

int main() {
    // Initialize Synchronization
    pthread_mutex_init(&g_aggregator.mtx, NULL);
    sem_init(&g_aggregator.sem_filled, 0, 0); 
    sem_init(&g_aggregator.sem_empty, 0, MAX_QUEUE); // Start with all slots empty

    // Spawn Workers
    pthread_t workers[NUM_WORKERS];
    for (int i = 0; i < NUM_WORKERS; i++) {
        pthread_create(&workers[i], NULL, processing_worker, NULL);
    }

    printf("QNX Aggregator Running... Handling 64KB chunks.\n");

    while (1) {
        // 1. Wait for a free slot in the queue (Flow Control)
        sem_wait(&g_aggregator.sem_empty);

        // 2. Allocate 64KB on the HEAP (Avoids Stack Overflow)
        float *sensor_readings = malloc(BUFFER_SIZE);
        if (!sensor_readings) {
            sem_post(&g_aggregator.sem_empty);
            continue;
        }

        // Simulate filling 64KB of data
        memset(sensor_readings, 0, BUFFER_SIZE); 

        // 3. Push to queue
        pthread_mutex_lock(&g_aggregator.mtx);
        g_aggregator.queue[g_aggregator.head].data_ptr = sensor_readings;
        g_aggregator.queue[g_aggregator.head].size = BUFFER_SIZE;
        g_aggregator.head = (g_aggregator.head + 1) % MAX_QUEUE;
        pthread_mutex_unlock(&g_aggregator.mtx);

        sem_post(&g_aggregator.sem_filled);
    }

    return 0;
}
