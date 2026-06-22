# Data_Aggregation_using_Neutrino_RTOS
### Component Breakdown
1. **The Producer (Main Thread):** Simulates a high-frequency embedded sensor framework. It generates large continuous data blocks (64KB+) on the system heap and posts them to the pipeline.
2. **The Shared Pipeline (Circular FIFO Queue):** A fixed-capacity ring buffer tracking data packets via `head` and `tail` indices. This prevents dynamic structural overhead during runtime.
3. **The Worker Pool (Consumer Threads):** A configurable array of parallel execution contexts that pull data references out of the queue, process mathematical metrics (moving averages), and aggregate them globally.

---

## 3. Core Technical Challenges & Solutions

### A. Heap vs. Stack Allocation (Fixing Memory Failures)
In standard desktop operating systems, declaring large arrays locally inside functions is common. However, QNX thread stacks are strictly constrained by default to maintain a low memory footprint.
* **The Vulnerability:** Declaring `float data[16384]` inside a thread function allocates 64KB directly onto the thread stack, causing instantaneous **Stack Overflow / Core Dumps**.
* **The Mitigation:** This implementation forces strict **Heap Allocation** using `malloc()`. The 64KB block resides in system heap space, and only its lightweight 8-byte memory pointer (`data_ptr`) is passed through the synchronization queue.

### B. Double-Semaphore Flow Control (Preventing Saturation)
High-frequency sensors can easily overwhelm processing threads if a sudden compute spike occurs. Without flow control, the producer would continue allocating 64KB chunks endlessly, depleting system RAM.

We implement **Backpressure** using two complementary counting semaphores:
* `sem_empty` (Initialized to `MAX_QUEUE`): Tracks available slots in the ring buffer. The producer must call `sem_wait(&sem_empty)` before generating data. If the queue fills up, the producer safely blocks, yielding CPU cycles.
* `sem_filled` (Initialized to `0`): Tracks unread data payloads. Worker threads call `sem_wait(&sem_filled)` and remain completely asleep until the producer signals that new data has dropped.

### C. Lock Scoping & Parallel Efficiency
To maximize multi-core CPU utilization, the mutex containment zones are designed to be as small as possible:
1. **The Dequeue Lock:** A worker locks `g_aggregator.mtx` solely to copy the memory pointer out of the queue array and advance the `tail` index. 
2. **Lock-Free Processing:** The worker immediately **unlocks** the mutex before starting the intensive loop over the 16,384 floats. This allows all four worker threads to crunch data simultaneously without cross-blocking.
3. **The Aggregate Lock:** The worker acquires the lock a second time briefly to update the final global moving average and safely execute `free()`.
