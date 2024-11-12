package implementations.java.rate_limiting;

import java.time.Instant;
import java.util.LinkedList;
import java.util.Queue;

public class LeakyBucket {
    private final long capacity;        // Maximum number of requests the bucket can hold
    private final double leakRate;      // Rate at which requests leak out of the bucket (requests per second)
    private final Queue<Request> bucket; // Queue to hold timestamps of requests
    private Instant lastLeakTimestamp;   // Last time we leaked from the bucket

    public LeakyBucket(long capacity, double leakRate) {
        this.capacity = capacity;
        this.leakRate = leakRate;
        this.bucket = new LinkedList<>();
        this.lastLeakTimestamp = Instant.now();
    }

    public synchronized boolean allowRequest(Request request) {
        //leak();  // First, leak out any requests based on elapsed time

        if (bucket.size() < capacity) {
            bucket.offer(request);  // Add the new request to the bucket
            return true;  // Allow the request
        }
        return false;  // Bucket is full, deny the request
    }

    //@EnableScheduleing at Spring boot application class
    //@Scheduled(fixedRate = 5000)
    //@Schduled(each 1 min)
    //Another thread removes from Q & processes requests. If leakRate is 4 per min, then it's scheduled every 15 secs, and processes a request
    //But Below is sample impl of processing from Q, whenever allowRequest() is called
    private void leak() {
        Instant now = Instant.now();
        long elapsedMillis = now.toEpochMilli() - lastLeakTimestamp.toEpochMilli();
        int leakedItems = (int) (elapsedMillis * leakRate / 1000.0);  // Calculate how many items should have leaked

        // Remove the leaked items from the bucket
        for (int i = 0; i < leakedItems && !bucket.isEmpty(); i++) {
            processRequest(bucket.poll());
        }

        lastLeakTimestamp = now;
    }
}
