package implementations.java.rate_limiting;

import java.time.Instant;

public class TokenBucket {
    private final long capacity;        // Maximum number of tokens the bucket can hold
    private final double fillRate;      // Rate at which tokens are added to the bucket (tokens per second)
    private double tokens;              // Current number of tokens in the bucket
    private Instant lastRefillTimestamp; // Last time we refilled the bucket

    public TokenBucket(long capacity, double fillRate) {
        this.capacity = capacity;
        this.fillRate = fillRate;
        this.tokens = capacity;  // Start with a full bucket
        this.lastRefillTimestamp = Instant.now();
    }

    public synchronized boolean allowRequest() {
        //refill();  // First, add any new tokens based on elapsed time

        if (this.tokens <= 0) {
            return false;  // Not enough tokens, deny the request
        }

        --this.tokens;  // Consume the tokens
        return true;  // Allow the request
    }

    @Scheduled(fixedRate = 60000)//millisecs
    //Another Thread Refills/Resets Bucket, fillRate tokens per second or min.
    //But below logic is to refill whenever any request comes, thus decide "tokensToAdd" based on last refill TimeStamp
    private void refill() {
        Instant now = Instant.now();
        // Calculate how many tokens to add based on the time elapsed
        double tokensToAdd = (now.toEpochMilli() - lastRefillTimestamp.toEpochMilli()) * fillRate / 1000.0;
        this.tokens = Math.min(capacity, this.tokens + tokensToAdd);  // Add tokens, but don't exceed capacity
        this.lastRefillTimestamp = now;
    }
}
