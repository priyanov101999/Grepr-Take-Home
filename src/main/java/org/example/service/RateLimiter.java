package org.example.service;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
// Rate limiter
public class RateLimiter {
    private static class Bucket {
        int tokens;
        long lastSec;
        Bucket(int tokens, long lastSec) {
            this.tokens = tokens;
            this.lastSec = lastSec;
        }
    }

    private final int perMinute;
    private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    public RateLimiter(int perMinute) {
        this.perMinute = perMinute;
    }

    public boolean allow(String userId) {
        long now = Instant.now().getEpochSecond();
        // Create bucket per user
        Bucket bucket = buckets.computeIfAbsent(userId, k -> new Bucket(perMinute, now));
        // Synchronize for global rate limit
        synchronized (bucket) {
            // token count is reset after 60 sec
            if (now - bucket.lastSec >= 60) {
                bucket.tokens = perMinute;
                bucket.lastSec = now;
            }
            // No tokens available, then reject
            if (bucket.tokens <= 0) return false;
            // consumes 1 token
            bucket.tokens--;
            return true;
        }
    }
}
