package org.example.service;

import org.example.dto.QueryResponse;
import org.example.dto.QueryStatus;
import org.example.repo.QueryRepo;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.FileInputStream;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QueryService {
    private final QueryRepo store;

    private final SqlGuard sqlGuard;
    private final RateLimiter rateLimiter;

    private final int maxPendingPerUser;
    private final int maxRunningPerUser;
    private final int maxRunningGlobal;

    private final BlockingQueue<QueryWorker.Job> queue;
    private final ExecutorService pool;
    private final QueryWorker queryWorker;

    public QueryService(
            QueryRepo store,
            SqlGuard sqlGuard,
            RateLimiter rateLimiter,
            int workerCount,
            int queueSize,
            int maxPendingPerUser,
            int maxRunningPerUser,
            int maxRunningGlobal,
            File resultsDir,
            int statementTimeoutMs,
            int fetchSize,
            long maxRows,
            long maxBytes
    ) {
        this.store = store;
        // Guard SQL safety
        this.sqlGuard = sqlGuard;
        // For per-user rate limits,and backpressure
        this.rateLimiter = rateLimiter;
        this.maxPendingPerUser = maxPendingPerUser;
        this.maxRunningPerUser = maxRunningPerUser;
        this.maxRunningGlobal = maxRunningGlobal;
        // Bounded queue and fixed worker pool
        this.queue = new ArrayBlockingQueue<>(queueSize);
        // Start background worker
        this.queryWorker = new QueryWorker(queue, store, resultsDir, statementTimeoutMs, fetchSize, maxRows, maxBytes);

        this.pool = Executors.newFixedThreadPool(workerCount);
        for (int i = 0; i < workerCount; i++) {
            pool.submit(queryWorker);
        }
    }
    // Validates SQL and also enforces rate/limit checks and enqueues it for async execution
    public QueryResponse submit(String userId, String sql, String idempotencyKey) throws Exception {
        sqlGuard.validate(sql);
        if (!rateLimiter.allow(userId)) {
            throw new WebApplicationException("rate limited", 429);
        }



        String idem = (idempotencyKey == null || idempotencyKey.trim().isEmpty())
                ? null
                : idempotencyKey.trim();

        if (idem != null) {
            QueryRepo.Row existing = store.byIdem(userId, idem).orElse(null);
            if (existing != null) return toResponse(existing);
        }

        if (store.countUser(userId, QueryStatus.PENDING) >= maxPendingPerUser) {
            throw new WebApplicationException("too many pending queries", 429);
        }
        if (store.countUser(userId, QueryStatus.RUNNING) >= maxRunningPerUser) {
            throw new WebApplicationException("too many running queries", 429);
        }
        if (store.countGlobal(QueryStatus.RUNNING) >= maxRunningGlobal) {
            throw new WebApplicationException("server busy", 429);
        }

        QueryRepo.Row row = new QueryRepo.Row();
        row.id = "q_" + UUID.randomUUID().toString().replace("-", "");
        row.userId = userId;
        row.idempotencyKey = idem;
        row.sql = sql;
        row.status = QueryStatus.PENDING;
        row.createdAt = Instant.now();

        store.insert(row);

        boolean enqueued = queue.offer(new QueryWorker.Job(row.id, userId));
        if (!enqueued) {
            store.fail(userId, row.id, Instant.now(), "queue full");
            throw new WebApplicationException("server busy", 429);
        }

        return toResponse(row);
    }
    //  Returns the current status
    public QueryResponse status(String userId, String id) throws Exception {
        QueryRepo.Row row = store.byId(userId, id).orElseThrow(() -> new WebApplicationException("not found", 404));
        return toResponse(row);
    }
    // Streams the completed query results
    public Response results(String userId, String id) throws Exception {
        QueryRepo.Row row = store.byId(userId, id).orElseThrow(() -> new WebApplicationException("not found", 404));

        if (row.status != QueryStatus.SUCCEEDED) {
            throw new WebApplicationException("not ready", 409);
        }
        if (row.resultPath == null) {
            throw new WebApplicationException("result missing", 500);
        }

        File file = new File(row.resultPath);
        if (!file.exists()) {
            throw new WebApplicationException("result file missing", 500);
        }

        StreamingOutput stream = out -> {
            try (FileInputStream in = new FileInputStream(file)) {
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) != -1) {
                    out.write(buf, 0, n);
                }
            }
        };

        return Response.ok(stream)
                .type("application/x-ndjson")
                .build();
    }
    // Cancels a pending or running query
    public QueryResponse cancel(String userId, String id) throws Exception {
        store.byId(userId, id).orElseThrow(() -> new WebApplicationException("not found", 404));
        store.cancel(userId, id, Instant.now());
        queryWorker.cancel(id);
        return status(userId, id);
    }
    // Maps  query state to response
    private static QueryResponse toResponse(QueryRepo.Row row) {
        QueryResponse resp = new QueryResponse();
        resp.id = row.id;
        resp.status = row.status;
        resp.createdAt = row.createdAt;
        resp.startedAt = row.startedAt;
        resp.endedAt = row.endedAt;
        resp.error = row.error;
        resp.rowsWritten = row.rowsWritten;
        resp.bytesWritten = row.bytesWritten;
        return resp;
    }
}
