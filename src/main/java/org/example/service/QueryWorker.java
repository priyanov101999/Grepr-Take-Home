package org.example.service;

import org.example.repo.QueryRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Instant;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
// Worker for executing  SQL queries asynchronously
public class QueryWorker implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(QueryWorker.class);

    public static class Job {
        public final String id;
        public final String userId;

        public Job(String id, String userId) {
            this.id = id;
            this.userId = userId;
        }
    }

    private final BlockingQueue<Job> queue;
    private final QueryRepo store;
    private final File resultsDir;

    private final int statementTimeoutMs;
    private final int fetchSize;
    private final long maxRows;
    private final long maxBytes;
    // Tracks currently executing JDBC statements
    private final ConcurrentHashMap<String, Statement> liveStatements = new ConcurrentHashMap<>();

    public QueryWorker(
            BlockingQueue<Job> queue,
            QueryRepo store,
            File resultsDir,
            int statementTimeoutMs,
            int fetchSize,
            long maxRows,
            long maxBytes
    ) {
        this.queue = queue;
        this.store = store;
        this.resultsDir = resultsDir;
        this.statementTimeoutMs = statementTimeoutMs;
        this.fetchSize = fetchSize;
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
    }

    public void cancel(String queryId) {
        Statement st = liveStatements.remove(queryId);
        if (st == null) return;

        try {
            st.cancel();
        } catch (Exception e) {
            LOG.warn("Cancel failed queryId={}", queryId, e);
        }
    }
    // Blocks on the queue
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Job job = queue.take();
                runOne(job);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                LOG.error("Worker loop error", e);
            }
        }
    }

    private void runOne(Job job) throws Exception {
        Instant startedAt = Instant.now();
        if (!store.pendingToRunning(job.userId, job.id, startedAt)) {
            return;
        }

        QueryRepo.Row row = store.byId(job.userId, job.id).orElse(null);
        if (row == null) return;
        // Stream query results to disk
        File outFile = new File(resultsDir, job.id + ".ndjson");
        long rows = 0;
        long bytes = 0;

        try (Connection c = store.conn()) {
            c.setAutoCommit(false);

            try (Statement st = c.createStatement()) {
                st.execute("set local statement_timeout = " + statementTimeoutMs);
            } catch (Exception ignore) {
            }
            // Executes the query with fetch size, row & byte limits
            try (PreparedStatement ps = c.prepareStatement(row.sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                ps.setFetchSize(fetchSize);
                liveStatements.put(job.id, ps);

                try (ResultSet rs = ps.executeQuery();
                     BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(outFile))) {

                    ResultSetMetaData md = rs.getMetaData();
                    int cols = md.getColumnCount();

                    while (rs.next()) {
                        rows++;
                        if (rows > maxRows) throw new RuntimeException("row limit exceeded");

                        String line = toNdjson(rs, md, cols);
                        byte[] data = line.getBytes(StandardCharsets.UTF_8);
                        bytes += data.length;

                        if (bytes > maxBytes) throw new RuntimeException("byte limit exceeded");

                        out.write(data);
                    }

                    out.flush();
                    c.commit();
                    store.succeed(job.userId, job.id, Instant.now(), outFile.getAbsolutePath(), rows, bytes);
                } finally {
                    liveStatements.remove(job.id);
                }
            }
        } catch (Exception e) {
            liveStatements.remove(job.id);
            try { outFile.delete(); } catch (Exception ignored) {}
            store.fail(job.userId, job.id, Instant.now(), safeMessage(e));
        }
    }
    // Serializes a single JDBC row into NDJSON format
    private static String toNdjson(ResultSet rs, ResultSetMetaData md, int cols) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (int i = 1; i <= cols; i++) {
            if (i > 1) sb.append(",");
            sb.append("\"").append(escape(md.getColumnLabel(i)))
                    .append("\":")
                    .append(asJson(rs.getObject(i)));
        }
        sb.append("}\n");
        return sb.toString();
    }

    private static String escape(String s) {

        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }

    private static String asJson(Object v) {
        if (v == null) return "null";
        if (v instanceof Number || v instanceof Boolean) return v.toString();
        return "\"" + escape(String.valueOf(v)) + "\"";
    }

    private static String safeMessage(Exception e) {
        String m = e.getMessage();
        if (m == null || m.trim().isEmpty()) return "failed";
        m = m.trim();
        return (m.length() > 300) ? m.substring(0, 300) : m;
    }
}
