package org.example.repo;

import org.example.dto.QueryStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

public class QueryRepo {
    private static final Logger LOG = LoggerFactory.getLogger(QueryRepo.class);

    private final String url;
    private final String user;
    private final String password;

    public QueryRepo(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public Connection conn() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public void failRunningOnStartup(String msg) {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "update queries set status='FAILED', ended_at=now(), error=? where status='RUNNING'")) {
            ps.setString(1, msg);
            ps.executeUpdate();
        } catch (Exception e) {
            LOG.warn("failRunningOnStartup failed", e);
        }
    }

    public Optional<Row> byId(String userId, String id) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement("select * from queries where id=? and user_id=?")) {
            ps.setString(1, id);
            ps.setString(2, userId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(read(rs));
            }
        }
    }

    public Optional<Row> byIdem(String userId, String idem) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "select * from queries where user_id=? and idempotency_key=?")) {
            ps.setString(1, userId);
            ps.setString(2, idem);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(read(rs));
            }
        }
    }

    public int countUser(String userId, QueryStatus st) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "select count(*) from queries where user_id=? and status=?")) {
            ps.setString(1, userId);
            ps.setString(2, st.name());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    public int countGlobal(QueryStatus st) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement("select count(*) from queries where status=?")) {
            ps.setString(1, st.name());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
    }

    public void insert(Row r) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "insert into queries(id,user_id,idempotency_key,sql,status,created_at) values(?,?,?,?,?,?)")) {
            ps.setString(1, r.id);
            ps.setString(2, r.userId);
            ps.setString(3, r.idempotencyKey);
            ps.setString(4, r.sql);
            ps.setString(5, r.status.name());
            ps.setTimestamp(6, Timestamp.from(r.createdAt));
            ps.executeUpdate();
        }
    }

    public boolean pendingToRunning(String userId, String id, Instant startedAt) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "update queries set status='RUNNING', started_at=? where id=? and user_id=? and status='PENDING'")) {
            ps.setTimestamp(1, Timestamp.from(startedAt));
            ps.setString(2, id);
            ps.setString(3, userId);
            return ps.executeUpdate() == 1;
        }
    }

    public void succeed(String userId, String id, Instant endedAt, String path, long rows, long bytes) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "update queries " +
                             "set status='SUCCEEDED', ended_at=?, result_path=?, rows_written=?, bytes_written=?, error=null " +
                             "where id=? and user_id=? and status='RUNNING'")) {
            ps.setTimestamp(1, Timestamp.from(endedAt));
            ps.setString(2, path);
            ps.setLong(3, rows);
            ps.setLong(4, bytes);
            ps.setString(5, id);
            ps.setString(6, userId);
            ps.executeUpdate();
        }
    }

    public void fail(String userId, String id, Instant endedAt, String error) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "update queries " +
                             "set status='FAILED', ended_at=?, error=? " +
                             "where id=? and user_id=? and status='RUNNING'")) {
            ps.setTimestamp(1, Timestamp.from(endedAt));
            ps.setString(2, error);
            ps.setString(3, id);
            ps.setString(4, userId);
            ps.executeUpdate();
        }
    }


    public void cancel(String userId, String id, Instant endedAt) throws SQLException {
        try (Connection c = conn();
             PreparedStatement ps = c.prepareStatement(
                     "update queries set status='CANCELLED', ended_at=? where id=? and user_id=? and status in ('PENDING','RUNNING')")) {
            ps.setTimestamp(1, Timestamp.from(endedAt));
            ps.setString(2, id);
            ps.setString(3, userId);
            ps.executeUpdate();
        }
    }

    private static Row read(ResultSet rs) throws SQLException {
        Row r = new Row();
        r.id = rs.getString("id");
        r.userId = rs.getString("user_id");
        r.idempotencyKey = rs.getString("idempotency_key");
        r.sql = rs.getString("sql");
        r.status = QueryStatus.valueOf(rs.getString("status"));
        r.createdAt = rs.getTimestamp("created_at").toInstant();

        Timestamp st = rs.getTimestamp("started_at");
        Timestamp en = rs.getTimestamp("ended_at");
        r.startedAt = (st == null) ? null : st.toInstant();
        r.endedAt = (en == null) ? null : en.toInstant();

        r.error = rs.getString("error");
        r.resultPath = rs.getString("result_path");

        long rw = rs.getLong("rows_written");
        r.rowsWritten = rs.wasNull() ? null : rw;

        long bw = rs.getLong("bytes_written");
        r.bytesWritten = rs.wasNull() ? null : bw;

        return r;
    }

    public static class Row {
        public String id;
        public String userId;
        public String idempotencyKey;
        public String sql;
        public QueryStatus status;
        public Instant createdAt;
        public Instant startedAt;
        public Instant endedAt;
        public String error;
        public String resultPath;
        public Long rowsWritten;
        public Long bytesWritten;
    }
}
