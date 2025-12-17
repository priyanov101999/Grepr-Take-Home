package org.example.dto;

import java.time.Instant;
// When user submits the query,this is the returned response dto
public class QueryResponse {
    public String id;
    public QueryStatus status;
    public Instant createdAt;
    public Instant startedAt;
    public Instant endedAt;
    public String error;
    public Long rowsWritten;
    public Long bytesWritten;
}
