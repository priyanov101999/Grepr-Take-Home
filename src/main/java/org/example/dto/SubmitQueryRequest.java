package org.example.dto;
// Request payload for submitting a SQL query
public class SubmitQueryRequest {
    private String sql;

    public SubmitQueryRequest() {}

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {

        this.sql = sql;
    }
}
