package org.example.service;

import java.util.Locale;

public class SqlGuard {
    private final int maxChars;

    public SqlGuard(int maxChars) {

        this.maxChars = maxChars;
    }

    public void validate(String sql) {
        // Trim the input
        String s = (sql == null) ? "" : sql.trim();
        // handling empty string, sql length exceeding max length, multiple statements cases
        if (s.isEmpty()) throw new IllegalArgumentException("sql required");
        if (s.length() > maxChars) throw new IllegalArgumentException("sql too long");
        if (s.contains(";")) throw new IllegalArgumentException("multiple statements are not allowed");
        //  Allowing only select statements
        String low = s.toLowerCase(Locale.ROOT);
        if (!low.startsWith("select")) throw new IllegalArgumentException("only SELECT allowed");

        String[] forbidden = {
                "insert", "update", "delete", "drop", "alter", "create", "copy", "grant", "revoke"
        };
       // Throwing error when DML,DDL is used
        for (String kw : forbidden) {
            if (low.contains(kw)) throw new IllegalArgumentException("keyword not allowed: " + kw);
        }
    }
}
