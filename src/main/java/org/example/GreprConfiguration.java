package org.example;

import io.dropwizard.Configuration;

public class GreprConfiguration extends Configuration {
    public String dbUrl;
    public String dbUser;
    public String dbPassword;

    public String resultsDir = "results";

    public int workerCount = 2;
    public int queueSize = 100;

    public int maxPendingPerUser = 10;
    public int maxRunningPerUser = 2;
    public int maxRunningGlobal = 10;

    public int statementTimeoutMs = 10_000;
    public int fetchSize = 500;

    public long maxRows = 200_000;
    public long maxBytes = 50_000_000;

    public int maxSqlChars = 10_000;
    public int rateLimitPerMinute = 30;
}
