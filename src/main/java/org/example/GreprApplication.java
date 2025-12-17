package org.example;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import org.example.repo.QueryRepo;
import org.example.service.QueryService;
import org.example.service.RateLimiter;
import org.example.service.SqlGuard;
import org.example.auth.AuthFilter;
import org.example.errors.GlobalExceptionMapper;
import org.example.resources.PingResource;
import org.example.resources.QueryResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class GreprApplication extends Application<GreprConfiguration> {
    private static final Logger LOG = LoggerFactory.getLogger(GreprApplication.class);

    @Override
    public void run(GreprConfiguration cfg, Environment env) {
        QueryRepo store = new QueryRepo(cfg.dbUrl, cfg.dbUser, cfg.dbPassword);
        store.failRunningOnStartup("server restarted while running");

        File resultsDir = new File(cfg.resultsDir);
        if (!resultsDir.exists() && !resultsDir.mkdirs()) {
            LOG.warn("Failed to create resultsDir");
        }

        SqlGuard sqlGuard = new SqlGuard(cfg.maxSqlChars);
        RateLimiter rateLimiter = new RateLimiter(cfg.rateLimitPerMinute);

        QueryService service = new QueryService(
                store,
                sqlGuard,
                rateLimiter,
                cfg.workerCount,
                cfg.queueSize,
                cfg.maxPendingPerUser,
                cfg.maxRunningPerUser,
                cfg.maxRunningGlobal,
                resultsDir,
                cfg.statementTimeoutMs,
                cfg.fetchSize,
                cfg.maxRows,
                cfg.maxBytes
        );

        env.jersey().register(new GlobalExceptionMapper());
        env.jersey().register(new AuthFilter());

        env.jersey().register(new PingResource());
        env.jersey().register(new QueryResource(service));
    }

    public static void main(String[] args) throws Exception {
        new GreprApplication().run(args);
    }
}
