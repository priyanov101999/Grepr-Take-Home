package org.example.errors;

import org.example.dto.ApiError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
// Global exception handler for entire application
@Provider
public class GlobalExceptionMapper implements ExceptionMapper<Throwable> {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalExceptionMapper.class);
    // Maps exceptions to HTTP responses
    @Override
    public Response toResponse(Throwable e) {
        if (e instanceof IllegalArgumentException) {
            return json(400, "invalid request", e.getMessage());
        }

        if (e instanceof WebApplicationException) {
            WebApplicationException we = (WebApplicationException) e;
            Response r = we.getResponse();
            int code = (r != null) ? r.getStatus() : 500;

            if (r != null && r.hasEntity()) return r;

            return json(code, messageCodeMapper(code), we.getMessage());
        }
        // For unexpected errors
        LOG.error("Unhandled exception", e);
        return json(500, "internal error", "unexpected error");
    }

    private static Response json(int code, String message, String detail) {
        return Response.status(code)
                .type(MediaType.APPLICATION_JSON)
                .entity(new ApiError(code, message, detail))
                .build();
    }
    // Maps HTTP status codes to error messages
    private static String messageCodeMapper(int code) {
        if (code == 401) return "unauthorized access";
        if (code == 404) return "URL not found";
        if (code == 409) return "not ready";
        if (code == 429) return "Too many hits, rate limited";
        if (code >= 400 && code < 500) return "bad request";
        return "internal error";
    }
}
