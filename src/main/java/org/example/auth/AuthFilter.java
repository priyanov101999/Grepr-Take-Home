package org.example.auth;

import org.example.dto.ApiError;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.security.Principal;

@Provider
public class AuthFilter implements ContainerRequestFilter {

    @Override
    public void filter(ContainerRequestContext ctx) throws IOException {
        String path = ctx.getUriInfo().getPath();

        if (path.equals("ping")) {

            return;
        }

        String auth = ctx.getHeaderString("Authorization");
        if (auth == null || !auth.startsWith("Bearer ")) {
            abort(ctx, 401, "unauthorized", "missing bearer token");
            return;
        }

        String token = auth.substring("Bearer ".length()).trim();
        if (!token.startsWith("user:") || token.length() <= "user:".length()) {
            abort(ctx, 401, "unauthorized", "invalid bearer token");
            return;
        }

        String userId = token.substring("user:".length());
        AuthUser user = new AuthUser(userId);

        SecurityContext original = ctx.getSecurityContext();
        SecurityContext sc = new SecurityContext() {
            @Override public Principal getUserPrincipal() { return user; }
            @Override public boolean isUserInRole(String role) { return false; }
            @Override public boolean isSecure() { return original != null && original.isSecure(); }
            @Override public String getAuthenticationScheme() { return "Bearer"; }
        };

        ctx.setSecurityContext(sc);
    }

    private static void abort(ContainerRequestContext ctx, int code, String message, String detail) {
        ctx.abortWith(Response.status(code)
                .type(MediaType.APPLICATION_JSON)
                .entity(new ApiError(code, message, detail))
                .build());
    }
}
