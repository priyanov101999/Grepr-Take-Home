package org.example.resources;

import org.example.dto.QueryResponse;
import org.example.dto.SubmitQueryRequest;
import org.example.service.QueryService;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

@Path("/queries")
@Produces(MediaType.APPLICATION_JSON)
public class QueryResource {
    private final QueryService service;

    public QueryResource(QueryService service) {
        this.service = service;
    }

    private static String userId(SecurityContext sc) {
        return (sc.getUserPrincipal() == null) ? "" : sc.getUserPrincipal().getName();
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public QueryResponse submit(@Context SecurityContext sc,
                                SubmitQueryRequest req,
                                @HeaderParam("Idempotency-Key") String idempotencyKey) throws Exception {
        String sql = (req == null) ? null : req.getSql();
        if (sql == null || sql.trim().isEmpty()) {
            throw new WebApplicationException("sql required", 400);
        }
        return service.submit(userId(sc), sql, idempotencyKey);
    }

    @GET
    @Path("/{id}")
    public QueryResponse status(@Context SecurityContext sc,
                                @PathParam("id") String id) throws Exception {
        return service.status(userId(sc), id);
    }

    @GET
    @Path("/{id}/results")
    @Produces("application/x-ndjson")
    public Response results(@Context SecurityContext sc,
                            @PathParam("id") String id) throws Exception {
        return service.results(userId(sc), id);
    }

    @POST
    @Path("/{id}/cancel")
    public QueryResponse cancel(@Context SecurityContext sc,
                                @PathParam("id") String id) throws Exception {
        return service.cancel(userId(sc), id);
    }
}
