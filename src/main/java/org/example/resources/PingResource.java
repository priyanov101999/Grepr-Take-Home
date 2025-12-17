package org.example.resources;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/ping")
@Produces(MediaType.TEXT_PLAIN)
public class PingResource {

    @GET
    public String ping() {
        return "ok";
    }
}
