package com.datastax.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jws.WebService;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@WebService
@Path("/")
public class DSEXMLLoaderWS {
    public static final String HEADER_ID = "DOC-ID";
    private static final Logger logger = LoggerFactory.getLogger(DSEXMLLoaderWS.class);
    private DSEXMLLoaderService service = new DSEXMLLoaderService();

    @POST
    @Path("/addXML")
    @Consumes(MediaType.APPLICATION_XML)
    public Response addXML(@HeaderParam(HEADER_ID) String id, String data) {
        logger.debug("Request Id: " + id);
        logger.debug("XML size is: " + data.length());
        try {
            service.loadXMLAndDetails(data, id);
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
        return Response.ok("success").build();
    }

    @GET
    @Path("/getXML")
    @Produces(MediaType.APPLICATION_XML)
    public Response getXML(@QueryParam("id") String id) {
        String data = null;

        logger.debug("Request Id: " + id);
        try {
            data = service.getXML(id);
            logger.debug("XML size is: " + data.length());
        } catch (Exception e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }

        if (data == null) {
            return Response.noContent().build();
        } else {
            return Response.ok(data, MediaType.APPLICATION_XML_TYPE).build();
        }
    }
}
