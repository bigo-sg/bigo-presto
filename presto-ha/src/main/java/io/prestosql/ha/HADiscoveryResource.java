package io.prestosql.ha;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

/**
 * @author tangyun@bigo.sg
 * @date 10/22/19 8:25 PM
 */
@Path("/v1/ha/whoismaster")
public class HADiscoveryResource {

    private final Elector elector;

    @Inject
    public HADiscoveryResource(Elector elector)
    {
        this.elector = elector;
    }

    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    public Elector.NodeInfo get()
    {
        return elector.getMasterNodeInfo();
    }
}
