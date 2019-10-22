package io.prestosql.ha;

import io.airlift.discovery.server.DynamicAnnouncement;
import io.airlift.discovery.server.DynamicStore;
import io.airlift.discovery.server.Id;
import io.airlift.discovery.server.Node;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.String.format;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

/**
 * @author tangyun@bigo.sg
 * @date 10/22/19 5:54 PM
 */
@Path("/v1/announcement/{node_id}")
public class HADynamicAnnouncementResource
{
    private final NodeInfo nodeInfo;
    private final DynamicStore dynamicStore;

    @Inject
    public HADynamicAnnouncementResource(DynamicStore dynamicStore, NodeInfo nodeInfo)
    {
        this.dynamicStore = dynamicStore;
        this.nodeInfo = nodeInfo;
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public Response put(@PathParam("node_id") Id<Node> nodeId, @Context UriInfo uriInfo, DynamicAnnouncement announcement)
    {
        if (!nodeInfo.getEnvironment().equals(announcement.getEnvironment())) {
            return Response.status(BAD_REQUEST)
                    .entity(format("Environment mismatch. Expected: %s, Provided: %s", nodeInfo.getEnvironment(), announcement.getEnvironment()))
                    .build();
        }

        String location = firstNonNull(announcement.getLocation(), "/somewhere/" + nodeId.toString());

        DynamicAnnouncement announcementWithLocation = DynamicAnnouncement.copyOf(announcement)
                .setLocation(location)
                .build();

        dynamicStore.put(nodeId, announcementWithLocation);

        return Response.status(ACCEPTED).build();
    }

    @DELETE
    public Response delete(@PathParam("node_id") Id<Node> nodeId)
    {
        dynamicStore.delete(nodeId);

        return Response.noContent().build();
    }
}
