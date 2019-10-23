package io.prestosql.ha;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author tangyun@bigo.sg
 * @date 10/22/19 8:40 PM
 */
public class HAConfig {
    public static final String ZK_SERVERS = "ha.zookeeper.server.list";
    public static final String CORDINATOR_SERVERS = "ha.coordinator.server.list";
    public static final String HTTP_SERVER_HTTP_PORT ="http-server.http.port";

    private String zkServers = "localhost:2181";
    private List<String> coordinatorServers;
    private String httpServerPort;

    @NotNull
    public String getHttpServerPort()
    {
        return httpServerPort;
    }

    @NotNull
    @Config(HTTP_SERVER_HTTP_PORT)
    @ConfigDescription("the zookeeper server list")
    public HAConfig setHttpServerPort(String httpServerPort)
    {
        this.httpServerPort = httpServerPort;
        return this;
    }

    @NotNull
    public String getZkServers()
    {
        return zkServers;
    }

    @NotNull
    @Config(ZK_SERVERS)
    @ConfigDescription("the zookeeper server list")
    public HAConfig setZkServers(String zkServers)
    {
        this.zkServers = zkServers;
        return this;
    }

    @NotNull
    public List<String> getCoordinatorServers()
    {
        return coordinatorServers;
    }

    @NotNull
    @Config(CORDINATOR_SERVERS)
    @ConfigDescription("the zookeeper server list")
    public HAConfig setCoordinatorServers(String coordinatorServers)
    {
        String[] tmp = coordinatorServers.split(",");
        this.coordinatorServers = new ArrayList<>();
        for (String coordinatorServer: tmp) {
            this.coordinatorServers.add(coordinatorServer);
        }
        return this;
    }
}
