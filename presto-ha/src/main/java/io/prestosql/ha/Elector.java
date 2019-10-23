package io.prestosql.ha;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import org.I0Itec.zkclient.ZkClient;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;

/**
 * @author tangyun@bigo.sg
 * @date 10/22/19 3:43 PM
 */
public class Elector {

    private boolean master = false;

    private HAConfig haConfig;
    private ZkClient zkClient;

    private NodeInfo thisNodeInfo = new NodeInfo();
    private NodeInfo masterNodeInfo = null;
    private final ScheduledThreadPoolExecutor executor =
            new ScheduledThreadPoolExecutor(1,
                    daemonThreadsNamed("ha-elector"));
    private static final Logger log = Logger.get(Elector.class);

    public NodeInfo getMasterNodeInfo() {
        return masterNodeInfo;
    }

    @Inject
    public Elector(HAConfig haConfig) {
        this.haConfig = haConfig;
        zkClient = new ZkClient(haConfig.getZkServers(), 50000);
    }


    public Elector() {

    }


    @PostConstruct
    public void start()
    {
        executor.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    elect();
                }
                catch (Throwable e) {
                    // ignore to avoid getting unscheduled
                    log.warn(e, "Error electing");
                }
            }
        }, 0, 5, TimeUnit.SECONDS);
    }


    public static void main(String[] args) throws InterruptedException {
        HAConfig haConfig = new HAConfig();
        haConfig.setZkServers("");
        Elector elector = new Elector(haConfig);
        elector.elect();
        while (true) {
            Thread.sleep(4000);
            System.out.println("im master?" + elector.isMaster());
        }
    }

    public boolean isMaster() {
        return master;
    }

    public void elect() {
        if (!zkClient.exists("/presto-ha")) {
            zkClient.createPersistent("/presto-ha");
        }
        if (!zkClient.exists("/presto-ha/elect")) {
            zkClient.createPersistent("/presto-ha/elect");
        }
        List<String> coordinators = zkClient.getChildren("/presto-ha/elect");
        if (coordinators == null || coordinators.size() == 0) {
            zkClient.createEphemeralSequential("/presto-ha/elect/coordinator-", thisNodeInfo);
        }
        coordinators = zkClient.getChildren("/presto-ha/elect");
        if (coordinators != null && coordinators.size() != 0) {
            String masterNode = coordinators.get(0);
            for (String coodinator: coordinators) {
                if (masterNode.compareTo(coodinator) > 0) {
                    masterNode = coodinator;
                }
            }
            NodeInfo remoteNodeInfo = zkClient.readData("/presto-ha/elect/" + masterNode);
            if (remoteNodeInfo.nodeId.equals(thisNodeInfo.nodeId)) {
                master = true;
            } else {
                master = false;
            }
            masterNodeInfo = remoteNodeInfo;
        }
    }

    public static String getRealIp() throws SocketException {
        String localip = null;
        String netip = null;

        Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
        InetAddress ip;
        boolean finded = false;
        while (netInterfaces.hasMoreElements() && !finded) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> address = ni.getInetAddresses();
            while (address.hasMoreElements()) {
                ip = address.nextElement();
                if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress() &&
                        ip.getHostAddress().indexOf(":") == -1) {
                    netip = ip.getHostAddress();
                    finded = true;
                    break;
                } else if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress()
                        && ip.getHostAddress().indexOf(":") == -1) {
                    localip = ip.getHostAddress();
                }
            }
        }

        if (netip != null && !"".equals(netip)) {
            return netip;
        } else {
            return localip;
        }
    }

    public static class NodeInfo implements Serializable {

        long serialVersionUID = -1693520978456877585L;
        private String ip;
        private String nodeId;

        @JsonProperty
        public String getIp() {
            return ip;
        }

        @JsonProperty
        public String getNodeId() {
            return nodeId;
        }

        public NodeInfo() {
            try {
                ip = getRealIp();
            } catch (SocketException e) {
                e.printStackTrace();
            }
            nodeId = String.valueOf(UUID.randomUUID());
        }
    }
}
