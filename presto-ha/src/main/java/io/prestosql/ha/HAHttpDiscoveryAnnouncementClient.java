package io.prestosql.ha;

import com.google.common.io.CharStreams;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.discovery.client.*;
import io.airlift.http.client.*;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.*;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

/**
 * @author tangyun@bigo.sg
 * @date 10/23/19 10:28 AM
 */
public class HAHttpDiscoveryAnnouncementClient
        implements DiscoveryAnnouncementClient
{
    private static final MediaType MEDIA_TYPE_JSON = MediaType.create("application", "json");

    private final Supplier<URI> discoveryServiceURI;
    private final NodeInfo nodeInfo;
    private final JsonCodec<Announcement> announcementCodec;
    private final HttpClient httpClient;
    private final HAConfig haConfig;

    @Inject
    public HAHttpDiscoveryAnnouncementClient (
            @ForDiscoveryClient Supplier<URI> discoveryServiceURI,
            NodeInfo nodeInfo,
            JsonCodec<Announcement> announcementCodec,
            @ForDiscoveryClient HttpClient httpClient,
            HAConfig haConfig)
    {
        requireNonNull(discoveryServiceURI, "discoveryServiceURI is null");
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(announcementCodec, "announcementCodec is null");
        requireNonNull(httpClient, "httpClient is null");

        this.nodeInfo = nodeInfo;
        this.discoveryServiceURI = discoveryServiceURI;
        this.announcementCodec = announcementCodec;
        this.httpClient = httpClient;
        this.haConfig = haConfig;
    }

    @Override
    public ListenableFuture<Duration> announce(Set<ServiceAnnouncement> services)
    {
        requireNonNull(services, "services is null");

        URI uri = getMasterURI();
        if (uri == null) {
            return immediateFailedFuture(new DiscoveryException("No discovery servers are available"));
        }

        Announcement announcement = new Announcement(nodeInfo.getEnvironment(), nodeInfo.getNodeId(), nodeInfo.getPool(), nodeInfo.getLocation(), services);
        Request request = preparePut()
                .setUri(URI.create(uri + "/v1/announcement/" + nodeInfo.getNodeId()))
                .setHeader("User-Agent", nodeInfo.getNodeId())
                .setHeader("Content-Type", MEDIA_TYPE_JSON.toString())
                .setBodyGenerator(jsonBodyGenerator(announcementCodec, announcement))
                .build();
        return httpClient.executeAsync(request, new HAHttpDiscoveryAnnouncementClient.DiscoveryResponseHandler<Duration>("Announcement", uri)
        {
            @Override
            public Duration handle(Request request, Response response)
                    throws DiscoveryException
            {
                int statusCode = response.getStatusCode();
                if (!isSuccess(statusCode)) {
                    throw new DiscoveryException(String.format("Announcement failed with status code %s: %s", statusCode, getBodyForError(response)));
                }

                Duration maxAge = extractMaxAge(response);
                return maxAge;
            }
        });
    }

    private URI getMasterURI() {
        Random random = new Random();
        List<String> urls = new LinkedList<>();
        haConfig.getCoordinatorServers().stream().forEach(s -> urls.add(s));
        URI uri;
        while (urls.size() > 0) {
            int nextIndex = random.nextInt(urls.size() + 1);
            uri = URI.create(urls.get(nextIndex - 1) + "/v1/ha/whoismaster");
            urls.remove(nextIndex - 1);
            Request request = prepareGet()
                    .setUri(uri)
                    .setHeader("User-Agent", nodeInfo.getNodeId())
                    .setHeader("Content-Type", MEDIA_TYPE_JSON.toString())
                    .build();
            JsonResponseHandler<Elector.NodeInfo> responseHandler =
                    createJsonResponseHandler(jsonCodec(Elector.NodeInfo.class));
            Elector.NodeInfo response = httpClient.execute(request, responseHandler);
            if (response != null && response.getIp() != null) {
                return URI.create("http://" + response.getIp() + haConfig.getHttpServerPort());
            }
        }
        throw new DiscoveryException("not get any proper master cordinator uri");
    }

    private static boolean isSuccess(int statusCode)
    {
        return statusCode / 100 == 2;
    }

    private static String getBodyForError(Response response)
    {
        try {
            return CharStreams.toString(new InputStreamReader(response.getInputStream(), UTF_8));
        }
        catch (IOException e) {
            return "(error getting body)";
        }
    }

    @Override
    public ListenableFuture<Void> unannounce()
    {
        URI uri = discoveryServiceURI.get();
        if (uri == null) {
            return immediateFuture(null);
        }

        Request request = prepareDelete()
                .setUri(URI.create(uri + "/v1/announcement/" + nodeInfo.getNodeId()))
                .setHeader("User-Agent", nodeInfo.getNodeId())
                .build();
        return httpClient.executeAsync(request, new HAHttpDiscoveryAnnouncementClient.DiscoveryResponseHandler<>("Unannouncement", uri));
    }

    private static Duration extractMaxAge(Response response)
    {
        String header = response.getHeader(HttpHeaders.CACHE_CONTROL);
        if (header != null) {
            CacheControl cacheControl = CacheControl.valueOf(header);
            if (cacheControl.getMaxAge() > 0) {
                return new Duration(cacheControl.getMaxAge(), TimeUnit.SECONDS);
            }
        }
        return DEFAULT_DELAY;
    }

    private class DiscoveryResponseHandler<T>
            implements ResponseHandler<T, DiscoveryException>
    {
        private final String name;
        private final URI uri;

        protected DiscoveryResponseHandler(String name, URI uri)
        {
            this.name = name;
            this.uri = uri;
        }

        @Override
        public T handle(Request request, Response response)
        {
            return null;
        }

        @Override
        public final T handleException(Request request, Exception exception)
        {
            if (exception instanceof InterruptedException) {
                throw new DiscoveryException(name + " was interrupted for " + uri);
            }
            if (exception instanceof CancellationException) {
                throw new DiscoveryException(name + " was canceled for " + uri);
            }
            if (exception instanceof DiscoveryException) {
                throw (DiscoveryException) exception;
            }

            throw new DiscoveryException(name + " failed for " + uri, exception);
        }
    }
}
