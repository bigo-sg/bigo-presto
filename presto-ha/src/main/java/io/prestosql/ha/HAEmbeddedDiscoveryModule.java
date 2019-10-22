package io.prestosql.ha;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.server.*;
import io.airlift.discovery.store.InMemoryStore;
import io.airlift.discovery.store.ReplicatedStoreModule;

import java.util.Set;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

/**
 * @author tangyun@bigo.sg
 * @date 10/22/19 5:44 PM
 */
public class HAEmbeddedDiscoveryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DiscoveryConfig.class);
        jaxrsBinder(binder).bind(ServiceResource.class);

        discoveryBinder(binder).bindHttpAnnouncement("discovery");

        jsonCodecBinder(binder).bindJsonCodec(Service.class);
        jsonCodecBinder(binder).bindListJsonCodec(Service.class);

        binder.bind(ServiceSelector.class).to(DiscoveryServiceSelector.class);
        binder.bind(StaticStore.class).to(HAEmbeddedDiscoveryModule.EmptyStaticStore.class);

        jaxrsBinder(binder).bind(HADynamicAnnouncementResource.class);
        binder.bind(DynamicStore.class).to(ReplicatedDynamicStore.class).in(Scopes.SINGLETON);
        binder.install(new ReplicatedStoreModule("dynamic", ForDynamicStore.class, InMemoryStore.class));
    }

    private static class EmptyStaticStore
            implements StaticStore
    {
        @Override
        public void put(Service service)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(Id<Service> id)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Service> getAll()
        {
            return ImmutableSet.of();
        }

        @Override
        public Set<Service> get(String type)
        {
            return ImmutableSet.of();
        }

        @Override
        public Set<Service> get(String type, String pool)
        {
            return ImmutableSet.of();
        }
    }
}