package org.apache.ranger.authorization.presto.authorizer;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.util.ArrayList;

/**
 * @author tangyun@bigo.sg
 * @date 9/25/19 5:55 PM
 */
public class PrestoRangerPlugin
        implements Plugin
{
    @Override
    public Iterable<SystemAccessControlFactory> getSystemAccessControlFactories()
    {
        ArrayList<SystemAccessControlFactory> list = new ArrayList<>();
        SystemAccessControlFactory factory = new RangerSystemAccessControlFactory();
        list.add(factory);
        return list;
    }
}
