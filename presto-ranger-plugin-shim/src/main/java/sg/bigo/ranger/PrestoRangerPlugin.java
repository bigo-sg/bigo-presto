package sg.bigo.ranger;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.util.ArrayList;
/**
 * @author tangyun@bigo.sg
 * @date 10/16/19 5:23 PM
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
