package sg.bigo.ranger;

import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.util.Map;

/**
 * @author tangyun@bigo.sg
 * @date 9/25/19 5:55 PM
 */
public class RangerSystemAccessControlFactory
  implements SystemAccessControlFactory {
  private static final String NAME = "ranger-plugin";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public SystemAccessControl create(Map<String, String> config) {
    RangerUtils.init(config);
    return new RangerSystemAccessControl();
  }
}
