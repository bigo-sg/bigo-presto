package sg.bigo.ranger;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.util.Map;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

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
    requireNonNull(config, "config is null");
    RangerUtils.init(config);
    try {
      Bootstrap app = new Bootstrap(
        binder -> {
          configBinder(binder);
          binder.bind(RangerSystemAccessControl.class).in(Scopes.SINGLETON);
        }
      );

      Injector injector = app
        .strictConfig()
        .doNotInitializeLogging()
        .setRequiredConfigurationProperties(config)
        .initialize();

      return injector.getInstance(RangerSystemAccessControl.class);
    } catch (Exception e) {
      throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
