package org.apache.ranger.authorization.presto.authorizer;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.SystemAccessControl;
import org.apache.ranger.authorization.presto.authorizer.utils.PrestoAccessType;
import org.apache.ranger.authorization.presto.authorizer.utils.RangerUtils;

import javax.inject.Inject;
import java.security.Principal;
import java.util.Optional;
import java.util.Set;

/**
 * @author tangyun@bigo.sg
 * @date 9/25/19 5:55 PM
 */
public class RangerSystemAccessControl
  implements SystemAccessControl {

  @Inject
  public RangerSystemAccessControl() {
  }

  @Override
  public void checkCanSetUser(Optional<Principal> principal, String userName) {
  }

  @Override
  public void checkCanSetSystemSessionProperty(Identity identity, String propertyName) {
  }

  @Override
  public void checkCanAccessCatalog(Identity identity, String catalogName) {
  }

  @Override
  public Set<String> filterCatalogs(Identity identity, Set<String> catalogs) {
    return catalogs;
  }

  @Override
  public void checkCanCreateSchema(Identity identity, CatalogSchemaName schema) {
    AccessDeniedException.denyCreateSchema(schema.getSchemaName());
  }

  @Override
  public void checkCanDropSchema(Identity identity, CatalogSchemaName schema) {
    AccessDeniedException.denyDropSchema(schema.getSchemaName());
  }

  @Override
  public void checkCanRenameSchema(Identity identity, CatalogSchemaName schema, String newSchemaName) {
    AccessDeniedException.denyRenameSchema(schema.getSchemaName(), newSchemaName);
  }

  @Override
  public void checkCanShowSchemas(Identity identity, String catalogName) {
  }

  @Override
  public Set<String> filterSchemas(Identity identity, String catalogName, Set<String> schemaNames) {
    return schemaNames;
  }

  @Override
  public void checkCanCreateTable(Identity identity, CatalogSchemaTableName table) {
    if (!RangerUtils.checkPermission(identity, table, PrestoAccessType.CREATE)) {
      AccessDeniedException.denyCreateTable(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanDropTable(Identity identity, CatalogSchemaTableName table) {
    if (!RangerUtils.checkPermission(identity, table, PrestoAccessType.DROP)) {
      AccessDeniedException.denyDropTable(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanRenameTable(Identity identity, CatalogSchemaTableName table, CatalogSchemaTableName newTable) {
    if (!RangerUtils.checkPermission(identity, table, PrestoAccessType.ALTER)) {
      AccessDeniedException.denyRenameTable(table.getSchemaTableName().getTableName(),
              newTable.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanShowTablesMetadata(Identity identity, CatalogSchemaName schema) {
      if (!RangerUtils.checkPermission(identity, schema, PrestoAccessType.SELECT)) {
          AccessDeniedException.denyShowTablesMetadata(schema.toString());
      }
  }

  @Override
  public Set<SchemaTableName> filterTables(Identity identity, String catalogName, Set<SchemaTableName> tableNames) {
    return tableNames;
  }

  @Override
  public void checkCanAddColumn(Identity identity, CatalogSchemaTableName table) {
    if (!RangerUtils.checkPermission(identity, table, PrestoAccessType.ALTER)) {
      AccessDeniedException.denyAddColumn(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanDropColumn(Identity identity, CatalogSchemaTableName table) {
    if (!RangerUtils.checkPermission(identity, table, PrestoAccessType.ALTER)) {
      AccessDeniedException.denyDropColumn(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanRenameColumn(Identity identity, CatalogSchemaTableName table) {
    // we rename column need drop
    if (!RangerUtils.checkPermission(identity, table, PrestoAccessType.ALTER)) {
      AccessDeniedException.denyRenameColumn(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns) {
    if (!RangerUtils.checkPermission(identity, table, PrestoAccessType.SELECT)) {
      AccessDeniedException.denySelectColumns(table.getSchemaTableName().getTableName(),
              columns);
    }
  }

  @Override
  public void checkCanInsertIntoTable(Identity identity, CatalogSchemaTableName table) {
    if (!RangerUtils.checkPermission(identity, table, PrestoAccessType.INSERT)) {
      AccessDeniedException.denyInsertTable(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanDeleteFromTable(Identity identity, CatalogSchemaTableName table) {
    // we think presto delete can act as insert overwrite
    if (!RangerUtils.checkPermission(identity, table, PrestoAccessType.DELETE)) {
      AccessDeniedException.denyDeleteTable(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanCreateView(Identity identity, CatalogSchemaTableName view) {
    AccessDeniedException.denyCreateView(view.getSchemaTableName().getTableName());
  }

  @Override
  public void checkCanDropView(Identity identity, CatalogSchemaTableName view) {
    AccessDeniedException.denyDropView(view.getSchemaTableName().getTableName());
  }

  @Override
  public void checkCanCreateViewWithSelectFromColumns(Identity identity, CatalogSchemaTableName table, Set<String> columns) {
    AccessDeniedException.denyCreateViewWithSelect(table.getSchemaTableName().getTableName(), identity);
  }

  @Override
  public void checkCanSetCatalogSessionProperty(Identity identity, String catalogName, String propertyName) {
  }
}
