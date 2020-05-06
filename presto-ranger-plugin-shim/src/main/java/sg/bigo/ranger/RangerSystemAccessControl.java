package sg.bigo.ranger;

import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemSecurityContext;

import javax.inject.Inject;
import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.spi.security.AccessDeniedException.*;

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
  public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName) {
  }

  @Override
  public void checkCanExecuteQuery(SystemSecurityContext context)
  {
  }

  @Override
  public void checkCanKillQueryOwnedBy(SystemSecurityContext context, String queryOwner)
  {
  }

  @Override
  public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName) {
  }

  @Override
  public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs) {
    return catalogs;
  }

  @Override
  public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema) {
    AccessDeniedException.denyCreateSchema(schema.getSchemaName());
  }

  @Override
  public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema) {
    AccessDeniedException.denyDropSchema(schema.getSchemaName());
  }

  @Override
  public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName) {
    AccessDeniedException.denyRenameSchema(schema.getSchemaName(), newSchemaName);
  }

  @Override
  public void checkCanShowSchemas(SystemSecurityContext context, String catalogName) {
  }

  @Override
  public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames) {
    return schemaNames;
  }

  public void checkCanShowColumns(SystemSecurityContext context, CatalogSchemaTableName table)
  {
  }
  @Override
  public List<ColumnMetadata> filterColumns(SystemSecurityContext context, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
  {
    return columns;
  }

  @Override
  public void checkCanShowCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
  {
  }
  @Override
  public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table) {
    if (!RangerUtils.checkPermission(context.getIdentity(), table, PrestoAccessType.CREATE)) {
      AccessDeniedException.denyCreateTable(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table) {
    if (!RangerUtils.checkPermission(context.getIdentity(), table, PrestoAccessType.DROP)) {
      AccessDeniedException.denyDropTable(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable) {
    if (!RangerUtils.checkPermission(context.getIdentity(), table, PrestoAccessType.ALTER)) {
      AccessDeniedException.denyRenameTable(table.getSchemaTableName().getTableName(),
              newTable.getSchemaTableName().getTableName());
    }
  }
  @Override
  public void checkCanShowTables(SystemSecurityContext context, CatalogSchemaName schema)
  {
  }
  @Override
  public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames) {
    return tableNames;
  }

  @Override
  public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
    if (!RangerUtils.checkPermission(context.getIdentity(), table, PrestoAccessType.ALTER)) {
      AccessDeniedException.denyAddColumn(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
    if (!RangerUtils.checkPermission(context.getIdentity(), table, PrestoAccessType.ALTER)) {
      AccessDeniedException.denyDropColumn(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table) {
    // we rename column need drop
    if (!RangerUtils.checkPermission(context.getIdentity(), table, PrestoAccessType.ALTER)) {
      AccessDeniedException.denyRenameColumn(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {
    if (!RangerUtils.checkPermission(context.getIdentity(), table, PrestoAccessType.SELECT)) {
      AccessDeniedException.denySelectColumns(table.getSchemaTableName().getTableName(),
              columns);
    }
  }

  @Override
  public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table) {
    if (!RangerUtils.checkPermission(context.getIdentity(), table, PrestoAccessType.INSERT)) {
      AccessDeniedException.denyInsertTable(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table) {
    // we think presto delete can act as insert overwrite
    if (!RangerUtils.checkPermission(context.getIdentity(), table, PrestoAccessType.DELETE)) {
      AccessDeniedException.denyDeleteTable(table.getSchemaTableName().getTableName());
    }
  }

  @Override
  public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view) {
    AccessDeniedException.denyCreateView(view.getSchemaTableName().getTableName());
  }

  @Override
  public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view) {
    AccessDeniedException.denyDropView(view.getSchemaTableName().getTableName());
  }

  @Override
  public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns) {
    AccessDeniedException.denyCreateViewWithSelect(table.getSchemaTableName().getTableName(), context.getIdentity());
  }

  @Override
  public void checkCanExecuteFunction(SystemSecurityContext systemSecurityContext, String functionName)
  {
  }

  @Override
  public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName) {
  }
}
