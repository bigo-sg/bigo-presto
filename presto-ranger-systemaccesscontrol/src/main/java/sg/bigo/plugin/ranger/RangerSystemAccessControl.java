/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sg.bigo.plugin.ranger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.plugin.base.security.ForwardingSystemAccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.*;

import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static io.prestosql.spi.security.AccessDeniedException.*;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RangerSystemAccessControl
        implements SystemAccessControl
{
    private static final Logger log = Logger.get(RangerSystemAccessControl.class);

    public static final String NAME = "ranger";

    private final List<CatalogAccessControlRule> catalogRules;
    private final Optional<List<PrincipalUserMatchRule>> principalUserMatchRules;

    private RangerSystemAccessControl(List<CatalogAccessControlRule> catalogRules, Optional<List<PrincipalUserMatchRule>> principalUserMatchRules)
    {
        this.catalogRules = catalogRules;
        this.principalUserMatchRules = principalUserMatchRules;
    }

    public static class Factory
            implements SystemAccessControlFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public SystemAccessControl create(Map<String, String> config)
        {
            requireNonNull(config, "config is null");

            String configFileName = config.get(RangerAccessControlConfig.SECURITY_CONFIG_FILE);
            Preconditions.checkState(configFileName != null, "Security configuration must contain the '%s' property", RangerAccessControlConfig.SECURITY_CONFIG_FILE);

            if (config.containsKey(RangerAccessControlConfig.SECURITY_REFRESH_PERIOD)) {
                Duration refreshPeriod;
                try {
                    refreshPeriod = Duration.valueOf(config.get(RangerAccessControlConfig.SECURITY_REFRESH_PERIOD));
                }
                catch (IllegalArgumentException e) {
                    throw invalidRefreshPeriodException(config, configFileName);
                }
                if (refreshPeriod.toMillis() == 0) {
                    throw invalidRefreshPeriodException(config, configFileName);
                }
                return ForwardingSystemAccessControl.of(memoizeWithExpiration(
                        () -> {
                            log.info("Refreshing system access control from %s", configFileName);
                            return create(configFileName);
                        },
                        refreshPeriod.toMillis(),
                        MILLISECONDS));
            }
            return create(configFileName);
        }

        private PrestoException invalidRefreshPeriodException(Map<String, String> config, String configFileName)
        {
            return new PrestoException(
                    CONFIGURATION_INVALID,
                    String.format("Invalid duration value '%s' for property '%s' in '%s'", config.get(RangerAccessControlConfig.SECURITY_REFRESH_PERIOD), RangerAccessControlConfig.SECURITY_REFRESH_PERIOD, configFileName));
        }

        private SystemAccessControl create(String configFileName)
        {
            ObjectMapper mapper = new ObjectMapper();

            RangerSystemAccessControlRules rules = null;
            try {
                rules = mapper.readValue(FileUtils.getFileAsBytes(configFileName), RangerSystemAccessControlRules.class);
            } catch (IOException e) {
                log.error("getting config file failed");
            }

            ImmutableList.Builder<CatalogAccessControlRule> catalogRulesBuilder = ImmutableList.builder();
            catalogRulesBuilder.addAll(rules.getCatalogRules());

            // Hack to allow Presto Admin to access the "system" catalog for retrieving server status.
            // todo Change userRegex from ".*" to one particular user that Presto Admin will be restricted to run as
            catalogRulesBuilder.add(new CatalogAccessControlRule(
                    true,
                    false,
                    Optional.of(Pattern.compile(".*")),
                    Optional.of(Pattern.compile("system")),
                    Optional.of(Pattern.compile(".*"))));

            return new RangerSystemAccessControl(catalogRulesBuilder.build(), rules.getPrincipalUserMatchRules());
        }
    }

    @Override
    public void checkCanSetUser(Optional<Principal> principal, String userName)
    {
        requireNonNull(principal, "principal is null");
        requireNonNull(userName, "userName is null");

        if (!principalUserMatchRules.isPresent()) {
            return;
        }

        if (!principal.isPresent()) {
            denySetUser(principal, userName);
        }

        String principalName = principal.get().getName();

        for (PrincipalUserMatchRule rule : principalUserMatchRules.get()) {
            Optional<Boolean> allowed = rule.match(principalName, userName);
            if (allowed.isPresent()) {
                if (allowed.get()) {
                    return;
                }
                denySetUser(principal, userName);
            }
        }

        denySetUser(principal, userName);
    }

    @Override
    public void checkCanSetSystemSessionProperty(SystemSecurityContext context, String propertyName)
    {
    }

    @Override
    public void checkCanAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        if (!canAccessCatalog(context, catalogName)) {
            denyCatalogAccess(catalogName);
        }
    }

    @Override
    public Set<String> filterCatalogs(SystemSecurityContext context, Set<String> catalogs)
    {
        ImmutableSet.Builder<String> filteredCatalogs = ImmutableSet.builder();
        for (String catalog : catalogs) {
            if (canAccessCatalog(context, catalog)) {
                filteredCatalogs.add(catalog);
            }
        }
        return filteredCatalogs.build();
    }

    private boolean canModifyCatalog(SystemSecurityContext context, String catalogName)
    {
        for (CatalogAccessControlRule rule : catalogRules) {
            CatalogAccessControlRule.MatchResult matchResult = rule.match(context.getIdentity().getUser(), catalogName);
            if (matchResult.isAllow().isPresent() && matchResult.isAllow().get()) {
                return true;
            }
        }
        return false;
    }

    private boolean canModifyCatalogSchema(SystemSecurityContext context, String catalogName, String schema)
    {
        for (CatalogAccessControlRule rule : catalogRules) {
            CatalogAccessControlRule.MatchResult matchResult = rule.match(context.getIdentity().getUser(), catalogName, schema);
            if (matchResult.isAllow().isPresent() && matchResult.isAllow().get()) {
                return true;
            }
        }
        return false;
    }

    private boolean canAccessCatalog(SystemSecurityContext context, String catalogName)
    {
        for (CatalogAccessControlRule rule : catalogRules) {
            CatalogAccessControlRule.MatchResult matchResult = rule.match(context.getIdentity().getUser(), catalogName);

            if (matchResult.isReadOnly().isPresent() && matchResult.isReadOnly().get()) {
                return true;
            }
            else if (matchResult.isAllow().isPresent() && matchResult.isAllow().get()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkCanCreateSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!canModifyCatalog(context, schema.getCatalogName())) {
            denyCreateSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanDropSchema(SystemSecurityContext context, CatalogSchemaName schema)
    {
        if (!canModifyCatalog(context, schema.getCatalogName())) {
            denyDropSchema(schema.getSchemaName());
        }
    }

    @Override
    public void checkCanRenameSchema(SystemSecurityContext context, CatalogSchemaName schema, String newSchemaName)
    {
        if (!canModifyCatalog(context, schema.getCatalogName())) {
            denyRenameSchema(schema.getSchemaName(), newSchemaName);
        }
    }

    @Override
    public void checkCanShowSchemas(SystemSecurityContext context, String catalogName)
    {
        if (!canAccessCatalog(context, catalogName)) {
            denyShowSchemas();
        }
    }

    @Override
    public Set<String> filterSchemas(SystemSecurityContext context, String catalogName, Set<String> schemaNames)
    {
        if (!canAccessCatalog(context, catalogName)) {
            return ImmutableSet.of();
        }

        return schemaNames;
    }

    @Override
    public void checkCanCreateTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        // only support create table for now.
        if (!canModifyCatalog(context, table.getCatalogName()) &&
                !canModifyCatalogSchema(context, table.getCatalogName(), table.getSchemaTableName().getSchemaName())) {
            denyCreateTable(table.toString());
        }
    }

    @Override
    public void checkCanDropTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(context, table.getCatalogName())) {
            denyDropTable(table.toString());
        }
    }

    @Override
    public void checkCanRenameTable(SystemSecurityContext context, CatalogSchemaTableName table, CatalogSchemaTableName newTable)
    {
        if (!canModifyCatalog(context, table.getCatalogName())) {
            denyRenameTable(table.toString(), newTable.toString());
        }
    }

    @Override
    public void checkCanSetTableComment(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(context, table.getCatalogName())) {
            denyCommentTable(table.toString());
        }
    }
    @Override
    public Set<SchemaTableName> filterTables(SystemSecurityContext context, String catalogName, Set<SchemaTableName> tableNames)
    {
        if (!canAccessCatalog(context, catalogName)) {
            return ImmutableSet.of();
        }

        return tableNames;
    }

    @Override
    public List<ColumnMetadata> filterColumns(SystemSecurityContext context, CatalogSchemaTableName tableName, List<ColumnMetadata> columns)
    {
        if (!canAccessCatalog(context, tableName.getCatalogName())) {
            return ImmutableList.of();
        }

        return columns;
    }

    @Override
    public void checkCanAddColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(context, table.getCatalogName())) {
            denyAddColumn(table.toString());
        }
    }

    @Override
    public void checkCanDropColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(context, table.getCatalogName())) {
            denyDropColumn(table.toString());
        }
    }

    @Override
    public void checkCanRenameColumn(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(context, table.getCatalogName())) {
            denyRenameColumn(table.toString());
        }
    }

    @Override
    public void checkCanSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canAccessCatalog(context, table.getCatalogName())) {
            denySelectColumns(table.toString(), columns);
        }
    }

    @Override
    public void checkCanInsertIntoTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(context, table.getCatalogName())) {
            denyInsertTable(table.toString());
        }
    }

    @Override
    public void checkCanDeleteFromTable(SystemSecurityContext context, CatalogSchemaTableName table)
    {
        if (!canModifyCatalog(context, table.getCatalogName())) {
            denyDeleteTable(table.toString());
        }
    }

    @Override
    public void checkCanCreateView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!canModifyCatalog(context, view.getCatalogName())) {
            denyCreateView(view.toString());
        }
    }

    @Override
    public void checkCanDropView(SystemSecurityContext context, CatalogSchemaTableName view)
    {
        if (!canModifyCatalog(context, view.getCatalogName())) {
            denyDropView(view.toString());
        }
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(SystemSecurityContext context, CatalogSchemaTableName table, Set<String> columns)
    {
        if (!canModifyCatalog(context, table.getCatalogName())) {
            denyCreateViewWithSelect(table.toString(), context.getIdentity());
        }
    }

    @Override
    public void checkCanSetCatalogSessionProperty(SystemSecurityContext context, String catalogName, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal grantee, boolean withGrantOption)
    {
    }

    @Override
    public void checkCanRevokeTablePrivilege(SystemSecurityContext context, Privilege privilege, CatalogSchemaTableName table, PrestoPrincipal revokee, boolean grantOptionFor)
    {
    }

    @Override
    public void checkCanShowRoles(SystemSecurityContext context, String catalogName)
    {
    }
}
