package org.apache.ranger.authorization.presto.authorizer.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.Identity;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author tangyun@bigo.sg
 * @date 9/25/19 5:55 PM
 */
@Slf4j
public class RangerUtils {
    private static String hostPrefix = null;
    private static final long POLICY_UPDATE_FREQUENCY = 1000 * 60 * 60 * 24;
    private static JSONObject hivePolicies = null;
    public static final String PRESTO_SOURCE = "X-Presto-Source";
    private static String userNameAndPassword = null;
    private static Lock lock = new ReentrantLock();
    private static long lastUpdateTime = System.currentTimeMillis();
    public static String getRangerData(String url) {
        HttpClient httpClient = HttpClientCreator.getHttpClient();
        HttpGet httpGet = new HttpGet(url);
        httpGet.setHeader("Accept", "application/json");
        httpGet.setHeader("X-Frame-Options", "DENY");
        httpGet.setHeader("Content-Type", "application/json");
        httpGet.setHeader("Transfer-Encoding", "chunked");
        httpGet.setHeader("Authorization", "Basic " +
                Base64.getEncoder().encodeToString(userNameAndPassword.getBytes()));
        try {
            log.info("before request http {}", url);
            HttpResponse httpResponse = httpClient.execute(httpGet);
            String response = EntityUtils.toString(httpResponse.getEntity());
            log.info("response {}", response);
            return response;
        } catch (IOException e) {
            log.error("failed to request url {}", url, e);
        }
        return null;
    }

    public static void setHostPrefix(String hostPrefix) {
        RangerUtils.hostPrefix = hostPrefix;
    }

    public static void setUserNameAndPassword(String userNameAndPassword) {
        RangerUtils.userNameAndPassword = userNameAndPassword;
    }

    public static void setHivePolicies(JSONObject hivePolicies) {
        RangerUtils.hivePolicies = hivePolicies;
    }

    public static void getPolicy() {
        if (hivePolicies == null) {
            lock.lock();
            try {
                long now = System.currentTimeMillis();
                // also update local policies everyday
                if (hivePolicies == null || now - lastUpdateTime > POLICY_UPDATE_FREQUENCY) {
                    lastUpdateTime = now;
                    String url = hostPrefix + "/service/plugins/policies/download/hivedev";
                    hivePolicies = JSONObject.parseObject(getRangerData(url));
                }
            } finally {
                lock.unlock();
            }
        }
    }

    public static List<String> getGroups(String userName) {

        String url = hostPrefix + "/service/xusers/users/userName/" + userName;
        String userInfo = getRangerData(url);
        if (userInfo == null) {
            return new ArrayList<>();
        }
        JSONObject userInfoJson = JSON.parseObject(userInfo);
        String uid = userInfoJson.getString("id");
        if (uid == null) {
            return new ArrayList<>();
        }
        url = hostPrefix + "/service/xusers/secure/users/" + uid;
        String userGroupInfo = getRangerData(url);
        if (userGroupInfo == null) {
            return new ArrayList<>();
        }
        JSONObject userGroupInfoJson = JSON.parseObject(userGroupInfo);
        List<String> groups = userGroupInfoJson.getJSONArray("groupNameList")
                .toJavaList(String.class);
        return groups;
    }

    static String getResource(Identity identity) {
        return identity.getExtraCredentials().get(PRESTO_SOURCE);
    }

    public static boolean checkPermission(Identity identity, CatalogSchemaTableName table,
                                          PrestoAccessType accessType) {
        getPolicy();
        List<String> groups = getGroups(identity.getUser());
        return checkPermission(groups, getResource(identity), identity.getUser(),
                table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), accessType);
    }

    public static boolean checkPermission(Identity identity, CatalogSchemaName schema,
                                          PrestoAccessType accessType) {
        getPolicy();
        List<String> groups = getGroups(identity.getUser());
        return checkPermission(groups, getResource(identity), identity.getUser(),
                schema.getSchemaName(),
                "", accessType);
    }

    public static boolean checkPermission(List<String> groups, String resource, String userName, String db,
                                          String table, PrestoAccessType accessType) {
        JSONArray policies = hivePolicies.getJSONArray("policies");
        groups.add(resource);

        for (Object o : policies) {
            JSONObject policy = (JSONObject) o;
            if (policy.getBoolean("isEnabled")) {
                JSONObject resources = policy.getJSONObject("resources");
                if (!isFitResource(db, table, resources)) {
                    continue;
                }
                JSONArray policyItems = policy.getJSONArray("policyItems");
                if (isFitPolicyItems(userName, groups, policyItems, accessType)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isFitPolicyItems(String user, List<String> groups,
                                           JSONArray policyItems, PrestoAccessType accessType) {
        for (Object o: policyItems) {
            JSONObject policyItem = (JSONObject) o;
            if (isFitPolicyItem(user, groups, policyItem, accessType)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isFitPolicyItem(String user, List<String> groups,
                                           JSONObject policyItem, PrestoAccessType accessType) {
        JSONArray users = policyItem.getJSONArray("users");
        JSONArray groupsJson = policyItem.getJSONArray("groups");
        if (!isFitUsers(user, users) && !isFitGroups(groups, groupsJson)) {
            return false;
        }
        JSONArray accesses = policyItem.getJSONArray("accesses");
        if (!isFitAccess(accessType, accesses)) {
            return false;
        }
        return true;
    }

    private static boolean isFitUsers(String user, JSONArray usersJson) {
        for (Object o: usersJson) {
            if (o.toString().equals(user)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isFitAccess(PrestoAccessType accessType, JSONArray accesses) {
        for (Object o: accesses) {
            JSONObject access = (JSONObject)o;
            if (access.getBoolean("isAllowed")) {
                if (access.getString("type").equals("all") ||
                        accessType.toString().equals(access.getString("type"))) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isFitGroups(List<String> groups, JSONArray groupsJson) {
        for (String group: groups) {
            if (groupsJson.contains(group)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isFitResource(String db, String table, JSONObject resources) {
        JSONObject database = resources.getJSONObject("database");
        JSONArray values = database.getJSONArray("values");
        boolean isExcludes = database.getBoolean("isExcludes");

        // no any access to all db of this policy
        if (!isFitIdentity(db, values, isExcludes)) {
            return false;
        }
        if (table == null) {
            return true;
        }
        JSONObject tables = resources.getJSONObject("table");
        values = tables.getJSONArray("values");
        isExcludes = tables.getBoolean("isExcludes");
        // no any access to all table of this policy
        if (!isFitIdentity(table, values, isExcludes)) {
            return false;
        }
        return true;
    }

    private static boolean isFitIdentity(String idntity, JSONArray values, boolean isExcludes) {
        for (Object value : values) {
            if (value.toString().equals("*") || value.toString().equals(idntity)) {
                return true ^ isExcludes;
            }
        }
        return false ^ isExcludes;
    }
}

