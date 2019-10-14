package sg.bigo.ranger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.Identity;
import sg.bigo.utils.FileUtils;
import sg.bigo.utils.HttpClientCreator;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

/**
 * @author tangyun@bigo.sg
 * @date 9/25/19 5:55 PM
 */
@Slf4j
public class RangerUtils {
    private static String hostPrefix = null;
    private static long policyUpdateCycle = 1000 * 60 * 10;
    private static long userInfoUpdateCycle = 1000 * 60 * 10;
    private static JSONArray hivePolicies = null;
    private static JSONObject userInfo = null;
    public static final String PRESTO_SOURCE = "X-Presto-Source";
    private static String cachePath = null;
    private static String userNameAndPassword = null;
    private static String[] policyNames = null;
    private static Lock policyLock = new ReentrantLock();
    private static Lock userInfoLock = new ReentrantLock();
    public static final String POLICY_CACHE_FILE_NAME = "hive-ranger-policies.json";
    public static final String USER_INFO_CACHE_FILE_NAME = "hive-ranger-users.json";
    private static long lastPolicyUpdateTime = System.currentTimeMillis();
    private static long lastUserUpdateTime = System.currentTimeMillis();

    private static final String USER_NOT_IN_RANGER = "user_not_in_ranger";
    private static final String USER_NOT_BELONG_ANY_GROUP = "user_not_belong_any_group";
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
            HttpResponse httpResponse = httpClient.execute(httpGet);
            String response = EntityUtils.toString(httpResponse.getEntity());
            return response;
        } catch (IOException e) {
            log.error("failed to request url {}", url, e);
        }
        return null;
    }

    public static void init(Map<String, String> config) {
        RangerUtils.hostPrefix = config.get("ranger.host-port");
        requireNonNull(hostPrefix, "ranger.host-port is null");
        RangerUtils.userNameAndPassword = config.get("ranger.username-password");
        String policies = config.get("ranger.policy-names");
        requireNonNull(policies, "ranger.policy-names is null");
        RangerUtils.policyNames = policies.split(",");
        RangerUtils.cachePath = config.get("ranger.cache-path");
        requireNonNull(cachePath, "ranger.cache-path is null");
        String cycle = config.get("ranger.policy-update-cycle");
        requireNonNull(cycle, "ranger.policy-update-cycle is null");
        RangerUtils.policyUpdateCycle = Long.parseLong(cycle);
        cycle = config.get("ranger.user-update-cycle");
        requireNonNull(cycle, "ranger.user-update-cycle is null");
        RangerUtils.userInfoUpdateCycle = Long.parseLong(cycle);
    }

    public static void getPolicy() {
        long now = System.currentTimeMillis();
        // also update local policies everyday
        if (hivePolicies == null || now - lastPolicyUpdateTime > policyUpdateCycle) {
            policyLock.lock();
            try {
                if (hivePolicies == null || now - lastPolicyUpdateTime > policyUpdateCycle) {
                    lastPolicyUpdateTime = now;
                    updateAndCachePolicies();
                }
            } finally {
                policyLock.unlock();
            }
        }
    }

    private static void updateAndCachePolicies() {
        JSONArray hivePoliciesTmp = new JSONArray();
        boolean success = true;
        for (String policyName: policyNames) {
            String url = hostPrefix + "/service/plugins/policies/download/" + policyName;
            int i = 3;
            while (true) {
                try {
                    JSONObject hivePolicy = JSONObject.parseObject(getRangerData(url));
                    if (hivePolicy == null) {
                        log.warn("get policy failed ");
                        --i;
                        if (i < 0) {
                            success = false;
                            break;
                        }
                    } else {
                        hivePoliciesTmp.add(hivePolicy);
                        break;
                    }
                } catch (Exception e) {
                    log.warn("get policy failed ", e);
                    --i;
                    if (i < 0) {
                        success = false;
                        break;
                    }
                }
            }
            if (!success) {
                break;
            }
        }
        if (success) {
            hivePolicies = hivePoliciesTmp;
            FileUtils.saveBytesAsFile(hivePolicies.toJSONString().getBytes(),
                    cachePath+ "/" + POLICY_CACHE_FILE_NAME);
        } else {
            if (hivePolicies == null) {
                log.warn("cant get remote policy, use cached policy");
                String policies = new String(FileUtils.getFileAsBytes(
                        cachePath+ "/" + POLICY_CACHE_FILE_NAME));
                hivePolicies = JSON.parseArray(policies);
            }
        }
    }

    public static List<String> getGroupsFromRemote(String userName) {
        String url = hostPrefix + "/service/xusers/users/userName/" + userName;
        String userInfo = getRangerData(url);
        if (userInfo == null) {
            return new ArrayList<>();
        }
        JSONObject userInfoJson = JSON.parseObject(userInfo);
        String uid = userInfoJson.getString("id");
        if (uid == null) {
            List<String> group = new ArrayList<>();
            group.add(USER_NOT_IN_RANGER);
            return group;
        }
        url = hostPrefix + "/service/xusers/secure/users/" + uid;
        String userGroupInfo = getRangerData(url);
        if (userGroupInfo == null) {
            return new ArrayList<>();
        }
        JSONObject userGroupInfoJson = JSON.parseObject(userGroupInfo);
        List<String> group = userGroupInfoJson.getJSONArray("groupNameList")
                .toJavaList(String.class);
        if (group.size() == 0) {
            group.add(USER_NOT_BELONG_ANY_GROUP);
        }
        return group;
    }

    public static List<String> getGroups(String userName) {
        if (userInfo == null) {
            userInfoLock.lock();
            try {
                if (userInfo == null) {
                    if (FileUtils.exists(cachePath + "/" + USER_INFO_CACHE_FILE_NAME)) {
                        userInfo = JSON.parseObject(new String(FileUtils.getFileAsBytes(
                                cachePath + "/" + USER_INFO_CACHE_FILE_NAME)));
                    }
                    if (userInfo == null) {
                        userInfo = new JSONObject();
                    }
                }
            } finally {
                userInfoLock.unlock();
            }
        }

        boolean success = true;
        int i = 3;
        List<String> groups;
        while (true) {
            groups = getGroupsFromRemote(userName);
            if (groups.size() == 0) {
                --i;
                if (i < 0) {
                    success = false;
                    break;
                }
            } else {
                break;
            }
        }

        userInfoLock.lock();
        try {
            if (success) {
                userInfo.put(userName, groups);
                long now = System.currentTimeMillis();
                if (lastUserUpdateTime - now > userInfoUpdateCycle) {
                    lastUserUpdateTime = now;
                    FileUtils.saveBytesAsFile(userInfo.toJSONString().getBytes(),
                            cachePath + "/" + USER_INFO_CACHE_FILE_NAME);
                }
            } else {
                groups = userInfo.getJSONArray(userName).toJavaList(String.class);
            }
        } finally {
            userInfoLock.unlock();
        }

        return groups;
    }

    static String getResource(Identity identity) {
        return identity.getExtraCredentials().get(PRESTO_SOURCE);
    }

    public static boolean checkPermission(Identity identity, CatalogSchemaTableName table,
                                          PrestoAccessType accessType) {
        getPolicy();
        List<String> groups = getGroups(identity.getUser());
        boolean ret = checkPermission(groups, getResource(identity), identity.getUser(),
                table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), accessType);
        log.info("user {},schema {}, table {}, source {}, type {}, result {}",
                identity.getUser(), table.getSchemaTableName().getSchemaName(),
                table.getSchemaTableName().getTableName(), identity.getExtraCredentials().get(PRESTO_SOURCE),
                accessType.toString(), ret);
        return ret;
    }

    public static boolean checkPermission(String user, String schema,
                                          String table, String source,
                                          PrestoAccessType accessType) {
        getPolicy();
        List<String> groups = getGroups(user);
        return checkPermission(groups, source, user,
                schema,
                table, accessType);
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
        for (Object o : hivePolicies) {
            JSONObject hivePolicy = (JSONObject) o;
            if (checkPermission(hivePolicy, groups, resource, userName, db, table, accessType)) {
                return true;
            }
        }
        return false;
    }

    public static boolean checkPermission(JSONObject hivePolicy, List<String> groups, String resource, String userName, String db,
                                          String table, PrestoAccessType accessType) {
        JSONArray policies = hivePolicy.getJSONArray("policies");
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

