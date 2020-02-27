package io.prestosql.plugin.hive;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Subdivision;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public final class MaxMindFunction {

    private static final Logger log = Logger.get(MaxMindFunction.class);

    private static final String HADOOP_CORE_SITE_FILE_PATH = "/data/opt/druid/current/hadoop_conf/core-site.xml";

    private static final String HADOOP_HDFS_SITE_FILE_PATH = "/data/opt/druid/current/hadoop_conf/hdfs-site.xml";

    private static final LoadingCache<String, DatabaseReader> dbCache = CacheBuilder.newBuilder()
            .maximumSize(3L)
            .ticker(Ticker.systemTicker())
            .build(new CacheLoader<String, DatabaseReader>() {
                @Override
                public DatabaseReader load(String timestamp) throws Exception {
                    String fileFullName = "/data/services/udf/geoip2/GeoIP2-City_" + timestamp + ".mmdb";
                    Path p = new Path(fileFullName);
                    Configuration conf = new Configuration();
                    conf.addResource(new File(HADOOP_CORE_SITE_FILE_PATH).toURI().toURL());
                    conf.addResource(new File(HADOOP_HDFS_SITE_FILE_PATH).toURI().toURL());
                    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
                    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
                    conf.setClassLoader(MaxMindFunction.class.getClassLoader());
                    FileSystem fs = FileSystem.get(conf);
                    try (FSDataInputStream inputStream = fs.open(p)) {
                        DatabaseReader reader = new DatabaseReader.Builder(inputStream).build();
                        return reader;
                    }
                }
            });

    @Description("get geographic information by IP address.")
    @ScalarFunction("IP2Country")
    @LiteralParameters("x")
    @SqlType("array(varchar(x))")
    public static Block ip2Country(@SqlType("varchar(x)") Slice ipString) {
        SimpleDateFormat simpleFormatter = new SimpleDateFormat("yyyy-MM-dd");
        String timestamp = simpleFormatter.format(new Date());
        return ip2Country(ipString, utf8Slice(timestamp));
    }

    @Description("get geographic information by IP address and timestamp.")
    @ScalarFunction("IP2Country")
    @LiteralParameters({"x", "y"})
    @SqlType("array(varchar(x))")
    public static Block ip2Country(@SqlType("varchar(x)") Slice ipString, @SqlType("varchar(y)") Slice timestamp) {
        try {
            List<String> resList = getCountry(ipString.toStringUtf8(), timestamp.toStringUtf8());
            // wrap the result as a Block object
            BlockBuilder parts = VARCHAR.createBlockBuilder(null, resList.size());
            for (String res : resList) {
                if (res == null) {
                    res = "null";
                }
                VARCHAR.writeSlice(parts, utf8Slice(res));
            }
            return parts.build();
        } catch (IOException | GeoIp2Exception | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }

    private static List<String> getCountry(String ip, String timestamp) throws IOException, GeoIp2Exception, ExecutionException {
        long start = System.currentTimeMillis();

        DatabaseReader reader = dbCache.get(timestamp);
        InetAddress ipAddress = InetAddress.getByName(ip);

        String countryCode = null;
        String subdivisionName = null;
        String cityName = null;
        String latitude = null;
        String longitude = null;
        try {
            // Do the lookup
            CityResponse response = reader.city(ipAddress);

            Country country = response.getCountry();
            if (country != null) {
                countryCode = country.getIsoCode();
            }

            Subdivision subdivision = response.getMostSpecificSubdivision();
            if (subdivision != null) {
                subdivisionName = subdivision.getName();
            }

            City city = response.getCity();
            if (city != null) {
                cityName = city.getName();
            }

            Location location = response.getLocation();
            if (location != null) {
                Double latitudeDouble = location.getLatitude();
                Double longitudeDouble = location.getLongitude();
                if (latitudeDouble != null) {
                    latitude = latitudeDouble.toString();
                }
                if (longitudeDouble != null) {
                    longitude = longitudeDouble.toString();
                }
            }
        } catch (AddressNotFoundException e) {
            log.info(e.getMessage());
        }

        List<String> resList = new ArrayList<>();
        resList.add(countryCode);
        resList.add(subdivisionName);
        resList.add(cityName);
        resList.add(latitude);
        resList.add(longitude);

        long end = System.currentTimeMillis();
        log.info("Total Time of Reading Data from GeoIP2-City.mmdb: " + (end - start) + " ms");
        return resList;
    }

}
