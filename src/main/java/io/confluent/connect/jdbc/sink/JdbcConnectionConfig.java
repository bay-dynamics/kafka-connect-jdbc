package io.confluent.connect.jdbc.sink;

import org.apache.kafka.common.config.ConfigException;

import java.io.UnsupportedEncodingException;
import java.net.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

class JdbcConnectionConfig {
    private final URI connectionUri;
    private Map<String, List<String>> queryParams;

    private JdbcConnectionConfig(String uri) throws URISyntaxException {
        connectionUri = new URI(uri);
        queryParams = Pattern.compile("&").splitAsStream(connectionUri.getQuery())
                .map(s -> Arrays.copyOf(s.split("="), 2))
                .collect(Collectors.groupingBy(s -> decode(s[0]), mapping(s -> decode(s[1]), toList())));
    }

    public static JdbcConnectionConfig parse(String url) throws ConfigException {
        String uri = url;
        if (url.startsWith("jdbc:"))
            uri =  url.substring(5);

        try {
            return new JdbcConnectionConfig(uri);
        }
        catch (URISyntaxException e) {
            throw new ConfigException("jdbc connectionUrl is malformed");
        }
    }

    public Map<String, List<String>> queryParams() {
        return queryParams;
    }

    public boolean reWriteBatchedInserts() {
        if (queryParams().containsKey("reWriteBatchedInserts")) {
            return queryParams().get("reWriteBatchedInserts").get(0).equals("true") ? true : false;
        }
        return false;
    }

    private static String decode(final String encoded) {
        try {
            return encoded == null ? null : URLDecoder.decode(encoded, "UTF-8");
        } catch(final UnsupportedEncodingException e) {
            throw new RuntimeException("Impossible: UTF-8 is a required encoding", e);
        }
    }
}