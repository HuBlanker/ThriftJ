package com.cyfonly.thriftj.pool;

import com.cyfonly.thriftj.constants.Constant;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;


/**
 * Thrift server 包装
 *
 * @author yunfeng.cheng
 * @create 2016-11-11
 */
public final class ThriftServer {

    private static final String SERVER_SEPARATOR = ":";
    private static final String LIST_SEPARATOR = ",";

    private final String host;
    private final int port;
    private final int weight;

    public ThriftServer(String serverConfig, Constant.ServersFormat format) {
        String[] split = serverConfig.split(SERVER_SEPARATOR);
        this.host = split[0];
        this.port = Integer.parseInt(split[1]);
        if (split.length > 2) {
            this.weight = Integer.parseInt(split[2]);
        } else {
            this.weight = 1;
        }
    }

    public static List<ThriftServer> parse(String servers) {
        return parse(servers.split(LIST_SEPARATOR));
    }

    public static List<ThriftServer> parse(String[] servers) {
        List<ThriftServer> serverList = Lists.newArrayList();
        for (String item : servers) {
            serverList.add(convert(item));
        }
        return serverList;
    }

    public static ThriftServer convert(String config) {
        return new ThriftServer(config, Constant.ServersFormat.HOST_PORT);
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getWeight() {
        return weight;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getKey());
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public String getKey() {
        return host + ":" + port + ":" + weight;
    }
}
