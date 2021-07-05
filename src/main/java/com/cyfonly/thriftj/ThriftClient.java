package com.cyfonly.thriftj;

import com.cyfonly.thriftj.constants.Constant;
import com.cyfonly.thriftj.exceptions.ValidationException;
import com.cyfonly.thriftj.failover.ClientValidator;
import com.cyfonly.thriftj.failover.FailoverStrategy;
import com.cyfonly.thriftj.loadbalance.ClientSelector;
import com.cyfonly.thriftj.pool.ThriftServer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;

import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * 基于 Apache commons-pool2 的高可用、负载均衡 Thrift client
 *
 * @author yunfeng.cheng
 * @create 2016-11-11
 */
@SuppressWarnings("rawtypes")
public class ThriftClient<X extends TServiceClient> {

    private final static int DEFAULT_CONN_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(5);

    // must
    private final String servers;
    private final Class<X> xClass;

    private Constant.LoadBalance loadBalance;
    private ClientValidator<X> validator;
    private GenericKeyedObjectPoolConfig poolConfig;
    private FailoverStrategy failoverStrategy;
    private int connTimeout;
    private String backupServers;
    private int serviceLevel;

    private ClientSelector clientSelector;

    public ThriftClient(Class<X> xClass, String servers) {
        this.xClass = xClass;
        this.servers = servers;
    }

    /**
     * 设置负载均衡策略
     *
     * @param loadBalance 负载均衡策略 {#link Constant#LoadBalance}
     * @return ThriftClient
     */
    public ThriftClient loadBalance(Constant.LoadBalance loadBalance) {
        this.loadBalance = loadBalance;
        return this;
    }

    /**
     * 设置连接验证器
     *
     * @param validator 连接验证器
     * @return ThriftClient
     */
    public ThriftClient connectionValidator(ClientValidator<X> validator) {
        this.validator = validator;
        return this;
    }

    /**
     * 设置连接池配置
     *
     * @param poolConfig 连接池配置
     * @return ThriftClient
     */
    public ThriftClient poolConfig(GenericKeyedObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
        return this;
    }

    /**
     * 设置 failover 策略
     *
     * @return ThriftClient
     */
    public ThriftClient failoverStrategy(FailoverStrategy failoverStrategy) {
        this.failoverStrategy = failoverStrategy;
        return this;
    }

    /**
     * 设置连接 timeout 时长
     *
     * @param connTimeout 连接 timeout 时长，单位秒
     * @return ThriftClient
     */
    public ThriftClient connTimeout(int connTimeout) {
        this.connTimeout = connTimeout;
        return this;
    }

    /**
     * 设置备用 Thrift server 列表
     *
     * @param backupServers 备用 Thrift server，格式 "127.0.0.1:11001,127.0.0.1:11002"
     * @return ThriftClient
     */
    public ThriftClient backupServers(String backupServers) {
        this.backupServers = backupServers;
        return this;
    }

    /**
     * 设置服务级别，根据此项配置进行服务降级
     *
     * @param serviceLevel 服务级别 {#link Constant#ServiceLevel}
     * @return
     */
    public ThriftClient serviceLevel(int serviceLevel) {
        this.serviceLevel = serviceLevel;
        return this;
    }

    /**
     * 启动 ThriftClient
     *
     * @return ThriftClient
     */
    public ThriftClient start() {
        checkAndInit();
        this.clientSelector = new ClientSelector<>(servers, loadBalance, validator, poolConfig, failoverStrategy, connTimeout, backupServers, serviceLevel, xClass);
        return this;
    }

    @SuppressWarnings("unchecked")
    public X iface() {
        return (X) this.clientSelector.iface(this.xClass);
    }

    /**
     * 获取当前可用的所有Thrift server 列表
     *
     * @return 当前可用的所有Thrift server 列表
     */
    public List<ThriftServer> getAvailableServers() {
        return clientSelector.getAvaliableServers();
    }

    /**
     * 关闭连接
     */
    public void close() {
        clientSelector.close();
    }

    private void checkAndInit() {
        if (this.servers == null || StringUtils.isEmpty(this.servers)) {
            throw new ValidationException("servers can not be null or empty.");
        }
        if (this.loadBalance == null) {
            this.loadBalance = Constant.LoadBalance.RANDOM;
        }
        if (this.validator == null) {
            this.validator = tServiceClient -> true;
        }
        if (this.poolConfig == null) {
            this.poolConfig = new GenericKeyedObjectPoolConfig();
        }
        if (this.failoverStrategy == null) {
            this.failoverStrategy = new FailoverStrategy<>();
        }
        if (this.connTimeout == 0) {
            this.connTimeout = DEFAULT_CONN_TIMEOUT;
        }
        if (!checkServiceLevel()) {
            this.serviceLevel = Constant.ServiceLevel.NOT_EMPTY;
        }
    }

    private boolean checkServiceLevel() {
        if (this.serviceLevel == Constant.ServiceLevel.SERVERS_ONLY ||
                this.serviceLevel == Constant.ServiceLevel.ALL_SERVERS ||
                this.serviceLevel == Constant.ServiceLevel.NOT_EMPTY) {
            return true;
        }
        return false;
    }
}
