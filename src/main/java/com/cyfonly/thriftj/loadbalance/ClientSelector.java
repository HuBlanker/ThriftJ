package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.constants.Constant;
import com.cyfonly.thriftj.exceptions.ValidationException;
import com.cyfonly.thriftj.failover.*;
import com.cyfonly.thriftj.pool.*;
import net.sf.cglib.proxy.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


/**
 * 基于 load balance 的 Client 选择器
 *
 * @author yunfeng.cheng
 * @create 2016-11-21
 */
public class ClientSelector<X extends TServiceClient> {

    private static final Logger logger = LoggerFactory.getLogger(ClientSelector.class);

    private final FailoverChecker<X> failoverChecker;
    private final DefaultThriftConnectionPool<X> poolProvider;
    private final AbstractLoadBalancer loadBalancer;
    private final Constant.LoadBalance loadBalance;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public ClientSelector(String servers, Constant.LoadBalance loadBalance, ClientValidator validator, GenericKeyedObjectPoolConfig poolConfig, FailoverStrategy strategy, int connTimeout, String backupServers, int serviceLevel,
                          Class<X> xClass) {
        this.failoverChecker = new FailoverChecker(validator, strategy, serviceLevel);
        this.poolProvider = new DefaultThriftConnectionPool(new ThriftConnectionFactory(failoverChecker, connTimeout, xClass), poolConfig);
        failoverChecker.setConnectionPool(poolProvider);
        failoverChecker.setServerList(ThriftServer.parse(servers));
        if (StringUtils.isNotEmpty(backupServers)) {
            failoverChecker.setBackupServerList(ThriftServer.parse(backupServers));
        } else {
            failoverChecker.setBackupServerList(new ArrayList<>());
        }
        failoverChecker.startChecking();
        this.loadBalance = loadBalance;
        this.loadBalancer = by(servers);
    }

    private AbstractLoadBalancer by(String servers) {
        switch (this.loadBalance) {
            case RANDOM:
                return new RandomLoadBalancer(servers);
            case ROUND_ROBIN:
                return new RoundLoadBalancer(servers);
            case RANDOM_WRIGHT:
                return new RandomWithWeightLoadBalancer(servers);
            case HASH:
            default:
                return null;
        }
    }

    @SuppressWarnings("unchecked")
    public <X extends TServiceClient> X iface(final Class<X> ifaceClass) {
        if (this.loadBalance == Constant.LoadBalance.HASH) {
            throw new ValidationException("Can not use HASH without a key.");
        }
        try {
            ClientFactory factory = new ClientFactory(ifaceClass);
            X x = (X) factory.getProxyInstance();

            return x;
        } catch (Exception e) {
            throw new RuntimeException("Fail to create proxy.", e);
        }
    }


    public List<ThriftServer> getAvaliableServers() {
        return failoverChecker.getAvailableServers();
    }

    public void close() {
        this.failoverChecker.stopChecking();
        this.poolProvider.close();
        this.loadBalancer.printStat();
    }

    public class ClientFactory implements MethodInterceptor {

        private final Class<?> target;
        AtomicReference<TProtocol> protocolRef = new AtomicReference<>();
        ThriftServer selected;
        private String key;
        private TServiceClient t;
        Enhancer en;

        public ClientFactory(Class<?> target) {
            this.target = target;
        }

        public Object getProxyInstance() {
            en = new Enhancer();
            en.setSuperclass(target);
            en.setCallback(this);

            return en.create(new Class[]{TProtocol.class}, new Object[]{protocolRef.get()});
        }

        @Override
        public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
            logger.info("before");

            // 负载均衡
            assert loadBalancer != null;
            this.selected = loadBalancer.find();
            logger.info("load balancer: {} {}", this.selected.getHost(), this.selected.getPort());

            X x;
            try {
                x = poolProvider.getClient(selected);
            } catch (RuntimeException e) {
                if (e.getCause() != null && e.getCause() instanceof TTransportException) {
                    failoverChecker.getFailoverStrategy().fail(selected);
                }
                throw e;
            }
            boolean success = false;
            try {
                Object result = methodProxy.invoke(x, objects);
                success = true;
                return result;
            } catch (Exception e) {
                logger.error("invoke method error. {} ", method.getName(), e);
            } finally {
                if (success) {
                    poolProvider.returnClient(selected, x);
                } else {
                    failoverChecker.getFailoverStrategy().fail(selected);
                    poolProvider.returnBrokenClient(selected, x);
                }
            }
            return null;
        }
    }

}
