package com.cyfonly.thriftj.loadbalance;

import com.cyfonly.thriftj.constants.Constant;
import com.cyfonly.thriftj.exceptions.NoServerAvailableException;
import com.cyfonly.thriftj.exceptions.ValidationException;
import com.cyfonly.thriftj.failover.*;
import com.cyfonly.thriftj.pool.*;
import com.cyfonly.thriftj.utils.MurmurHash3;
import com.cyfonly.thriftj.utils.ThriftClientUtil;
import com.google.common.base.Charsets;
import net.sf.cglib.proxy.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


/**
 * 基于 load balance 的 Client 选择器
 *
 * @author yunfeng.cheng
 * @create 2016-11-21
 */
public class ClientSelector<X extends TServiceClient> {

    private static final Logger logger =
            LoggerFactory.getLogger(ClientSelector.class);

    private FailoverChecker failoverChecker;
    private DefaultThriftConnectionPool poolProvider;
    private int loadBalance;
    private Class<X> xClass;

    private AtomicInteger i = new AtomicInteger(0);

    @SuppressWarnings({"rawtypes", "unchecked"})
    public ClientSelector(String servers, int loadBalance, ClientValidator validator, GenericKeyedObjectPoolConfig poolConfig, FailoverStrategy strategy, int connTimeout, String backupServers, int serviceLevel,
                          Class<X> xClass) {
        this.failoverChecker = new FailoverChecker(validator, strategy, serviceLevel);
        this.poolProvider = new DefaultThriftConnectionPool(new ThriftConnectionFactory(failoverChecker, connTimeout, xClass), poolConfig);
        failoverChecker.setConnectionPool(poolProvider);
        failoverChecker.setServerList(ThriftServer.parse(servers));
        if (StringUtils.isNotEmpty(backupServers)) {
            failoverChecker.setBackupServerList(ThriftServer.parse(backupServers));
        } else {
            failoverChecker.setBackupServerList(new ArrayList<ThriftServer>());
        }
        failoverChecker.startChecking();
        this.xClass = xClass;
    }


    protected ThriftServer getRandomClient() {
        return choose(ThriftClientUtil.randomNextInt());
    }

    protected ThriftServer getRRClient() {
        return choose(i.getAndDecrement());
    }

    protected ThriftServer getWeightClient() {
        List<ThriftServer> servers = getAvaliableServers();
        if (servers == null || servers.isEmpty()) {
            throw new NoServerAvailableException("No server available.");
        }
        int[] weights = new int[servers.size()];
        for (int i = 0; i < servers.size(); i++) {
            weights[i] = servers.get(i).getWeight();
        }
        return servers.get(ThriftClientUtil.chooseWithWeight(weights));
    }

    protected ThriftServer getHashIface(String key) {
        byte[] bytes = key.getBytes(Charsets.UTF_8);
        return choose(MurmurHash3.murmurhash3_x86_32(bytes, 0, bytes.length, 0x1234ABCD));
    }

    protected ThriftServer choose(int index) {
        List<ThriftServer> serverList = getAvaliableServers();
        if (serverList == null || serverList.isEmpty()) {
            throw new NoServerAvailableException("No server available.");
        }
        index = Math.abs(index);
        final ThriftServer selected = serverList.get(index % serverList.size());
        return selected;
    }

    private ThriftServer findServer() {
        switch (this.loadBalance) {
            case Constant.LoadBalance.RANDOM:
                return getRandomClient();
            case Constant.LoadBalance.ROUND_ROBIN:
                return getRRClient();
            case Constant.LoadBalance.WEIGHT:
                return getWeightClient();
            default:
                return getRandomClient();
        }
    }

    @SuppressWarnings("unchecked")
    public <X extends TServiceClient> X iface(final Class<X> ifaceClass, String key) {
        try {

            ClientFactory factory = new ClientFactory(ifaceClass, key);
            X x = (X) factory.getProxyInstance();

            return x;

        } catch (Exception e) {
            throw new RuntimeException("Fail to create proxy.", e);
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
        failoverChecker.stopChecking();
        poolProvider.close();
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
//            this.selected = findServer();
        }

        public ClientFactory(Class<?> target, String key) {
            this.target = target;
//            this.selected = getHashIface(key);
            this.key = key;
        }


        public Object getProxyInstance() {
            en = new Enhancer();
            en.setSuperclass(target);
            en.setCallback(this);

//            try {
//                return target.newInstance();
//            } catch (InstantiationException e) {
//                e.printStackTrace();
//            } catch (IllegalAccessException e) {
//                e.printStackTrace();
//            }
            return en.create(new Class[]{TProtocol.class}, new Object[]{protocolRef.get()});
        }

        @Override
        public Object intercept(Object o, Method method, Object[] objects, MethodProxy methodProxy) throws Throwable {
            logger.info("before");

            // 负载均衡
            this.selected = StringUtils.isEmpty(key) ? findServer() : getHashIface(key);
            logger.info("load balancer: {} {}", this.selected.getHost(), this.selected.getPort());

//            final TTransport transport = new TFramedTransport(new T)
            X x;
            try {
                x = (X) poolProvider.getClient(selected);
            } catch (RuntimeException e) {
                if (e.getCause() != null && e.getCause() instanceof TTransportException) {
                    failoverChecker.getFailoverStrategy().fail(selected);
                }
                throw e;
            }
//            TBinaryProtocol newValue = new TBinaryProtocol(transport);

//            protocolRef.set(newValue);
            en.setCallback(null);
//            Object oooo = en.create(new Class[]{TProtocol.class}, new Object[]{protocolRef.get()});

            Constructor<?> c = this.target.getConstructor(TProtocol.class);
//            Object oooo = c.newInstance(newValue);


            boolean success = false;
            try {
                Object result = methodProxy.invoke(x, objects);
                success = true;
                return result;
            } catch (Exception e) {
                logger.error("error. ", e);
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
