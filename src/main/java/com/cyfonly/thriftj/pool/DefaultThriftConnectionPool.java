package com.cyfonly.thriftj.pool;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * ThriftConnectionPool 的默认实现
 *
 * @author yunfeng.cheng
 * @create 2016-11-18
 */
public class DefaultThriftConnectionPool<X extends TServiceClient> implements ThriftConnectionPool<X> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultThriftConnectionPool.class);

    private final GenericKeyedObjectPool<ThriftServer, X> connections;

    public DefaultThriftConnectionPool(KeyedPooledObjectFactory<ThriftServer, X> factory, GenericKeyedObjectPoolConfig<X> config) {
        connections = new GenericKeyedObjectPool<>(factory, config);
    }

    @Override
    public X getClient(ThriftServer thriftServer) {
        try {
            X x = connections.borrowObject(thriftServer);
            thriftServer.incrGoing();
            return x;
        } catch (Exception e) {
            logger.error("Fail to get client for {}:{}", thriftServer.getHost(), thriftServer.getPort(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void returnClient(ThriftServer thriftServer, X x) {
        connections.returnObject(thriftServer, x);
        thriftServer.decrGoing();
    }

    @Override
    public void returnBrokenClient(ThriftServer thriftServer, X x) {
        try {
            connections.invalidateObject(thriftServer, x);
            thriftServer.decrGoing();
        } catch (Exception e) {
            logger.warn("Fail to invalid object:{},{}", thriftServer, x, e);
        }
    }

    @Override
    public void close() {
        connections.close();
    }

    @Override
    public void clear(ThriftServer thriftServer) {
        connections.clear(thriftServer);
    }
}
