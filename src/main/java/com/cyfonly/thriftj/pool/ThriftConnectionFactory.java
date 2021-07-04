package com.cyfonly.thriftj.pool;

import com.cyfonly.thriftj.failover.FailoverChecker;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;


/**
 * Thrift Connection 工厂类
 *
 * @author yunfeng.cheng
 * @create 2016-11-19
 */
public class ThriftConnectionFactory<X extends TServiceClient> implements KeyedPooledObjectFactory<ThriftServer, X> {
    private static final Logger logger = LoggerFactory.getLogger(ThriftConnectionFactory.class);

    private FailoverChecker failoverChecker;
    private final int timeout;
    private final Class<X> xClass;

    public ThriftConnectionFactory(FailoverChecker failoverChecker, int timeout, Class<X> xClass) {
        this.failoverChecker = failoverChecker;
        this.timeout = timeout;
        this.xClass = xClass;
    }

    @Override
    public PooledObject<X> makeObject(ThriftServer thriftServer) throws Exception {
        TSocket tsocket = new TSocket(thriftServer.getHost(), thriftServer.getPort());
        tsocket.setTimeout(timeout);
        tsocket.open();
        TFramedTransport transport = new TFramedTransport(tsocket);
        TBinaryProtocol tb = new TBinaryProtocol(transport);


        Constructor<X> c = xClass.getConstructor(TProtocol.class);
        X x = c.newInstance(tb);

        DefaultPooledObject<X> result = new DefaultPooledObject<>(x);
        logger.info("Make new thrift connection: {}:{}", thriftServer.getHost(), thriftServer.getPort());
        return result;
    }

    @Override
    public boolean validateObject(ThriftServer thriftServer, PooledObject<X> pooledObject) {
        boolean isValidate;
//		try {
//            X x = pooledObject.getObject();
//            if (failoverChecker == null) {
//			} else {
//				ConnectionValidator validator = failoverChecker.getConnectionValidator();
//				isValidate = x.isOpen() && (validator == null || validator.isValid(x));
//			}
//		} catch (Throwable e) {
//			logger.warn("Fail to validate tsocket: {}:{}", new Object[]{thriftServer.getHost(), thriftServer.getPort(), e});
//			isValidate = false;
//		}
//		if (failoverChecker != null && !isValidate) {
//			failoverChecker.getFailoverStrategy().fail(thriftServer);
//		}
//		logger.info("ValidateObject isValidate:{}", isValidate);

        return true;
    }

    @Override
    public void destroyObject(ThriftServer thriftServer, PooledObject<X> pooledObject) throws Exception {
        X x = pooledObject.getObject();
        if (x != null) {
            x = null;
            logger.trace("Close thrift connection: {}:{}", thriftServer.getHost(), thriftServer.getPort());
        }
    }

    @Override
    public void activateObject(ThriftServer thriftServer, PooledObject<X> pooledObject) throws Exception {
    }

    @Override
    public void passivateObject(ThriftServer arg0, PooledObject<X> pooledObject) throws Exception {
    }
}
