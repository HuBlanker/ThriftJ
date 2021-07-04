package com.cyfonly.thriftj.failover;

import com.cyfonly.thriftj.constants.Constant;
import com.cyfonly.thriftj.pool.ThriftConnectionPool;
import com.cyfonly.thriftj.pool.ThriftServer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;


/**
 * failover 定时侦测
 *
 * @author yunfeng.cheng
 * @create 2016-11-19
 */
public class FailoverChecker<X extends TServiceClient> {
    private final Logger logger = LoggerFactory.getLogger(FailoverChecker.class);

    private volatile List<ThriftServer> serverList;
    private List<ThriftServer> backupServerList;
    private final FailoverStrategy<ThriftServer> failoverStrategy;
    private ThriftConnectionPool<X> poolProvider;
    private final ClientValidator<X> clientValidator;
    private ScheduledExecutorService checkExecutor;
    private final int serviceLevel;

    public FailoverChecker(ClientValidator<X> connectionValidator, FailoverStrategy<ThriftServer> failoverStrategy, int serviceLevel) {
        this.clientValidator = connectionValidator;
        this.failoverStrategy = failoverStrategy;
        this.serviceLevel = serviceLevel;
    }

    public void setConnectionPool(ThriftConnectionPool<X> poolProvider) {
        this.poolProvider = poolProvider;
    }

    public void startChecking() {
        if (clientValidator != null) {
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Fail Check Worker").build();
            checkExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
            checkExecutor.scheduleAtFixedRate(new Checker(), 5000, 5000, TimeUnit.MILLISECONDS);
        }
    }

    private class Checker implements Runnable {
        @Override
        public void run() {
            for (ThriftServer thriftServer : getAvailableServers(true)) {
                boolean valid = false;
                X x = poolProvider.getClient(thriftServer);
                try {
                    valid = clientValidator.isValid(x);
                } catch (Exception e) {
                    valid = false;
                    logger.warn(e.getMessage(), e);
                } finally {
                    if (x != null) {
                        if (valid) {
                            poolProvider.returnClient(thriftServer, x);
                        } else {
                            failoverStrategy.fail(thriftServer);
                            poolProvider.returnBrokenClient(thriftServer, x);
                        }
                    } else {
                        failoverStrategy.fail(thriftServer);
                    }
                }
            }
        }
    }

    public void setServerList(List<ThriftServer> serverList) {
        this.serverList = serverList;
    }

    public void setBackupServerList(List<ThriftServer> backupServerList) {
        this.backupServerList = backupServerList;
    }

    public List<ThriftServer> getAvailableServers() {
        return getAvailableServers(false);
    }

    private List<ThriftServer> getAvailableServers(boolean all) {
        List<ThriftServer> returnList = new ArrayList<>();
        Set<ThriftServer> failedServers = failoverStrategy.getFailed();
        for (ThriftServer thriftServer : serverList) {
            if (!failedServers.contains(thriftServer))
                returnList.add(thriftServer);
        }
        if (this.serviceLevel == Constant.ServiceLevel.SERVERS_ONLY) {
            return returnList;
        }
        if ((all || returnList.isEmpty()) && !backupServerList.isEmpty()) {
            for (ThriftServer thriftServer : backupServerList) {
                if (!failedServers.contains(thriftServer))
                    returnList.add(thriftServer);
            }
        }
        if (this.serviceLevel == Constant.ServiceLevel.ALL_SERVERS) {
            return returnList;
        }
        if (returnList.isEmpty()) {
            returnList.addAll(serverList);
        }
        return returnList;
    }

    public FailoverStrategy<ThriftServer> getFailoverStrategy() {
        return failoverStrategy;
    }

    public ClientValidator<X> getClientValidator() {
        return clientValidator;
    }

    public void stopChecking() {
        if (checkExecutor != null)
            checkExecutor.shutdown();
    }
}
