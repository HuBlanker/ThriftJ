package com.cyfonly.thriftj.test.thriftclient;

import com.cyfonly.thriftj.ThriftClient;
import com.cyfonly.thriftj.constants.Constant;
import com.cyfonly.thriftj.failover.ClientValidator;
import com.cyfonly.thriftj.failover.FailoverStrategy;
import com.cyfonly.thriftj.test.thriftserver.thrift.QryResult;
import com.cyfonly.thriftj.test.thriftserver.thrift.TestThriftJ;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;


/**
 * TestThriftJ.thrift Client，基于 ThriftJ 组件
 *
 * @author yunfeng.cheng
 * @create 2016-11-21
 */
public class ThriftClientTest {
    private static final Logger logger = LoggerFactory.getLogger(ThriftClientTest.class);

    private static final String servers = "127.0.0.1:10001:13,127.0.0.1:10002:26";

    public static void main(String[] args) throws TException {

        ClientValidator<TestThriftJ.Client> validator = c -> true;
        GenericKeyedObjectPoolConfig<TestThriftJ.Client> poolConfig = new GenericKeyedObjectPoolConfig<>();
        FailoverStrategy<TestThriftJ> failoverStrategy = new FailoverStrategy<>();

        final ThriftClient<TestThriftJ.Client> thriftClient = new ThriftClient<>(TestThriftJ.Client.class, servers);
        TestThriftJ.Client c = (TestThriftJ.Client) thriftClient
                .loadBalance(Constant.LoadBalance.LEAST_CONNECTION)
                .connectionValidator(validator)
                .poolConfig(poolConfig)
                .failoverStrategy(failoverStrategy)
                .connTimeout(5)
                .backupServers("")
                .serviceLevel(Constant.ServiceLevel.NOT_EMPTY)
                .start().ifaceHash(new Function<Object[], byte[]>() {
                    @Override
                    public byte[] apply(Object[] objects) {
                        Object o = objects[0];
                        Integer i = (Integer) o;
                        byte[] r = new byte[4];
                        r[3] = i.byteValue();
                        return r;
                    }
                });

        System.out.println("===============");
        try {

            //测试服务是否可用
            for (int i = 0; i < 100; i++) {
                QryResult result = c.qryTest(i);
                System.out.println("result[code=" + result.code + " msg=" + result.msg + "]");
            }
        } catch (Throwable t) {
            logger.error("-------------exception happen", t);
        }

        thriftClient.close();
    }


}
