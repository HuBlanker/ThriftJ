package com.cyfonly.thriftj.test.thriftclient;

import com.cyfonly.thriftj.ThriftClient;
import com.cyfonly.thriftj.constants.Constant;
import com.cyfonly.thriftj.failover.ClientValidator;
import com.cyfonly.thriftj.failover.FailoverStrategy;
import com.cyfonly.thriftj.test.thriftserver.thrift.QryResult;
import com.cyfonly.thriftj.test.thriftserver.thrift.TestThriftJ;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
                .loadBalance(Constant.LoadBalance.RANDOM_WRIGHT)
                .connectionValidator(validator)
                .poolConfig(poolConfig)
                .failoverStrategy(failoverStrategy)
                .connTimeout(5)
                .backupServers("")
                .serviceLevel(Constant.ServiceLevel.NOT_EMPTY)
                .start().iface();

//		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
//			@Override
//			public void run() {
//				//打印从thriftClient获取到的可用服务列表
//				StringBuffer buffer = new StringBuffer();
//				List<ThriftServer> servers = thriftClient.getAvailableServers();
//				for(ThriftServer server : servers){
//					buffer.append(server.getHost()).append(":").append(server.getPort()).append(",");
//				}
//				logger.info("ThriftServers:[" + (buffer.length() == 0 ? "No avaliable server" : buffer.toString().substring(0, buffer.length()-1)) + "]");
//
//				if(buffer.length() > 0){
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


//				}
//			}
//		}, 0, 10, TimeUnit.SECONDS);

//        TSocket ts = new TSocket("127.0.0.1", 10001);
//        ts.open();
//        TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(new TFramedTransport(ts));
//
//
//        TestThriftJ.Client client = new TestThriftJ.Client(tBinaryProtocol);
//        QryResult q = client.qryTest(1);
//        System.out.println(q);
        thriftClient.close();
    }


}
