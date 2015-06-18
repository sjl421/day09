package drpctest;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

public class DRPCClientTest {
	public static void main(String[] args) throws TException, DRPCExecutionException {
		DRPCClient client = null;
		client = new DRPCClient("localhost", 3772);
		System.out.println("开始执行DRPC客户端调用");
		for (int i = 0; i < 10; i++) {
			String tt = client.execute("exclamation", "你好");
			System.out.println("tt = " + tt);
		}
		
	}
}
