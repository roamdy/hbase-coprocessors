package com.ourpalm.hbase.aries.test;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import com.ourpalm.hbase.aries.service.StatService;

public class EndPointServerTest {

	public static Configuration config;
	
	public EndPointServerTest() {
		config = HBaseConfiguration.create();
		config.set("hbase.master","master:60000");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum","master,node1,node2,node3,node4");
		
	}
	
	/**
	 * @param args
	 * @throws Throwable
	 * @throws IOException
	 */
	public static void main(String[] args) throws Throwable {

		EndPointServerTest test = new EndPointServerTest();
		
		HTable table = new HTable(config,"aries_index");
		
		final String game = "liangshan" ;
		final String chanId = "A93440";
		final Integer version = 102;
		final Integer uid = 5571323;
		final String keyName = "system-login";
		final String beginTime = "20140818193932";
		final String endTime = "20160818193932";
		final Long minValue = new Long(0);
		final Long maxValue = new Long(1000);
		
		final String start = game + "_" + chanId + "_" + version + "_" + uid + "_bhvr:definedKey:" + keyName + "_" + beginTime +"_#";
		final String stop =  game + "_" + chanId + "_" + version + "_" + uid + "_bhvr:definedKey:" + keyName + "_" + endTime +"_#";
		
		Batch.Call<StatService, Long> callable = new Batch.Call<StatService, Long>() {
			@Override
			public Long call(StatService instance) throws IOException {
				return instance.ScannerDefinedKeyWithUID(start,stop, minValue, maxValue, "count");
			}
		};
		byte[] startKey = start.getBytes();
		byte[] endKey = stop.getBytes();
		
		Map<byte[], Long> rs = table.coprocessorExec(StatService.class, startKey, endKey, callable);
		long sum = 0;
		for (Map.Entry<byte[], Long> e : rs.entrySet()) {
			sum += e.getValue().longValue();
		}
		
		table.close();
		System.out.println(sum);
	}
}


