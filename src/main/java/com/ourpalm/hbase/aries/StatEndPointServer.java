package com.ourpalm.hbase.aries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.ourpalm.hbase.aries.service.StatService;

public class StatEndPointServer extends BaseEndpointCoprocessor implements StatService {

	private final Log log = LogFactory.getLog(this.getClass());

	private final static List<String> statTypes = new ArrayList<String>(){{
		add("count");
		add("total");
	}};
	
	@Override
	public Long ScannerDefinedKey(String startKey,String stopKey,Long minValue,Long maxValue,String statType) throws IOException {

		log.info("================================== begin");
		if(!statTypes.contains(statType)) {
			statType = statTypes.get(0);
		}
		Scan scan = new Scan();
		/* version */
		scan.setStartRow(startKey.getBytes());
		scan.setStopRow(stopKey.getBytes());
		scan.setMaxVersions(1);
		
		/* batch and caching */
		scan.setBatch(0);
		scan.setCaching(100000);

		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		InternalScanner scanner = env.getRegion().getScanner(scan);

		long sum = 0;
		try {
			List<KeyValue> kvList = new ArrayList<KeyValue>();
			boolean hasMore = false;
			do {
				hasMore = scanner.next(kvList);
				for (KeyValue kv : kvList) {
					String key = new String(kv.getRow());
					Long value = Long.parseLong(key.substring(key.lastIndexOf("_") + 1, key.length()));
					if (value >= minValue && value <= maxValue) {
						if("count".equals(statType)) {
							sum++;
						} else {
							sum = sum + value;
						}
					}
				}
				kvList.clear();
			} while (hasMore);
		} finally {
			scanner.close();
		}
		log.info("================================== end");
		return sum;

	}

	@Override
	public Long ScannerDefinedKeyWithUID(String startKey,String stopKey,Long minValue, Long maxValue,String statType) throws IOException {
		// TODO Auto-generated method stub
		log.info("================================== begin");
		if(!statTypes.contains(statType)) {
			statType = statTypes.get(0);
		}
		Scan scan = new Scan();
		scan.setStartRow(startKey.getBytes());
		scan.setStopRow(stopKey.getBytes());
		scan.setMaxVersions(1);
		
		/* batch and caching */
		scan.setBatch(0);
		scan.setCaching(100000);

		RegionCoprocessorEnvironment env = (RegionCoprocessorEnvironment) getEnvironment();
		InternalScanner scanner = env.getRegion().getScanner(scan);

		long sum = 0;
		try {
			List<KeyValue> kvList = new ArrayList<KeyValue>();
			boolean hasMore = false;
			do {
				hasMore = scanner.next(kvList);
				for (KeyValue kv : kvList) {
					String key = new String(kv.getRow());
					Long value = Long.parseLong(key.substring(key.lastIndexOf("_") + 1, key.length()));
					if (value >= minValue && value <= maxValue) {
						if("count".equals(statType)) {
							sum++;
						} else {
							sum = sum + value;
						}
					}
				}
				kvList.clear();
			} while (hasMore);
		} finally {
			scanner.close();
		}
		log.info("================================== end");
		return sum;
	}
}
