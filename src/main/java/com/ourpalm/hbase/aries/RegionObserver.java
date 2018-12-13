package com.ourpalm.hbase.aries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ourpalm.hbase.util.DateUtil;

public class RegionObserver extends BaseRegionObserver {
	Logger logger = LoggerFactory.getLogger(RegionObserver.class);

	private byte[] indexTableName = "aries_index".getBytes();
	private byte[] statTableName = "aries_stat".getBytes();
	private byte[] targetTableName = "aries_user".getBytes();
	private byte[] indexColumnFamily = "index".getBytes();
	private byte[] valueColumn = "value".getBytes();
	private HTablePool htable_pool;

	/***
	 * init aries_index pool
	 * @param conf
	 * @throws IOException
	 */
	private void initIndexPool(Configuration conf) throws IOException {
		htable_pool = new HTablePool(conf, 100);
		HTableInterface[] tables = new HTableInterface[100];
		for (int n = 0; n < 100; n++) {
			tables[n] = htable_pool.getTable(targetTableName);
		}
		for (HTableInterface table : tables) {
			table.close();
		}
	}

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		System.out.println("#######################################################");
		System.out.println("######  Start coprocessor ...");
		System.out.println("#######################################################");
		initIndexPool(e.getConfiguration());
	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> rc,Put put, WALEdit edit, boolean writeToWAL)
			throws IOException {
		byte[] table = rc.getEnvironment().getRegion().getRegionInfo().getTableName();
		logger.info(" Prepare put table ..." + new String(table));
		// do aries_user
		if (Bytes.equals(table, targetTableName)) {
			logger.info("###### Prepare put . data is " + put.toJSON());
			for (byte[] key : put.getFamilyMap().keySet()) {
				List<KeyValue> kvs = put.getFamilyMap().get(key);
				HTableInterface indexTable = htable_pool.getTable(indexTableName);
				try {
					doAriesIndex(kvs, indexTable);
					doAriesIndexDetail(kvs, indexTable);
				} finally {
					indexTable.close();
				}
				HTableInterface statTable = htable_pool.getTable(statTableName);
				try {
					doAriesIndexDetailStat(kvs, statTable);
					doAriesIndexDetailStatDetail(kvs, statTable);
				} finally {
					statTable.close();
				}
			}
		}
	}

	/***
	 * 在aries_index插入没有UID的索引
	 */
	private void doAriesIndex(List<KeyValue> kvs, HTableInterface indexTable) {
		try {
			if (null != kvs && kvs.size() > 0) {
				String game = "default";
				String chanId = "default";
				String version = "default";
				List<KeyValue> keyValues = new ArrayList<KeyValue>();
				// init game chanId version
				for (KeyValue kv : kvs) {
					String qualifier = new String(kv.getQualifier()).toLowerCase();
		        	if(qualifier.indexOf("game".toLowerCase()) > 0) {
		        		game = new String(kv.getValue());
		        	}
		        	if(qualifier.indexOf("chanId".toLowerCase()) > 0) {
		        		chanId = new String(kv.getValue());
		        	}
		        	if(qualifier.indexOf("version".toLowerCase()) > 0) {
		        		version = new String(kv.getValue());
		        	}
					// 不处理timestamp字段
					if (qualifier.indexOf("timestamp".toLowerCase()) == -1) {
						keyValues.add(kv);
					}
				}
				List<Put> puts = new ArrayList<Put>();
				// for keyValue
				for (KeyValue kv : keyValues) {
					byte[] kv_row = kv.getRow();
					Integer uid = Integer.reverse(Integer.parseInt(new String(kv_row)));
					String column = new String(kv.getFamily()) + ":" + new String(kv.getQualifier());
					String timeRange = DateUtil.format(new Date(kv.getTimestamp()),"yyyyMMddHHmmss");
					String columnValue = new String(kv.getValue());
					String row = game + "_" + chanId + "_" + version + "_" + column + "_" + timeRange + "_" + columnValue;
					//logger.info("###### doAriesIndex Put rowKey is " + row);
					Put put = new Put(row.getBytes(), kv.getTimestamp());
					put.add(indexColumnFamily, valueColumn, String.valueOf(uid).getBytes());
					puts.add(put);
				}
				// put in hdfs
				logger.info("###### Put into hdfs... size is : " + puts.size());
				indexTable.put(puts);
				keyValues.clear();
				puts.clear();
			}
		} catch(Exception ex){
			logger.error(ExceptionUtils.getMessage(ex));
		}
	}

	/***
	 * 在aries_index插入含有UID的索引
	 */
	private void doAriesIndexDetail(List<KeyValue> kvs,HTableInterface indexTable) {
		try {
			if (null != kvs && kvs.size() > 0) {
				String game = "default";
				String chanId = "default";
				String version = "default";
				List<KeyValue> keyValues = new ArrayList<KeyValue>();
				// init game chanId version
				for (KeyValue kv : kvs) {
					String qualifier = new String(kv.getQualifier()).toLowerCase();
		        	if(qualifier.indexOf("game".toLowerCase()) > 0) {
		        		game = new String(kv.getValue());
		        	}
		        	if(qualifier.indexOf("chanId".toLowerCase()) > 0) {
		        		chanId = new String(kv.getValue());
		        	}
		        	if(qualifier.indexOf("version".toLowerCase()) > 0) {
		        		version = new String(kv.getValue());
		        	}
					// 不处理timestamp字段
		        	if (qualifier.indexOf("timestamp".toLowerCase()) == -1) {
						keyValues.add(kv);
					}
				}
				List<Put> puts = new ArrayList<Put>();
				// for keyValue
				for (KeyValue kv : keyValues) {
					byte[] kv_row = kv.getRow();
					Integer uid = Integer.reverse(Integer.parseInt(new String(kv_row)));
					String column = new String(kv.getFamily()) + ":" + new String(kv.getQualifier());
					String timeRange = DateUtil.format(new Date(kv.getTimestamp()),"yyyyMMddHHmmss");
					String columnValue = new String(kv.getValue());
					String row = game + "_" + chanId + "_" + version + "_" + uid + "_" + column + "_" + timeRange + "_" + columnValue;
					//logger.info("###### doAriesIndexDetail Put rowKey is " + row);
					Put put = new Put(row.getBytes(), kv.getTimestamp());
					put.add(indexColumnFamily, valueColumn, String.valueOf(uid).getBytes());
					puts.add(put);
				}
				// put in hdfs
				//logger.info("###### Put into hdfs... size is : " + puts.size());
				indexTable.put(puts);
				keyValues.clear();
				puts.clear();
			}
		} catch(Exception ex){
		    logger.error(ExceptionUtils.getMessage(ex));
		}
	}

	/***
	 * 处理aries_stat没有UID的索引
	 */
	private void doAriesIndexDetailStat(List<KeyValue> kvs,HTableInterface indexTable) {
		try {
			if (null != kvs && kvs.size() > 0) {
				String game = "default";
				String chanId = "default";
				String version = "default";
				List<KeyValue> keyValues = new ArrayList<KeyValue>();
				// init game chanId version
				for (KeyValue kv : kvs) {
					String qualifier = new String(kv.getQualifier()).toLowerCase();
		        	if(qualifier.indexOf("game".toLowerCase()) > 0) {
		        		game = new String(kv.getValue());
		        	}
		        	if(qualifier.indexOf("chanId".toLowerCase()) > 0) {
		        		chanId = new String(kv.getValue());
		        	}
		        	if(qualifier.indexOf("version".toLowerCase()) > 0) {
		        		version = new String(kv.getValue());
		        	}
					// 不处理timestamp字段
					if (qualifier.indexOf("timestamp".toLowerCase()) == -1) {
						keyValues.add(kv);
					}
				}
				String familyColumn = "stat";
				// for keyValue
				for (KeyValue kv : keyValues) {
					String column = new String(kv.getFamily()) + ":" + new String(kv.getQualifier());
					String timeRange_day = DateUtil.format(new Date(kv.getTimestamp()),"yyyy-MM-dd");
					String timeRange_hour = DateUtil.format(new Date(kv.getTimestamp()),"yyyy-MM-dd_HH");
					String row = game + "_" + chanId + "_" + version + "_" + timeRange_day;
					String qualifierColumn = column + ":" + timeRange_hour;
					indexTable.incrementColumnValue(row.getBytes() , familyColumn.getBytes(), qualifierColumn.getBytes(), new Long(1));
				}
				//以下是统计游戏、渠道、版本的数据量
				String timeRange_day = DateUtil.format(new Date(),"yyyy-MM-dd");
				String timeRange_hour = DateUtil.format(new Date(),"yyyy-MM-dd_HH");
				String qualifierColumn = "stat" + ":" + timeRange_hour;
				String rowGameChanIdVersion = game + "_" + chanId + "_" + version + "_" + timeRange_day;
				indexTable.incrementColumnValue(rowGameChanIdVersion.getBytes() , familyColumn.getBytes(), qualifierColumn.getBytes(), new Long(1));
				String rowGameChanId = game + "_" + chanId + "_" + timeRange_day;
				indexTable.incrementColumnValue(rowGameChanId.getBytes() , familyColumn.getBytes(), qualifierColumn.getBytes(), new Long(1));
				String rowGame = game + "_" + timeRange_day;
				indexTable.incrementColumnValue(rowGame.getBytes() , familyColumn.getBytes(), qualifierColumn.getBytes(), new Long(1));
				keyValues.clear();
			}
		} catch(Exception ex){
		    logger.error(ExceptionUtils.getMessage(ex));
		}
	}

	/***
	 * 处理aries_stat含有UID的索引
	 */
	private void doAriesIndexDetailStatDetail(List<KeyValue> kvs,HTableInterface indexTable) {
		try {
			if (null != kvs && kvs.size() > 0) {
				String game = "default";
				String chanId = "default";
				String version = "default";
				String userId = "default";
				List<KeyValue> keyValues = new ArrayList<KeyValue>();
				// init game chanId version
				for (KeyValue kv : kvs) {
					String qualifier = new String(kv.getQualifier()).toLowerCase();
		        	if(qualifier.indexOf("game".toLowerCase()) > 0) {
		        		game = new String(kv.getValue());
		        	}
		        	if(qualifier.indexOf("chanId".toLowerCase()) > 0) {
		        		chanId = new String(kv.getValue());
		        	}
		        	if(qualifier.indexOf("version".toLowerCase()) > 0) {
		        		version = new String(kv.getValue());
		        	}
		        	if(qualifier.indexOf("uid".toLowerCase()) > 0) {
		        		userId = new String(kv.getValue());
		        	}
					// 不处理timestamp字段
					if (qualifier.indexOf("timestamp".toLowerCase()) == -1) {
						keyValues.add(kv);
					}
				}
				String familyColumn = "stat";
				// for keyValue
				for (KeyValue kv : keyValues) {
					byte[] kv_row = kv.getRow();
					Integer uid = Integer.reverse(Integer.parseInt(new String(kv_row)));
					String column = new String(kv.getFamily()) + ":" + new String(kv.getQualifier());
					String timeRange_day = DateUtil.format(new Date(kv.getTimestamp()),"yyyy-MM-dd");
					String timeRange_hour = DateUtil.format(new Date(kv.getTimestamp()),"yyyy-MM-dd_HH");
					String row = game + "_" + chanId + "_" + version + "_" + uid + "_" +  timeRange_day;
					String qualifierColumn = column + ":" + timeRange_hour;
					indexTable.incrementColumnValue(row.getBytes() , familyColumn.getBytes(), qualifierColumn.getBytes(), new Long(1));
				}
				//以下是统计游戏、渠道、版本的数据量
				String timeRange_day = DateUtil.format(new Date(),"yyyy-MM-dd");
				String timeRange_hour = DateUtil.format(new Date(),"yyyy-MM-dd_HH");
				String qualifierColumn = "stat" + ":" + timeRange_hour;
				String rowGameChanIdVersionUID = game + "_" + chanId + "_" + version + "_" + userId + "_" + timeRange_day;
				indexTable.incrementColumnValue(rowGameChanIdVersionUID.getBytes() , familyColumn.getBytes(), qualifierColumn.getBytes(), new Long(1));
				String rowGameChanIdUID = game + "_" + chanId + "_"  + userId + "_" + timeRange_day;
				indexTable.incrementColumnValue(rowGameChanIdUID.getBytes() , familyColumn.getBytes(), qualifierColumn.getBytes(), new Long(1));
				String rowGameUID = game + "_"  + userId + "_" + timeRange_day;
				indexTable.incrementColumnValue(rowGameUID.getBytes() , familyColumn.getBytes(), qualifierColumn.getBytes(), new Long(1));
				keyValues.clear();
			}
		} catch(Exception ex){
		    logger.error(ExceptionUtils.getMessage(ex));
		}
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		System.out.println("#######################################################");
		System.out.println("######  Stop coprocessor ...");
		System.out.println("#######################################################");
		if (null != htable_pool) {
			htable_pool.close();
		}
	}

}
