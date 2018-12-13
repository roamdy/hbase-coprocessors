package com.ourpalm.hbase.aries.service;

import java.io.IOException;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

public interface StatService extends CoprocessorProtocol {

	/****
	 * 统计key
	 */
	public Long ScannerDefinedKey(String startKey,String stopKey,Long minValue,Long maxValue,String statType) throws IOException;

	/***
	 * 按照UID统计key
	 */
	public Long ScannerDefinedKeyWithUID(String startKey,String stopKey,Long minValue, Long maxValue,String statType) throws IOException;

}
