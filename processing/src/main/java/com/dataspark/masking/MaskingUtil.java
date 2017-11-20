/*
 * Licensed to DataSpark Pte Ltd.
 * Copyright © DataSpark Pte Ltd 2014 - 2017. This software and any related documentation contain
 * confidential and proprietary information of DataSpark and its licensors (if any). Use of this
 * software and any related documentation is governed by the terms of your written agreement with
 * DataSpark. You may not use, download or install this software or any related documentation
 * without obtaining an appropriate licence agreement from DataSpark. All rights reserved.
 */
package com.dataspark.masking;

import io.druid.hll.HyperLogLogCollector;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.DataSource;
import io.druid.query.TableDataSource;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


/**
 * mask an original value.
 */
public class MaskingUtil
{

  private static final Logger log = new Logger(MaskingUtil.class);

  private static HyperLogLogCollector FAKE_HUMANS_PRIVACY_SET; //the size of the set is constant.

  static {
    log.warn("**The server has been configured to do masking at Runtime: " + maskAtRuntime());
    log.warn("**The server has been configured to do masking WHILE indexing: " + maskWhileIndexing());

    log.warn("**The server has been configured to do masking for metrics : " + metricsToMask());
    log.warn("**The server has been configured with extrapolation factor: " + getExtrapolationFactor());
    log.warn("**The server has been configured with privacy threshold: " + getPrivacyThreshold());
    log.warn("**The server has been configured to mask zero count values: " + shouldMaskZeroCounts());
    FAKE_HUMANS_PRIVACY_SET = createFakeHyperLog(getPrivacyThreshold());
  }

  //extrapolate functions
  public static long extrapolate(double value)
  {
    return extrapolate(Math.round(value));
  }

  public static long extrapolate(float value)
  {
    return extrapolate(Math.round(value));
  }

  public static long extrapolate(long value)
  {
    return Math.round(value * getExtrapolationFactor());
  }

  //privacy function
  public static long applyPrivacyCensor(long value)
  {
    final long privacyThreshold = getPrivacyThreshold();
    return value < privacyThreshold ? privacyThreshold : value;
  }

  //masking functions
  public static boolean shouldMaskAtRunTime(DataSource dataSource, String fieldName)
  {
    if(!maskAtRuntime() || dataSource == null){
      log.warn("[ MASKING_UTIL ] will not mask: " + fieldName + " of datasource: " + dataSource);
      return false;
    }

    //Masking should apply ONLY to 'TableDatasource' as 'TableDatasource' is the source of data.
    //if you have a groupBy query with 'QueryDatasource', the masking should apply to the TableDatasource in the inner query only and not on the datasource of outer query.
    //http://druid.io/docs/latest/querying/datasource.html - refer to 'Query Data Source'
    final boolean shouldMask = (dataSource instanceof TableDataSource) && !org.apache.commons.lang.StringUtils.isEmpty(
        fieldName) && metricsToMask().contains(
        ((TableDataSource) dataSource).getName().concat(".").concat(fieldName));

    log.warn("[ MASKING_UTIL ] should mask: " + fieldName + " of datasource: " + dataSource.getNames() + " = " + shouldMask + " ,DatasourceClass: " + dataSource.getClass());
    return shouldMask;
  }

  public static long mask(long value)
  {
    if(value == 0l && !shouldMaskZeroCounts()){
      return 0l;
    }
    return applyPrivacyCensor(extrapolate(value));
  }

  public static long mask(float value)
  {
    if(value == 0.0f && !shouldMaskZeroCounts()){
      return 0l;
    }
    return applyPrivacyCensor(extrapolate(value));
  }

  public static long mask(double value)
  {
    if(value == 0.0d && !shouldMaskZeroCounts()){
      return 0l;
    }
    return applyPrivacyCensor(extrapolate(value));
  }

  public static HyperLogLogCollector mask(HyperLogLogCollector hal)
  {
    if(hal.estimateCardinality() < 1.0d && !shouldMaskZeroCounts()){//its an estimate, so 0.00099 is treated as a zero.
      return hal;
    }
    final long privacyThreshold = getPrivacyThreshold();
    final double originalEstimate = hal.estimateCardinality();
    final long extrapolatedEstimate = extrapolate(originalEstimate);

    if(extrapolatedEstimate < privacyThreshold) {
      return FAKE_HUMANS_PRIVACY_SET;
    }//else return extrapolated number.

    return createFakeHyperLog(extrapolatedEstimate);
  }

  public static Object doMask(Object value)
  {
    if(value instanceof Long) {
      return mask((Long)value);
    }else if(value instanceof Double) {
      return mask((Double)value);
    }else if(value instanceof Float) {
      return mask((Float)value);
    }else if(value instanceof HyperLogLogCollector) {
      return mask((HyperLogLogCollector) value);
    }
    return value;
  }

  public static HyperLogLogCollector createFakeHyperLog(long numFake)
  {
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    try {
      MessageDigest MD5 = MessageDigest.getInstance("MD5");
      for(long i = 0 ; i < numFake ; i++) {
        //create fake people :)
        final String fakeId = "DatasparkFakeId_" + i;
        collector.add(MD5.digest(fakeId.getBytes()));
      }
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    return collector;
  }

  //info functions
  public static long getPrivacyThreshold()
  {
    return Long.parseLong(System.getProperty("DATASPARK_PRIVACY_THRESHOLD", "200"));
  }

  public static double getExtrapolationFactor()
  {
    return Double.parseDouble(System.getProperty("DATASPARK_EXTRAPOLATION_FACTOR", "1.35"));
  }

  public static boolean maskAtRuntime(){
    return Boolean.parseBoolean(System.getProperty("DATASPARK_DO_MASKING", "false"));//of by default
  }

  public static boolean shouldMaskZeroCounts(){
    //ie. report 0 trips as 0 trips , 0 humans as 0
    return Boolean.parseBoolean(System.getProperty("MASK_ZERO_COUNTS", "true"));//yes by default
  }

  public static Set<String> metricsToMask(){
    String inputMetricsToMask = System.getProperty("DATASPARK_MASKED_METRICS");
    Set<String> metricsToMask = null;
    if (org.apache.commons.lang.StringUtils.isEmpty(inputMetricsToMask)) {
      metricsToMask = new HashSet<>();
    } else {
      metricsToMask = new HashSet<String>(Arrays.asList(inputMetricsToMask.split(",")));
    }
    return metricsToMask;
  }

  //make sure to turn on revealing of hidden in plain sight metrics refer HipMetricUtil.java
  public static boolean maskWhileIndexing(){
    return Boolean.parseBoolean(System.getProperty("DATASPARK_DO_MASKING_INDEX", "false"));//off by default
  }

  public static boolean shouldMaskWhileIndexing(DataSource dataSource, String fieldName)
  {
    if(!maskWhileIndexing() || dataSource == null){
      log.warn("[ MASKING_UTIL ] will not mask: " + fieldName + " of datasource: " + dataSource);
      return false;
    }

    //Masking should apply ONLY to 'TableDatasource' as 'TableDatasource' is the source of data.
    //if you have a groupBy query with 'QueryDatasource', the masking should apply to the TableDatasource in the inner query only and not on the datasource of outer query.
    //http://druid.io/docs/latest/querying/datasource.html - refer to 'Query Data Source'
    final boolean shouldMask = (dataSource instanceof TableDataSource) && !org.apache.commons.lang.StringUtils.isEmpty(
        fieldName) && metricsToMask().contains(
        ((TableDataSource) dataSource).getName().concat(".").concat(fieldName));

    log.warn("[ MASKING_UTIL ] should mask: " + fieldName + " of datasource: " + dataSource.getNames() + " = " + shouldMask + " ,DatasourceClass: " + dataSource.getClass());
    return shouldMask;
  }

}
