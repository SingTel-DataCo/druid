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

  static {
    System.out.println("**The server has been configured to do masking: " + maskingOn());
    System.out.println("**The server has been configured to do masking for metrics : " + metricsToMask());
    System.out.println("**The server has been configured with extrapolation factor: " + getExtrapolationFactor());
    System.out.println("**The server has been configured with privacy threshold: " + getPrivacyThreshold());
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
  public static boolean shouldMask(DataSource dataSource, String fieldName)
  {
    if(!maskingOn() || dataSource == null){
      System.out.println("[ MASKING_UTIL ] will not mask: " + fieldName +" of datasource: " + dataSource);
      return false;
    }

    //only do if the datasource is a table ie. true source of data and not a nested query datasource
    final boolean shouldMask = (dataSource instanceof TableDataSource) && !org.apache.commons.lang.StringUtils.isEmpty(
        fieldName) && metricsToMask().contains(
        ((TableDataSource) dataSource).getName().concat(".").concat(fieldName));

    System.out.println("[ MASKING_UTIL ] should mask: " + fieldName +" of datasource: "+ dataSource.getNames() +" = " + shouldMask + " ,DatasourceClass: " + dataSource.getClass());
    return shouldMask;
  }

  public static long mask(long value)
  {
    return applyPrivacyCensor(extrapolate(value));
  }

  public static long mask(float value)
  {
    return applyPrivacyCensor(extrapolate(value));
  }

  public static long mask(double value)
  {
    return applyPrivacyCensor(extrapolate(value));
  }

  public static HyperLogLogCollector mask(HyperLogLogCollector hal)
  {
    final long privacyThreshold = getPrivacyThreshold();
    final double originalEstimate = hal.estimateCardinality();
    final long extrapolatedEstimate = extrapolate(originalEstimate);

    if(extrapolatedEstimate < privacyThreshold) {
      return createFakeHyperLog(privacyThreshold);
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

  public static boolean maskingOn(){
    return Boolean.parseBoolean(System.getProperty("DATASPARK_DO_MASKING", "true"));
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

}
