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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


/**
 * Created by vishnuhr on 24/9/17.
 */
public class MaskingUtilTest {

  private String datasourceName = "test";

  @Before
  public void before()
  {
    System.setProperty("DATASPARK_PRIVACY_THRESHOLD", "21");
    System.setProperty("DATASPARK_EXTRAPOLATION_FACTOR", "1.4");
    System.setProperty("DATASPARK_DO_MASKING", "true");
    System.setProperty("DATASPARK_MASKED_METRICS", datasourceName.concat(".").concat("hello").concat(",test2.bye"));
    System.setProperty("MASK_ZERO_COUNTS", "true");
  }

  @Before
  public void after()
  {
    System.setProperty("DATASPARK_DO_MASKING", "false");
  }

  @Test
  public void testOffSwitch()
  {
    System.setProperty("DATASPARK_DO_MASKING", "false");
    Assert.assertFalse(MaskingUtil.shouldMask(new TableDataSource(datasourceName), "hello"));
  }

  @Test
  public void testMaskingWhenNullDatasource()
  {
    Assert.assertFalse(MaskingUtil.shouldMask(null, "hello"));
  }


  @Test
  public void testMaskingWhenNonTableDatasource()
  {
    Assert.assertFalse(MaskingUtil.shouldMask(new DataSource() {
      @Override
      public List<String> getNames() {
        return null;
      }
    }, "hello"));
  }

  @Test
  public void testMaskingWhenTableDatasource()
  {
    Assert.assertTrue(MaskingUtil.shouldMask(new TableDataSource(datasourceName), "hello"));
  }

  @Test
  public void testMaskingWhenMetricNotregistered()
  {
    Assert.assertFalse(MaskingUtil.shouldMask(new TableDataSource(datasourceName), "Notregistered"));
  }

  @Test
  public void testFakeHll()
  {
    Assert.assertTrue(196.09544605111853d == MaskingUtil.createFakeHyperLog(200).estimateCardinality());
  }

  @Test
  public void testPrivacyThreshold()
  {
    Assert.assertEquals(21, MaskingUtil.getPrivacyThreshold());
  }

  @Test
  public void testExtrapolationFactor()
  {
    Assert.assertTrue(1.4d == MaskingUtil.getExtrapolationFactor());
  }

  @Test
  public void testDoMaskforUnknownType()
  {
    Foo foo = new Foo();
    Object masked = MaskingUtil.doMask(foo);
    Assert.assertTrue(foo.hashCode() == masked.hashCode() && foo.equals(masked));
  }

  @Test
  public void testDoMask()
  {
    long l = 1l;
    double d = 1d;
    HyperLogLogCollector hc = MaskingUtil.createFakeHyperLog(1l);
    Assert.assertEquals(MaskingUtil.getPrivacyThreshold(), MaskingUtil.doMask(l));
    Assert.assertEquals(MaskingUtil.getPrivacyThreshold(), MaskingUtil.doMask(d));
    Assert.assertTrue(MaskingUtil.createFakeHyperLog(
        MaskingUtil.getPrivacyThreshold()).estimateCardinality() == ((HyperLogLogCollector) MaskingUtil.doMask(
        hc)).estimateCardinality());
  }

  @Test
  public void testDoMaskWhenGreaterThanPrivacy(){
    long l = MaskingUtil.getPrivacyThreshold() + 1;
    double d = MaskingUtil.getPrivacyThreshold() + 1;
    HyperLogLogCollector hc = MaskingUtil.createFakeHyperLog(MaskingUtil.getPrivacyThreshold() + 1);
    Assert.assertEquals(Math.round(l * MaskingUtil.getExtrapolationFactor()), MaskingUtil.doMask(l));
    Assert.assertEquals(Math.round(d * MaskingUtil.getExtrapolationFactor()), MaskingUtil.doMask(d));
    Assert.assertTrue(31.23701392422207 == ((HyperLogLogCollector) MaskingUtil.doMask(
        hc)).estimateCardinality());
  }

  @Test
  public void testExtrapolation(){
    Assert.assertEquals(3, MaskingUtil.extrapolate(2l));
    Assert.assertEquals(3, MaskingUtil.extrapolate(2d));
    Assert.assertEquals(3, MaskingUtil.extrapolate(2f));
  }

  @Test
  public void testApplyPrivacy(){
    Assert.assertEquals(MaskingUtil.getPrivacyThreshold(), MaskingUtil.applyPrivacyCensor(1));
    Assert.assertEquals(MaskingUtil.getPrivacyThreshold() + 100, MaskingUtil.applyPrivacyCensor(
        MaskingUtil.getPrivacyThreshold() + 100));
  }

  @Test
  public void testNoMaskingOfZeroCounts(){

    try {
      System.setProperty("MASK_ZERO_COUNTS", "false");
      HyperLogLogCollector zeroPeople = MaskingUtil.createFakeHyperLog(0);
      HyperLogLogCollector onePerson = MaskingUtil.createFakeHyperLog(1);
      System.out.println(zeroPeople.estimateCardinality());

      final boolean maskZeroes = MaskingUtil.shouldMaskZeroCounts();
      Assert.assertFalse(maskZeroes);

      Assert.assertTrue(MaskingUtil.mask(new Long(0)) == 0l);
      Assert.assertTrue(MaskingUtil.mask(new Double(0)) == 0l);
      Assert.assertTrue(MaskingUtil.mask(new Float(0)) == 0l);
      Assert.assertTrue(MaskingUtil.mask(zeroPeople).estimateCardinality() == zeroPeople.estimateCardinality());
      Assert.assertTrue(
          MaskingUtil.mask(onePerson).estimateCardinality() == MaskingUtil.createFakeHyperLog(
              MaskingUtil.getPrivacyThreshold()).estimateCardinality());
      Assert.assertFalse(MaskingUtil.mask(new Long(1)) == 0l);
      Assert.assertFalse(MaskingUtil.mask(new Double(0.1)) == 0l);
      Assert.assertFalse(MaskingUtil.mask(new Float(0.1)) == 0l);
    } finally {
      //reset it.
      System.setProperty("MASK_ZERO_COUNTS", "true");
    }
  }

  private class Foo{
    int bar = 10;
  }


}
