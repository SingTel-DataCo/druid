/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.server;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by vishnuhr on 20/11/17.
 */
public class HipMetricUtilTest {


  @After
  public void before() {
    System.setProperty(HipMetricUtil.REVEAL_HIP_METRICS_KEY, "");
    System.setProperty(HipMetricUtil.REVEAL_HIP_METRICS, "false");
  }

  @Test
  public void testForHipRevealingOff() {
    Assert.assertFalse(HipMetricUtil.isRevealingHipOn());
  }

  @Test
  public void testForHipRevealingOn() {
    System.setProperty(HipMetricUtil.REVEAL_HIP_METRICS, "true");
    Assert.assertTrue(HipMetricUtil.isRevealingHipOn());
  }

  @Test
  public void testGetRevealKey() {
    System.setProperty(HipMetricUtil.REVEAL_HIP_METRICS_KEY, "abc");
    Assert.assertEquals("abc", HipMetricUtil.getRevealKey());
  }

  @Test(expected = RuntimeException.class)
  public void testKeyValidationLessThan20chars() {
    HipMetricUtil.validateServerRevealKey("aA#1aA#1aA#1aA#1aA#");//19<20
  }

  @Test(expected = RuntimeException.class)
  public void testKeyValidationNoNumber() {
    HipMetricUtil.validateServerRevealKey("aA##aA##aA##aA##aA##");
  }

  @Test(expected = RuntimeException.class)
  public void testKeyValidationNoUpperCase() {
    HipMetricUtil.validateServerRevealKey("a1##a1##a1##a1##a1##");
  }

  @Test(expected = RuntimeException.class)
  public void testKeyValidationNoLowerCase() {
    HipMetricUtil.validateServerRevealKey("A#11A#11A#11A#11A#11");
  }

  @Test(expected = RuntimeException.class)
  public void testKeyValidationSplChar() {
    HipMetricUtil.validateServerRevealKey("aA11aA11aA11aA11aA11");
  }

  @Test
  public void testKeyValidationGoodKey() {
    HipMetricUtil.validateServerRevealKey("aA11aA11aA11aA11aA11#");
  }

  @Test(expected = RuntimeException.class)
  public void testCanServerRevealHipMetricsWhenNotConfigured() {
    HipMetricUtil.canServerRevealHipMetrics();
  }

  @Test(expected = RuntimeException.class)
  public void testCanServerRevealHipMetricsWhenConfiguredWithBadKey() {
    System.setProperty(HipMetricUtil.REVEAL_HIP_METRICS, "true");
    HipMetricUtil.canServerRevealHipMetrics();
  }

  @Test
  public void testCanServerRevealHipMetricsWhenConfiguredWithGoodKey() {
    System.setProperty(HipMetricUtil.REVEAL_HIP_METRICS, "true");
    System.setProperty(HipMetricUtil.REVEAL_HIP_METRICS_KEY, "aA11aA11aA11aA11aA11#");
    HipMetricUtil.canServerRevealHipMetrics();
  }

  @Test
  public void testIsHiddenMetric() {
    Assert.assertTrue(HipMetricUtil.isHiddenMetric("abc" + HipMetricUtil.HIDDEN_METRIC_SUFFIX));
    Assert.assertFalse(HipMetricUtil.isHiddenMetric("abc"));
    Assert.assertFalse(HipMetricUtil.isHiddenMetric(" "));
    Assert.assertFalse(HipMetricUtil.isHiddenMetric(null));
    Assert.assertFalse(HipMetricUtil.isHiddenMetric(""));
  }


}
