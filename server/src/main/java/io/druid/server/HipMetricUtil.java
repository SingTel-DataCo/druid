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

import java.util.regex.Pattern;

/**
 * Created by vishnuhr on 15/11/17.
 * util class for hidden in plain sight metrics.
 * concept of HIP similar to hidden columns in databases.
 * HIP = hidden in plain sight
 *
 * revealing hip metrics :
 *
 * (1) you need to set java system property 'REVEAL_HIP_METRIC' to true for the druid broker.
 * (2) then you need to set the reveal key as another java system property i.e. 'HIP_METRIC_KEY'
 *     The reveal keY should follow certain rules refer to the method 'validateServerRevealKey()'
 *
 *
 *  So in node.sh for the broker, set the following as part of JAVA_OPTS.
 *  i.e.
 *  JAVA_OPTS=" ${MASKING_OPTS} -DREVEAL_HIP_METRIC=true -DHIP_METRIC_KEY=b275822c7da805d368bcd56fb614e95C#
 *  and restart broker.
 *
 *  when the broker receives a http post druid query, you should have http header 'BEHIP' and u must set it to the same reveal key to see HIP metrics.
 */
public class HipMetricUtil {

  public static final String HIDDEN_METRIC_SUFFIX = "_hip_" ;
  public static final String ANY_HIDDEN_METRIC = "anymetric" + HIDDEN_METRIC_SUFFIX ;

  //its a java system property which has 'true'/'false' values.
  public static final String REVEAL_HIP_METRICS = "REVEAL_HIP_METRIC" ;
  //its a java system property - the key to unlock the metrics if 'DATASPARK_REVEAL_HIP_METRIC' is 'true'
  public static final String REVEAL_HIP_METRICS_KEY = "HIP_METRIC_KEY" ;
  public static final String REVEAL_HTTP_HEADER = "BEHIP" ;

  static {
//    System.out.println(new DateTime(DateTimeZone.forID("WAKT")).toString());
    System.out.println("**The server has been configured to reveal HIP metrics: " + isRevealingHipOn());
    System.out.println("**The server has been configured to reveal HIP metrics: " + isRevealingHipOn());
    try {
      canServerRevealHipMetrics();
    } catch (RuntimeException e) {
      System.out.println("** Server will NOT reveal metrics due to invalid reveal key.");
      e.printStackTrace();
    }
  }


  static boolean isHiddenMetric(String metricName) {
    if (metricName == null || "".equalsIgnoreCase(metricName.trim())){
      return false;
    }
    return metricName.endsWith(HIDDEN_METRIC_SUFFIX);
  }

  /**
   * @throws Exception if not configured
   */
  static void canServerRevealHipMetrics() throws RuntimeException {
    if (isRevealingHipOn()) {
      validateServerRevealKey(getRevealKey());
    } else {
      throw new RuntimeException("Server not configured to reveal hip metrics.");
    }
  }

  //check for a secure key
  static void validateServerRevealKey(String key)throws RuntimeException {
    if(key == null || "".equals(key.trim())) {
      throw new RuntimeException("Server has been configured with BAD reveal key.");
    }

    if(key.length() < 20) {
      throw new RuntimeException("Server has been configured with BAD reveal key..");
    }

    if(key.equals(key.toLowerCase())){
      //atleast one upper case
      throw new RuntimeException("Server has been configured with BAD reveal key...");
    }

    if(key.equals(key.toUpperCase())){
      //atleast one lower case
      throw new RuntimeException("Server has been configured with BAD reveal key....");
    }

    if(!key.matches(".*\\d.*")) {
      //has atleast 1 number
      throw new RuntimeException("Server has been configured with BAD reveal key.....");
    }

    Pattern splChars = Pattern.compile("[a-zA-Z0-9]*");
    if(splChars.matcher(key).matches()) {
      //has atleast 1 spl char
      throw new RuntimeException("Server has been configured with BAD reveal key......");
    }
  }

  static String getRevealKey(){
    return System.getProperty(REVEAL_HIP_METRICS_KEY, "");
  }

  static boolean isRevealingHipOn(){
    return Boolean.parseBoolean(System.getProperty(REVEAL_HIP_METRICS, "false"));
  }

}
