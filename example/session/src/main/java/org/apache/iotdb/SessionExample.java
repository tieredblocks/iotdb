/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

public class SessionExample {

  public static void main(String[] args) {

    for (int i = 0; i < 1; i++) {
      new Thread(() -> {
        try {
          insert();
        } catch (IoTDBConnectionException e) {
          e.printStackTrace();
        } catch (StatementExecutionException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }).start();

//      try {
//        Thread.sleep(10000);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
//
//      new Thread(() -> {
//        try {
//          query();
//        } catch (IoTDBConnectionException e) {
//          e.printStackTrace();
//        } catch (StatementExecutionException e) {
//          e.printStackTrace();
//        }
//      }).start();
    }

  }

  private static void insert()
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {

    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

//    while (true) {
//      Thread.sleep(5000);
      long start = System.currentTimeMillis();

      String deviceId = "root.sg1.d1";
      List<String> measurements = new ArrayList<>();
      for (int i = 0; i < 300000; i++) {
        measurements.add("s" + i);
      }

      List<String> values = new ArrayList<>();
      for (int i = 0; i < 300000; i++) {
        values.add("1");
      }

      session.insert(deviceId, start, measurements, values);
      System.out.println(
          Thread.currentThread().getName() + " write: " + (System.currentTimeMillis() - start));
//    }
  }

  private static void query() throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet;
    Session session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    while (true) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      long start = System.currentTimeMillis();

      StringBuilder builder = new StringBuilder("select last ");
      for (int c = 0; c < 49999; c++) {
        builder.append("s").append(c).append(",");
      }

      builder.append("s49999 ");
      builder.append("from root.sg1.d1");

      dataSet = session.executeQueryStatement(builder.toString());
//      System.out.println(dataSet.getColumnNames());
      int a = 0;
      while (dataSet.hasNext()) {
        a++;
        dataSet.next();
      }
      System.out.print(Thread.currentThread().getName() + " read " + a + "  ");
      System.out.println(System.currentTimeMillis() - start);
      dataSet.closeOperationHandle();
    }

  }

}