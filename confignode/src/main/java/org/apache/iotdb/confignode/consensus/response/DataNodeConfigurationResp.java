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
package org.apache.iotdb.confignode.consensus.response;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TGlobalConfig;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.List;

public class DataNodeConfigurationResp implements DataSet {

  private TSStatus status;
  private List<TConfigNodeLocation> configNodeList;
  private Integer dataNodeId;
  private TGlobalConfig globalConfig;

  public DataNodeConfigurationResp() {
    this.dataNodeId = null;
    this.globalConfig = null;
  }

  public TSStatus getStatus() {
    return status;
  }

  public void setStatus(TSStatus status) {
    this.status = status;
  }

  public void setConfigNodeList(List<TConfigNodeLocation> configNodeList) {
    this.configNodeList = configNodeList;
  }

  public void setDataNodeId(Integer dataNodeId) {
    this.dataNodeId = dataNodeId;
  }

  public void setGlobalConfig(TGlobalConfig globalConfig) {
    this.globalConfig = globalConfig;
  }

  public void convertToRpcDataNodeRegisterResp(TDataNodeRegisterResp resp) {
    resp.setStatus(status);
    resp.setConfigNodeList(configNodeList);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || status.getCode() == TSStatusCode.DATANODE_ALREADY_REGISTERED.getStatusCode()) {
      resp.setDataNodeId(dataNodeId);
      resp.setGlobalConfig(globalConfig);
    }
  }
}
