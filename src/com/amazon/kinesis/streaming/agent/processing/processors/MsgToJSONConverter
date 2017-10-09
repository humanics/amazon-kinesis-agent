/*
 * Copyright 2014-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file.
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

package com.amazon.kinesis.streaming.agent.processing.processors;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;

import com.amazon.kinesis.streaming.agent.ByteBuffers;
import com.amazon.kinesis.streaming.agent.config.Configuration;
import com.amazon.kinesis.streaming.agent.processing.exceptions.DataConversionException;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IDataConverter;
import com.amazon.kinesis.streaming.agent.processing.interfaces.IJSONPrinter;
import com.amazon.kinesis.streaming.agent.processing.utils.ProcessingUtilsFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Convert Msg/Message property to json.
 *
 * Configuration of this converter looks like:
 * {
 *     "optionName": "MSG2JSON",
 *     "logFormat": "RFC3339SYSLOG"
 * }
 *
 * @author buholzer
 *
 */
public class MsgToJSONConverter implements IDataConverter {

  private IJSONPrinter jsonProducer;

  public MsgToJSONConverter(Configuration config) {
    jsonProducer = ProcessingUtilsFactory.getPrinter(config);
  }

  @Override
  public ByteBuffer convert(ByteBuffer data) throws DataConversionException {
  
    String dataStr = ByteBuffers.toString(data, StandardCharsets.UTF_8);

    ObjectMapper mapper = new ObjectMapper();
    TypeReference<LinkedHashMap<String,Object>> typeRef = 
      new TypeReference<LinkedHashMap<String,Object>>() {};

    LinkedHashMap<String,Object> dataObj = null;
    try {
      dataObj = mapper.readValue(dataStr, typeRef);
    } catch (Exception ex) {
      throw new DataConversionException("Error converting json source data to map", ex);
    }
    
    String msgStr = (String) dataObj.get("message");

    if (msgStr == null) return data;
    if (!msgStr.trim().startsWith("{")) return data;
    
    // message string starts with { let's try to parse it
    LinkedHashMap<String,Object> msgObj = null;
    try {
      msgObj = mapper.readValue(msgStr, typeRef);
    } catch (Exception ex) {
      // Can't parse it, let's just ignore it then
      return data;
    }
    
    // add the original message
    dataObj.put("messageRaw", msgStr);
    
    // replace message field
    if (msgObj.containsKey("msg")) dataObj.put("message", msgObj.remove("msg"));
    else if (msgObj.containsKey("message")) dataObj.put("message", msgObj.remove("message"));
    else if (msgObj.containsKey("@message")) dataObj.put("message", msgObj.remove("@message"));

    // Merge hash maps
    msgObj.putAll(dataObj);

    String dataJson = jsonProducer.writeAsString(msgObj) + NEW_LINE;
    return ByteBuffer.wrap(dataJson.getBytes(StandardCharsets.UTF_8));
  }
}
