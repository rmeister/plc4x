/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
*/
package org.apache.plc4x.java.opcua.protocol;

import org.apache.plc4x.java.PlcDriverManager;
import org.apache.plc4x.java.api.PlcConnection;
import org.apache.plc4x.java.api.messages.PlcSubscriptionRequest;
import org.apache.plc4x.java.api.messages.PlcSubscriptionResponse;
import org.apache.plc4x.java.api.model.PlcSubscriptionHandle;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.apache.plc4x.java.opcua.OpcuaPlcDriverTest;
import org.assertj.core.api.Assertions;
import org.eclipse.milo.examples.server.ExampleServer;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class OpcuaSubscriptionHandleTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpcuaPlcDriverTest.class);

    private static ExampleServer exampleServer;

    // Address of local milo server
    private String miloLocalAddress = "127.0.0.1:12686/milo";
    //Tcp pattern of OPC UA
    private String opcPattern = "opcua:tcp://";

    private String paramSectionDivider = "?";
    private String paramDivider = "&";

    private String tcpConnectionAddress = opcPattern + miloLocalAddress;

    // Read only variables of milo example server of version 3.6
    private static final String BOOL_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/Boolean";
    private static final String BYTE_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/Byte";
    private static final String DOUBLE_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/Double";
    private static final String FLOAT_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/Float";
    private static final String INT16_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/Int16";
    private static final String INT32_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/Int32";
    private static final String INT64_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/Int64";
    private static final String INTEGER_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/Integer";
    private static final String SBYTE_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/SByte";
    private static final String STRING_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/String";
    private static final String UINT16_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/UInt16";
    private static final String UINT32_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/UInt32";
    private static final String UINT64_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/UInt64";
    private static final String UINTEGER_IDENTIFIER_READ_WRITE = "ns=2;s=HelloWorld/ScalarTypes/UInteger";
    private static final String DOES_NOT_EXIST_IDENTIFIER_READ_WRITE = "ns=2;i=12512623";

    @BeforeEach
    public void before() {
    }

    @AfterEach
    public void after() {

    }

    @BeforeAll
    public static void setup() {
        try {
            exampleServer = new ExampleServer();
            exampleServer.startup().get();
        } catch (Exception e) {

        }
    }

    @AfterAll
    public static void tearDown() {
        try {
            exampleServer.shutdown().get();
        } catch (Exception e) {

        }
    }

    @Test
    public void subscribeVariables() throws Exception {
        PlcConnection opcuaConnection = new PlcDriverManager().getConnection(tcpConnectionAddress);
        assert opcuaConnection.isConnected();

        PlcSubscriptionRequest.Builder builder = opcuaConnection.subscriptionRequestBuilder();

        builder.addChangeOfStateField("Bool", BOOL_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("Byte", BYTE_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("Double", DOUBLE_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("Float", FLOAT_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("Int16", INT16_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("Int32", INT32_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("Int64", INT64_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("Integer", INTEGER_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("SByte", SBYTE_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("String", STRING_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("UInt16", UINT16_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("UInt32", UINT32_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("UInt64", UINT64_IDENTIFIER_READ_WRITE);
        builder.addChangeOfStateField("UInteger", UINTEGER_IDENTIFIER_READ_WRITE);
        //builder.addChangeOfStateField("DoesNotExists", DOES_NOT_EXIST_IDENTIFIER_READ_WRITE);

        PlcSubscriptionRequest request = builder.build();

        PlcSubscriptionResponse response = request.execute().get();

        for (String subscriptionName : response.getFieldNames()) {
            final PlcSubscriptionHandle subscriptionHandle = response.getSubscriptionHandle(subscriptionName);
            subscriptionHandle.register(plcSubscriptionEvent -> {
                for (String fieldName : plcSubscriptionEvent.getFieldNames()) {
                    assert plcSubscriptionEvent.getResponseCode(fieldName).equals(PlcResponseCode.OK);
                }
            });
        }
        Thread.sleep(1000);

        opcuaConnection.close();
        assert !opcuaConnection.isConnected();

    }
}
