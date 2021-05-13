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

import org.apache.plc4x.java.api.messages.PlcSubscriptionEvent;
import org.apache.plc4x.java.api.messages.PlcSubscriptionRequest;
import org.apache.plc4x.java.api.messages.PlcSubscriptionResponse;
import org.apache.plc4x.java.api.model.PlcConsumerRegistration;
import org.apache.plc4x.java.api.model.PlcSubscriptionHandle;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.apache.plc4x.java.api.value.PlcValue;
import org.apache.plc4x.java.opcua.context.SecureChannel;
import org.apache.plc4x.java.opcua.field.OpcuaField;
import org.apache.plc4x.java.opcua.readwrite.*;
import org.apache.plc4x.java.opcua.readwrite.io.ExtensionObjectIO;
import org.apache.plc4x.java.opcua.readwrite.types.*;
import org.apache.plc4x.java.spi.ConversationContext;
import org.apache.plc4x.java.spi.generation.ParseException;
import org.apache.plc4x.java.spi.generation.ReadBuffer;
import org.apache.plc4x.java.spi.generation.WriteBuffer;
import org.apache.plc4x.java.spi.messages.DefaultPlcSubscriptionEvent;
import org.apache.plc4x.java.spi.messages.utils.ResponseItem;
import org.apache.plc4x.java.spi.model.DefaultPlcConsumerRegistration;
import org.apache.plc4x.java.spi.model.DefaultPlcSubscriptionField;
import org.apache.plc4x.java.spi.model.DefaultPlcSubscriptionHandle;
import org.apache.plc4x.java.spi.transaction.RequestTransactionManager;
import org.apache.plc4x.protocol.opcua.OpcuaProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 */
public class OpcuaSubscriptionHandle extends DefaultPlcSubscriptionHandle {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpcuaSubscriptionHandle.class);

    private Set<Consumer<PlcSubscriptionEvent>> consumers;
    private List<String> fieldNames;
    private long clientHandle;
    private SecureChannel channel;
    private PlcSubscriptionRequest subscriptionRequest;

    private AtomicBoolean destroy = new AtomicBoolean(false);
    private OpcuaProtocolLogic plcSubscriber;
    private Long subscriptionId;
    private long cycleTime;

    private final AtomicLong clientHandles = new AtomicLong(1L);

    private ConversationContext context;

    public OpcuaSubscriptionHandle(ConversationContext<OpcuaAPU> context, OpcuaProtocolLogic plcSubscriber, SecureChannel channel, PlcSubscriptionRequest subscriptionRequest, Long subscriptionId, long cycleTime) {
        super(plcSubscriber);
        this.consumers = new HashSet<>();
        this.subscriptionRequest = subscriptionRequest;
        this.fieldNames = new ArrayList<>( subscriptionRequest.getFieldNames() );
        this.channel = channel;
        this.subscriptionId = subscriptionId;
        this.plcSubscriber = plcSubscriber;
        this.cycleTime = cycleTime;
        this.context = context;
        try {
            onSubscribeCreateMonitoredItemsRequest().get();
        } catch (InterruptedException e) {
            LOGGER.info("Ah ha");
        } catch (ExecutionException e) {
            LOGGER.info("Ah ha");
        } catch (Exception e) {
            LOGGER.info("What?");
            e.printStackTrace();
        }

        startSubscriber();
    }

    private CompletableFuture<CreateMonitoredItemsResponse> onSubscribeCreateMonitoredItemsRequest()  {
        MonitoredItemCreateRequest[] requestList = new MonitoredItemCreateRequest[this.fieldNames.size()];
        for (int i = 0; i <  this.fieldNames.size(); i++) {
            final DefaultPlcSubscriptionField fieldDefaultPlcSubscription = (DefaultPlcSubscriptionField) subscriptionRequest.getField(fieldNames.get(i));

            NodeId idNode = generateNodeId((OpcuaField) fieldDefaultPlcSubscription.getPlcField());

            ReadValueId readValueId = new ReadValueId(
                idNode,
                0xD,
                OpcuaProtocolLogic.NULL_STRING,
                new QualifiedName(0, OpcuaProtocolLogic.NULL_STRING));

            MonitoringMode monitoringMode;
            switch (fieldDefaultPlcSubscription.getPlcSubscriptionType()) {
                case CYCLIC:
                    monitoringMode = MonitoringMode.monitoringModeSampling;
                    break;
                case CHANGE_OF_STATE:
                    monitoringMode = MonitoringMode.monitoringModeReporting;
                    break;
                case EVENT:
                    monitoringMode = MonitoringMode.monitoringModeReporting;
                    break;
                default:
                    monitoringMode = MonitoringMode.monitoringModeReporting;
            }

            long clientHandle = clientHandles.getAndIncrement();

            MonitoringParameters parameters = new MonitoringParameters(
                clientHandle,
                (double) cycleTime,     // sampling interval
                OpcuaProtocolLogic.NULL_EXTENSION_OBJECT,       // filter, null means use default
                1L,   // queue size
                true        // discard oldest
            );

            MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(
                readValueId, monitoringMode, parameters);

            requestList[i] = request;
        }

        CompletableFuture<CreateMonitoredItemsResponse> future = new CompletableFuture<>();

        RequestHeader requestHeader = new RequestHeader(channel.getAuthenticationToken(),
            SecureChannel.getCurrentDateTime(),
            channel.getRequestHandle(),
            0L,
            OpcuaProtocolLogic.NULL_STRING,
            SecureChannel.REQUEST_TIMEOUT_LONG,
            OpcuaProtocolLogic.NULL_EXTENSION_OBJECT);

        CreateMonitoredItemsRequest createMonitoredItemsRequest = new CreateMonitoredItemsRequest(
            requestHeader,
            subscriptionId,
            TimestampsToReturn.timestampsToReturnBoth,
            requestList.length,
            requestList
        );

        ExpandedNodeId expandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
            false,            //Server Index Specified
            new NodeIdFourByte((short) 0, Integer.valueOf(createMonitoredItemsRequest.getIdentifier())),
            null,
            null);

        ExtensionObject extObject = new ExtensionObject(
            expandedNodeId,
            null,
            createMonitoredItemsRequest);

        try {
            WriteBuffer buffer = new WriteBuffer(extObject.getLengthInBytes(), true);
            ExtensionObjectIO.staticSerialize(buffer, extObject);

            /* Functional Consumer example using inner class */
            Consumer<OpcuaMessageResponse> consumer = opcuaResponse -> {
                CreateMonitoredItemsResponse responseMessage = null;
                try {
                    responseMessage = (CreateMonitoredItemsResponse) ExtensionObjectIO.staticParse(new ReadBuffer(opcuaResponse.getMessage(), true), false).getBody();
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                // Pass the response back to the application.
                future.complete(responseMessage);

            };

            /* Functional Consumer example using inner class */
            Consumer<TimeoutException> timeout = t -> {
                LOGGER.info("Error While Sending Message");
                t.printStackTrace();
                // Pass the response back to the application.
                future.completeExceptionally(t);
            };

            /* Functional Consumer example using inner class */
            BiConsumer<OpcuaAPU, Throwable> error = (message, t) -> {
                LOGGER.info("Error While Sending Message");
                t.printStackTrace();
                // Pass the response back to the application.
                future.completeExceptionally(t);
            };
            LOGGER.info("Not Sure");
            channel.submit(context, timeout, error, consumer, buffer);

        } catch (ParseException e) {
            LOGGER.error("Unable to serialise the ReadRequest");
        }

        return future;
    }

    /**
     *
     * @return
     */
    public void startSubscriber() {
        LOGGER.info("Starting Subscription");
        CompletableFuture.supplyAsync(() -> {
            try {
                LinkedList<Long> outstandingAcknowledgements = new LinkedList<>();
                LinkedList<Long> outstandingRequests = new LinkedList<>();
                AtomicInteger sequenceNumber = new AtomicInteger(1);
                while (!this.destroy.get()) {
                    LOGGER.trace("SubscriberLoop");
                    try {
                        Thread.sleep(this.cycleTime);
                    } catch (InterruptedException e) {
                        LOGGER.trace("Interrupted Exception");
                    }

                    long requestHandle = channel.getRequestHandle();

                    RequestHeader requestHeader = new RequestHeader(channel.getAuthenticationToken(),
                        SecureChannel.getCurrentDateTime(),
                        channel.getRequestHandle(),
                        0L,
                        OpcuaProtocolLogic.NULL_STRING,
                        SecureChannel.REQUEST_TIMEOUT_LONG,
                        OpcuaProtocolLogic.NULL_EXTENSION_OBJECT);


                    //Make a copy of the outstanding requests.
                    LinkedList<Long> outstandingAcknowledgementsSnapshot = (LinkedList<Long>) outstandingAcknowledgements.clone();

                    SubscriptionAcknowledgement[] acks = new SubscriptionAcknowledgement[outstandingAcknowledgementsSnapshot.size()];;
                    int ackLength = outstandingAcknowledgementsSnapshot.size();

                    int i = 0;
                    for (long outstanding : outstandingAcknowledgementsSnapshot) {
                        acks[i] = new SubscriptionAcknowledgement(this.subscriptionId, outstanding);
                        i++;
                    }
                    outstandingAcknowledgements.removeAll(outstandingAcknowledgementsSnapshot);

                    PublishRequest publishRequest = new PublishRequest(
                        requestHeader,
                        ackLength,
                        acks
                    );

                    ExpandedNodeId extExpandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
                        false,            //Server Index Specified
                        new NodeIdFourByte((short) 0, Integer.valueOf(publishRequest.getIdentifier())),
                        null,
                        null);

                    ExtensionObject extObject = new ExtensionObject(
                        extExpandedNodeId,
                        null,
                        publishRequest);

                    try {
                        WriteBuffer buffer = new WriteBuffer(extObject.getLengthInBytes(), true);
                        ExtensionObjectIO.staticSerialize(buffer, extObject);

                        Consumer<OpcuaMessageResponse> consumer = opcuaResponse -> {
                            PublishResponse responseMessage = null;
                            try {
                                responseMessage = (PublishResponse) ExtensionObjectIO.staticParse(new ReadBuffer(opcuaResponse.getMessage(), true), false).getBody();
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }
                            outstandingRequests.remove(((ResponseHeader) responseMessage.getResponseHeader()).getRequestHandle());
                            //outstandingAcknowledgements.add(((ResponseHeader) responseMessage.getResponseHeader()).getRequestHandle());

                            for (long availableSequenceNumber : responseMessage.getAvailableSequenceNumbers()) {
                                outstandingAcknowledgements.add(availableSequenceNumber);
                            }

                            for (ExtensionObject notificationMessage : ((NotificationMessage) responseMessage.getNotificationMessage()).getNotificationData()) {
                                ExtensionObjectDefinition notification = notificationMessage.getBody();
                                if (notification instanceof DataChangeNotification) {
                                    LOGGER.info("Found a Data Change notification");
                                    ExtensionObjectDefinition[] items = ((DataChangeNotification) notification).getMonitoredItems();
                                    MonitoredItemNotification[] monitoredItems = Arrays.copyOf(items, items.length, MonitoredItemNotification[].class);
                                    onSubscriptionValue(monitoredItems);
                                } else {
                                    LOGGER.warn("Unsupported Notification type");
                                }
                            }

                            // Pass the response back to the application.

                        };

                        /* Functional Consumer example using inner class */
                        Consumer<TimeoutException> timeout = t -> {

                        };

                        /* Functional Consumer example using inner class */
                        BiConsumer<OpcuaAPU, Throwable> error = (message, t) -> {

                        };

                        outstandingRequests.add(requestHandle);
                        channel.submit(context, timeout, error, consumer, buffer);

                    } catch (ParseException e) {
                        LOGGER.warn("Unable to serialize subscription request");
                        e.printStackTrace();
                    }
                }


            } catch (Exception e) {
                LOGGER.error("Failed :(");
                e.printStackTrace();
            }
            return null;
        });
        return;
    }


    /**
     *
     * @return
     */
    public void stopSubscriber() {
        this.destroy.set(true);
    }


    public long getClientHandle() {
        return clientHandle;
    }

    /**
     * @param values
     */
    public void onSubscriptionValue(MonitoredItemNotification[] values) {
        LOGGER.info("Consumer Length {}", consumers.size());
        consumers.forEach(plcSubscriptionEventConsumer -> {
            PlcResponseCode resultCode = PlcResponseCode.OK;
            PlcValue stringItem = null;
            try {
                LinkedHashSet<String> fieldList = new LinkedHashSet<>();
                DataValue[] dataValues = new DataValue[values.length];
                int i = 0;
                for (MonitoredItemNotification value : values) {

                    fieldList.add(fieldNames.get((int) value.getClientHandle() - 1));
                    dataValues[i] = value.getValue();
                    i++;
                }
                LOGGER.info("Variant Type - {} ", dataValues[0].getValue().getVariantType());
                Map<String, ResponseItem<PlcValue>> fields = plcSubscriber.readResponse(fieldList, dataValues);
                PlcSubscriptionEvent event = new DefaultPlcSubscriptionEvent(Instant.now(), fields);
                plcSubscriptionEventConsumer.accept(event);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public PlcConsumerRegistration register(Consumer<PlcSubscriptionEvent> consumer) {
        LOGGER.info("Registering within Handle class");
        consumers.add(consumer);
        return new DefaultPlcConsumerRegistration(plcSubscriber, consumer, this);
    }

    private NodeId generateNodeId(OpcuaField field) {
        NodeId nodeId = null;
        if (field.getIdentifierType() == OpcuaIdentifierType.BINARY_IDENTIFIER) {
            nodeId = new NodeId(new NodeIdTwoByte(Short.valueOf(field.getIdentifier())));
        } else if (field.getIdentifierType() == OpcuaIdentifierType.NUMBER_IDENTIFIER) {
            nodeId = new NodeId(new NodeIdNumeric((short) field.getNamespace(), Long.valueOf(field.getIdentifier())));
        } else if (field.getIdentifierType() == OpcuaIdentifierType.GUID_IDENTIFIER) {
            nodeId = new NodeId(new NodeIdGuid((short) field.getNamespace(), field.getIdentifier()));
        } else if (field.getIdentifierType() == OpcuaIdentifierType.STRING_IDENTIFIER) {
            nodeId = new NodeId(new NodeIdString((short) field.getNamespace(), new PascalString(field.getIdentifier())));
        }
        return nodeId;
    }



}
