/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.plc4x.java.opcua.context;

import io.netty.util.concurrent.CompleteFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.plc4x.java.api.exceptions.PlcConnectionException;
import org.apache.plc4x.java.api.exceptions.PlcRuntimeException;
import org.apache.plc4x.java.api.messages.PlcFieldResponse;
import org.apache.plc4x.java.api.messages.PlcReadResponse;
import org.apache.plc4x.java.opcua.config.OpcuaConfiguration;
import org.apache.plc4x.java.opcua.protocol.OpcuaSubscriptionHandle;
import org.apache.plc4x.java.opcua.readwrite.*;
import org.apache.plc4x.java.opcua.readwrite.io.ExtensionObjectIO;
import org.apache.plc4x.java.opcua.readwrite.io.OpcuaAPUIO;
import org.apache.plc4x.java.opcua.readwrite.types.*;
import org.apache.plc4x.java.spi.ConversationContext;
import org.apache.plc4x.java.spi.configuration.Configuration;
import org.apache.plc4x.java.spi.context.DriverContext;
import org.apache.plc4x.java.spi.generation.ParseException;
import org.apache.plc4x.java.spi.generation.ReadBuffer;
import org.apache.plc4x.java.spi.generation.WriteBuffer;
import org.apache.plc4x.java.spi.messages.DefaultPlcReadResponse;
import org.apache.plc4x.java.spi.transaction.RequestTransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class SecureChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecureChannel.class);
    public static final String FINAL_CHUNK = "F";
    private static final String CONTINUATION_CHUNK = "C";
    private static final String ABORT_CHUNK = "F";
    private static final int VERSION = 0;
    private static final int DEFAULT_MAX_CHUNK_COUNT = 64;

    private static final int DEFAULT_MAX_MESSAGE_SIZE = 2097152;
    private static final int DEFAULT_RECEIVE_BUFFER_SIZE = 65535;
    private static final int DEFAULT_SEND_BUFFER_SIZE = 65535;
    public static final Duration REQUEST_TIMEOUT = Duration.ofMillis(1000000);
    public static final long REQUEST_TIMEOUT_LONG = 10000L;
    private static final String PASSWORD_ENCRYPTION_ALGORITHM = "http://www.w3.org/2001/04/xmlenc#rsa-oaep";
    private static final PascalString SECURITY_POLICY_NONE = new PascalString("http://opcfoundation.org/UA/SecurityPolicy#None");
    protected static final PascalString NULL_STRING = new PascalString( "");
    private static final PascalByteString NULL_BYTE_STRING = new PascalByteString( -1, null);
    private static ExpandedNodeId NULL_EXPANDED_NODEID = new ExpandedNodeId(false,
        false,
        new NodeIdTwoByte((short) 0),
        null,
        null
    );
    protected static final ExtensionObject NULL_EXTENSION_OBJECT = new ExtensionObject(
        NULL_EXPANDED_NODEID,
        new ExtensionObjectEncodingMask(false, false, false),
        new NullExtension());               // Body
    private static final long EPOCH_OFFSET = 116444736000000000L;         //Offset between OPC UA epoch time and linux epoch time.

    private static final PascalString APPLICATION_URI = new PascalString("urn:apache:plc4x:client");
    private static final PascalString PRODUCT_URI = new PascalString("urn:apache:plc4x:client");
    private static final PascalString APPLICATION_TEXT = new PascalString("OPCUA client for the Apache PLC4X:PLC4J project");
    private static final long DEFAULT_CONNECTION_LIFETIME = 36000000;


    private final String sessionName = "UaSession:" + APPLICATION_TEXT.getStringValue() + ":" + RandomStringUtils.random(20, true, true);
    private final byte[] clientNonce = RandomUtils.nextBytes(40);

    private AtomicInteger requestHandleGenerator = new AtomicInteger(1);

    private PascalString policyId;
    private PascalString endpoint;
    private boolean discovery;
    private String username;
    private String password;
    private String certFile;
    private String securityPolicy;
    private String keyStoreFile;
    private CertificateKeyPair ckp;
    private PascalByteString publicCertificate;
    private PascalByteString thumbprint;
    private boolean isEncrypted;
    private byte[] senderCertificate = null;
    private byte[] senderNonce = null;
    private PascalByteString certificateThumbprint = null;
    private boolean checkedEndpoints = false;
    private EncryptionHandler encryptionHandler = null;
    private OpcuaConfiguration configuration;


    private AtomicInteger channelId = new AtomicInteger(1);
    private AtomicInteger tokenId = new AtomicInteger(1);


    private NodeIdTypeDefinition authenticationToken = new NodeIdTwoByte((short) 0);
    private DriverContext driverContext;
    ConversationContext<OpcuaAPU> context;
    private SecureChannelTransactionManager channelTransactionManager = new SecureChannelTransactionManager();
    private RequestTransactionManager tm;

    private Boolean isConnected;
    private long lifetime = DEFAULT_CONNECTION_LIFETIME;

    private CompletableFuture<Void> keepAlive;

    public SecureChannel(DriverContext driverContext, OpcuaConfiguration configuration) {
        this.driverContext = driverContext;
        this.configuration = configuration;

        this.endpoint = new PascalString(configuration.getEndpoint());
        this.discovery = configuration.isDiscovery();
        this.username = configuration.getUsername();
        this.password = configuration.getPassword();
        this.certFile = configuration.getCertDirectory();
        this.securityPolicy = "http://opcfoundation.org/UA/SecurityPolicy#" + configuration.getSecurityPolicy();
        this.ckp = configuration.getCertificateKeyPair();

        if (configuration.getSecurityPolicy().equals("Basic256Sha256")) {
            //Sender Certificate gets populated during the discover phase when encryption is enabled.
            this.senderCertificate = configuration.getSenderCertificate();
            this.encryptionHandler = new EncryptionHandler(this.ckp, this.senderCertificate);
            try {
                this.publicCertificate = new PascalByteString(this.ckp.getCertificate().getEncoded().length, this.ckp.getCertificate().getEncoded());
                this.isEncrypted = true;
            } catch (CertificateEncodingException e) {
                throw new PlcRuntimeException("Failed to encode the certificate");
            }
            this.thumbprint = configuration.getThumbprint();
        } else {
            this.publicCertificate = NULL_BYTE_STRING;
            this.thumbprint = NULL_BYTE_STRING;
            this.isEncrypted = false;
        }
        this.keyStoreFile = configuration.getKeyStoreFile();
        this.isConnected = false;


        // Initialize Transaction Manager.

        this.tm = new RequestTransactionManager(1);

    }

    public void submit(ConversationContext<OpcuaAPU> context, Consumer<TimeoutException> onTimeout, BiConsumer<OpcuaAPU, Throwable> error, Consumer<OpcuaMessageResponse> consumer, WriteBuffer buffer) {
        int transactionId = channelTransactionManager.getTransactionIdentifier();

        OpcuaMessageRequest messageRequest = new OpcuaMessageRequest(FINAL_CHUNK,
            channelId.get(),
            tokenId.get(),
            transactionId,
            transactionId,
            buffer.getData());

        final OpcuaAPU apu;
        try {
            if (this.isEncrypted) {
                apu = OpcuaAPUIO.staticParse(encryptionHandler.encodeMessage(messageRequest, buffer.getData()), false);
            } else {
                apu = new OpcuaAPU(messageRequest);
            }
        } catch (ParseException e) {
           throw new PlcRuntimeException("Unable to encrypt message before sending");
        }

        Consumer<Integer> requestConsumer = t -> {
            try {
                context.sendRequest(apu)
                    .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                    .onTimeout(onTimeout)
                    .onError(error)
                    .check(p -> p.getMessage() instanceof OpcuaMessageResponse)
                    .unwrap(p -> (OpcuaMessageResponse) p.getMessage())
                    .handle(opcuaResponse -> {
                        try {
                            if (this.isEncrypted) {
                                opcuaResponse = (OpcuaMessageResponse) OpcuaAPUIO.staticParse(encryptionHandler.decodeMessage(opcuaResponse, opcuaResponse.getMessage()), true).getMessage();
                            }
                        } catch (ParseException e) {
                            throw new PlcRuntimeException("Error while decoding message");
                        }
                        consumer.accept(opcuaResponse);
                    });
            } catch (Exception e) {
                throw new PlcRuntimeException("Error while sending message");
            }
        };
        LOGGER.debug("Submitting Transaction to TransactionManager {}", transactionId);
        channelTransactionManager.submit(requestConsumer, transactionId);
    }

    public void onConnect(ConversationContext<OpcuaAPU> context) {
        // Only the TCP transport supports login.
        LOGGER.info("Opcua Driver running in ACTIVE mode.");
        this.context = context;

        OpcuaHelloRequest hello = new OpcuaHelloRequest(FINAL_CHUNK,
            VERSION,
            DEFAULT_RECEIVE_BUFFER_SIZE,
            DEFAULT_SEND_BUFFER_SIZE,
            DEFAULT_MAX_MESSAGE_SIZE,
            DEFAULT_MAX_CHUNK_COUNT,
            this.endpoint);

        Consumer<Integer> requestConsumer = t -> {
            context.sendRequest(new OpcuaAPU(hello))
                .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                .check(p -> p.getMessage() instanceof OpcuaAcknowledgeResponse)
                .unwrap(p -> (OpcuaAcknowledgeResponse) p.getMessage())
                .handle(opcuaAcknowledgeResponse -> {
                    LOGGER.debug("Got Hello Response Connection Response");
                    onConnectOpenSecureChannel(context, opcuaAcknowledgeResponse);
                });
        };
        channelTransactionManager.submit(requestConsumer, channelTransactionManager.getTransactionIdentifier());
    }

    public void onConnectOpenSecureChannel(ConversationContext<OpcuaAPU> context, OpcuaAcknowledgeResponse opcuaAcknowledgeResponse) {

        int transactionId = channelTransactionManager.getTransactionIdentifier();

        RequestHeader requestHeader = new RequestHeader(new NodeId(authenticationToken),
            getCurrentDateTime(),
            0L,                                         //RequestHandle
            0L,
            NULL_STRING,
            REQUEST_TIMEOUT_LONG,
            NULL_EXTENSION_OBJECT);

        OpenSecureChannelRequest openSecureChannelRequest = null;
        if (this.isEncrypted) {
            openSecureChannelRequest = new OpenSecureChannelRequest(
                requestHeader,
                VERSION,
                SecurityTokenRequestType.securityTokenRequestTypeIssue,
                MessageSecurityMode.messageSecurityModeSignAndEncrypt,
                new PascalByteString(clientNonce.length, clientNonce),
                lifetime);
        } else {
            openSecureChannelRequest = new OpenSecureChannelRequest(
                requestHeader,
                VERSION,
                SecurityTokenRequestType.securityTokenRequestTypeIssue,
                MessageSecurityMode.messageSecurityModeNone,
                NULL_BYTE_STRING,
                lifetime);
        }

        ExpandedNodeId expandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
            false,            //Server Index Specified
            new NodeIdFourByte((short) 0, Integer.valueOf(openSecureChannelRequest.getIdentifier())),
            null,
            null);

        ExtensionObject extObject = new ExtensionObject(
            expandedNodeId,
            null,
            openSecureChannelRequest);

        try {
            WriteBuffer buffer = new WriteBuffer(extObject.getLengthInBytes(), true);
            ExtensionObjectIO.staticSerialize(buffer, extObject);

            OpcuaOpenRequest openRequest = new OpcuaOpenRequest(FINAL_CHUNK,
                0,
                new PascalString(this.securityPolicy),
                this.publicCertificate,
                this.thumbprint,
                transactionId,
                transactionId,
                buffer.getData());

            final OpcuaAPU apu;

            if (this.isEncrypted) {
                apu = OpcuaAPUIO.staticParse(encryptionHandler.encodeMessage(openRequest, buffer.getData()), false);
            } else {
                apu = new OpcuaAPU(openRequest);
            }

            Consumer<Integer> requestConsumer = t -> {
                context.sendRequest(apu)
                    .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                    .check(p -> p.getMessage() instanceof OpcuaOpenResponse)
                    .unwrap(p -> (OpcuaOpenResponse) p.getMessage())
                    .handle(opcuaOpenResponse -> {
                        try {
                            if (this.isEncrypted) {
                                opcuaOpenResponse = (OpcuaOpenResponse) OpcuaAPUIO.staticParse(encryptionHandler.decodeMessage(opcuaOpenResponse, opcuaOpenResponse.getMessage()), true).getMessage();
                            }
                            ReadBuffer readBuffer = new ReadBuffer(opcuaOpenResponse.getMessage(), true);
                            ExtensionObject message = ExtensionObjectIO.staticParse(readBuffer, false);

                            if (message.getBody() instanceof ServiceFault) {
                                ServiceFault fault = (ServiceFault) message.getBody();
                                LOGGER.error("Failed to connect to opc ua server for the following reason:- {}, {}", ((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode(), OpcuaStatusCodes.enumForValue(((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode()));
                            } else {
                                LOGGER.debug("Got Secure Response Connection Response");
                                try {
                                    onConnectCreateSessionRequest(context, opcuaOpenResponse, (OpenSecureChannelResponse) message.getBody());
                                } catch (PlcConnectionException e) {
                                    LOGGER.error("Error occurred while connecting to OPC UA server");
                                    e.printStackTrace();
                                }
                            }
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    });
            };
            LOGGER.info("Submitting OpenSecureChannel with id of {}", transactionId);
            channelTransactionManager.submit(requestConsumer, transactionId);
        } catch (ParseException e) {
            LOGGER.error("Unable to to Parse Open Secure Request");
        }
    }

    public void onConnectCreateSessionRequest(ConversationContext<OpcuaAPU> context, OpcuaOpenResponse opcuaOpenResponse, OpenSecureChannelResponse openSecureChannelResponse) throws PlcConnectionException {

        certificateThumbprint = opcuaOpenResponse.getReceiverCertificateThumbprint();
        tokenId.set((int) ((ChannelSecurityToken) openSecureChannelResponse.getSecurityToken()).getTokenId());
        channelId.set((int) ((ChannelSecurityToken) openSecureChannelResponse.getSecurityToken()).getChannelId());


        int transactionId = channelTransactionManager.getTransactionIdentifier();

        Integer nextSequenceNumber = opcuaOpenResponse.getSequenceNumber() + 1;
        Integer nextRequestId = opcuaOpenResponse.getRequestId() + 1;

        if (!(transactionId == nextSequenceNumber)) {
            throw new PlcConnectionException("Sequence number isn't as expected, we might have missed a packet. - " +  transactionId + " != " + nextSequenceNumber);
        }

        RequestHeader requestHeader = new RequestHeader(new NodeId(authenticationToken),
            getCurrentDateTime(),
            0L,
            0L,
            NULL_STRING,
            REQUEST_TIMEOUT_LONG,
            NULL_EXTENSION_OBJECT);

        LocalizedText applicationName = new LocalizedText(
            true,
            true,
            new PascalString("en"),
            APPLICATION_TEXT);

        PascalString gatewayServerUri = NULL_STRING;
        PascalString discoveryProfileUri = NULL_STRING;
        int noOfDiscoveryUrls = -1;
        PascalString[] discoveryUrls = new PascalString[0];

        ApplicationDescription clientDescription = new ApplicationDescription(APPLICATION_URI,
            PRODUCT_URI,
            applicationName,
            ApplicationType.applicationTypeClient,
            gatewayServerUri,
            discoveryProfileUri,
            noOfDiscoveryUrls,
            discoveryUrls);

        CreateSessionRequest createSessionRequest = new CreateSessionRequest(
            requestHeader,
            clientDescription,
            NULL_STRING,
            this.endpoint,
            new PascalString(sessionName),
            new PascalByteString(clientNonce.length, clientNonce),
            NULL_BYTE_STRING,
            120000L,
            0L);

        ExpandedNodeId expandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
            false,            //Server Index Specified
            new NodeIdFourByte((short) 0, Integer.valueOf(createSessionRequest.getIdentifier())),
            null,
            null);

        ExtensionObject extObject = new ExtensionObject(
            expandedNodeId,
            null,
            createSessionRequest);

        try {
            WriteBuffer buffer = new WriteBuffer(extObject.getLengthInBytes(), true);
            ExtensionObjectIO.staticSerialize(buffer, extObject);

            OpcuaMessageRequest messageRequest = new OpcuaMessageRequest(FINAL_CHUNK,
                channelId.get(),
                tokenId.get(),
                nextSequenceNumber,
                nextRequestId,
                buffer.getData());

            final OpcuaAPU apu;

            if (this.isEncrypted) {
                apu = OpcuaAPUIO.staticParse(encryptionHandler.encodeMessage(messageRequest, buffer.getData()), false);
            } else {
                apu = new OpcuaAPU(messageRequest);
            }

            Consumer<Integer> requestConsumer = t -> {
                context.sendRequest(apu)
                    .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                    .check(p -> p.getMessage() instanceof OpcuaMessageResponse)
                    .unwrap(p -> (OpcuaMessageResponse) p.getMessage())
                    .handle(opcuaMessageResponse -> {
                        try {
                            ExtensionObject message = ExtensionObjectIO.staticParse(new ReadBuffer(opcuaMessageResponse.getMessage(), true), false);
                            if (message.getBody() instanceof ServiceFault) {
                                ServiceFault fault = (ServiceFault) message.getBody();
                                LOGGER.error("Failed to connect to opc ua server for the following reason:- {}, {}", ((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode(), OpcuaStatusCodes.enumForValue(((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode()));
                            } else {
                                LOGGER.debug("Got Create Session Response Connection Response");
                                try {
                                    onConnectActivateSessionRequest(context, opcuaMessageResponse, (CreateSessionResponse) message.getBody());
                                } catch (PlcConnectionException e) {
                                    LOGGER.error("Error occurred while connecting to OPC UA server");
                                }
                            }
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                    });
            };

            channelTransactionManager.submit(requestConsumer, transactionId);
        } catch (ParseException e) {
            LOGGER.error("Unable to to Parse Create Session Request");
        }
    }

    private void onConnectActivateSessionRequest(ConversationContext<OpcuaAPU> context, OpcuaMessageResponse opcuaMessageResponse, CreateSessionResponse sessionResponse) throws PlcConnectionException {

        senderCertificate = sessionResponse.getServerCertificate().getStringValue();
        senderNonce = sessionResponse.getServerNonce().getStringValue();

        for (ExtensionObjectDefinition extensionObject: sessionResponse.getServerEndpoints()) {
            EndpointDescription endpointDescription = (EndpointDescription) extensionObject;
            LOGGER.trace("{} - {}", endpointDescription.getEndpointUrl().getStringValue(), this.endpoint.getStringValue());
            if (endpointDescription.getEndpointUrl().getStringValue().equals(this.endpoint.getStringValue())) {
                for (ExtensionObjectDefinition userTokenCast :  endpointDescription.getUserIdentityTokens()) {
                    UserTokenPolicy identityToken = (UserTokenPolicy) userTokenCast;
                    if (identityToken.getTokenType() == UserTokenType.userTokenTypeAnonymous) {
                        if (this.username == null) {
                            policyId = identityToken.getPolicyId();
                        }
                    } else if (identityToken.getTokenType() == UserTokenType.userTokenTypeUserName) {
                        if (this.username != null) {
                            policyId = identityToken.getPolicyId();
                        }
                    }
                }
            }
        }

        authenticationToken = sessionResponse.getAuthenticationToken().getNodeId();
        tokenId.set((int) opcuaMessageResponse.getSecureTokenId());
        channelId.set((int) opcuaMessageResponse.getSecureChannelId());

        int transactionId = channelTransactionManager.getTransactionIdentifier();

        Integer nextSequenceNumber = opcuaMessageResponse.getSequenceNumber() + 1;
        Integer nextRequestId = opcuaMessageResponse.getRequestId() + 1;

        if (!(transactionId == nextSequenceNumber)) {
            throw new PlcConnectionException("Sequence number isn't as expected, we might have missed a packet. - " +  transactionId + " != " + nextSequenceNumber);
        }

        int requestHandle = getRequestHandle();

        RequestHeader requestHeader = new RequestHeader(new NodeId(authenticationToken),
            getCurrentDateTime(),
            requestHandle,
            0L,
            NULL_STRING,
            REQUEST_TIMEOUT_LONG,
            NULL_EXTENSION_OBJECT);

        SignatureData clientSignature = new SignatureData(NULL_STRING, NULL_BYTE_STRING);

        SignedSoftwareCertificate[] signedSoftwareCertificate = new SignedSoftwareCertificate[1];

        signedSoftwareCertificate[0] = new SignedSoftwareCertificate(NULL_BYTE_STRING, NULL_BYTE_STRING);


        ExtensionObject userIdentityToken = null;
        if (this.username == null) {
            userIdentityToken = getIdentityToken("anonymous");
        } else {
            userIdentityToken = getIdentityToken("username");
        }

        ActivateSessionRequest activateSessionRequest = new ActivateSessionRequest(
            requestHeader,
            clientSignature,
            0,
            null,
            0,
            null,
            userIdentityToken,
            clientSignature);

        ExpandedNodeId expandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
            false,            //Server Index Specified
            new NodeIdFourByte((short) 0, Integer.valueOf(activateSessionRequest.getIdentifier())),
            null,
            null);

        ExtensionObject extObject = new ExtensionObject(
            expandedNodeId,
            null,
            activateSessionRequest);

        try {
            WriteBuffer buffer = new WriteBuffer(extObject.getLengthInBytes(), true);
            ExtensionObjectIO.staticSerialize(buffer, extObject);

            OpcuaMessageRequest activateMessageRequest = new OpcuaMessageRequest(FINAL_CHUNK,
                channelId.get(),
                tokenId.get(),
                nextSequenceNumber,
                nextRequestId,
                buffer.getData());

            Consumer<Integer> requestConsumer = t -> {
                context.sendRequest(new OpcuaAPU(activateMessageRequest))
                    .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                    .check(p -> p.getMessage() instanceof OpcuaMessageResponse)
                    .unwrap(p -> (OpcuaMessageResponse) p.getMessage())
                    .handle(opcuaActivateResponse -> {
                        LOGGER.debug("Got Activate Session Response Connection Response");
                        try {
                            ExtensionObject message = ExtensionObjectIO.staticParse(new ReadBuffer(opcuaActivateResponse.getMessage(), true), false);
                            if (message.getBody() instanceof ServiceFault) {
                                ServiceFault fault = (ServiceFault) message.getBody();
                                LOGGER.error("Failed to connect to opc ua server for the following reason:- {}, {}", ((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode(), OpcuaStatusCodes.enumForValue(((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode()));
                            } else {
                                ActivateSessionResponse activateMessageResponse = (ActivateSessionResponse) message.getBody();

                                long returnedRequestHandle = ((ResponseHeader) activateMessageResponse.getResponseHeader()).getRequestHandle();
                                if (!(requestHandle == returnedRequestHandle)) {
                                    LOGGER.error("Request handle isn't as expected, we might have missed a packet. {} != {}", requestHandle, returnedRequestHandle);
                                }

                                // Send an event that connection setup is complete.
                                keepAlive();
                                context.fireConnected();
                                this.isConnected = true;
                            }
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                    });
            };

            channelTransactionManager.submit(requestConsumer, transactionId);
        } catch (ParseException e) {
            LOGGER.error("Unable to serialise the ActivateSessionRequest");
        }
    }

    public void onDisconnect(ConversationContext<OpcuaAPU> context) {
        int transactionId = channelTransactionManager.getTransactionIdentifier();

        int requestHandle = getRequestHandle();

        if (keepAlive != null) {
            keepAlive.complete(null);
        }

        ExpandedNodeId expandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
            false,            //Server Index Specified
            new NodeIdFourByte((short) 0, 473),
            null,
            null);    //Identifier for OpenSecureChannel

        RequestHeader requestHeader = new RequestHeader(
            new NodeId(authenticationToken),
            getCurrentDateTime(),
            requestHandle,                                         //RequestHandle
            0L,
            NULL_STRING,
            5000L,
            NULL_EXTENSION_OBJECT);

        CloseSessionRequest closeSessionRequest = new CloseSessionRequest(
            requestHeader,
            true);

        ExtensionObject extObject = new ExtensionObject(
            expandedNodeId,
            null,
            closeSessionRequest);

        try {
            WriteBuffer buffer = new WriteBuffer(extObject.getLengthInBytes(), true);
            ExtensionObjectIO.staticSerialize(buffer, extObject);

            OpcuaMessageRequest messageRequest = new OpcuaMessageRequest(FINAL_CHUNK,
                channelId.get(),
                tokenId.get(),
                transactionId,
                transactionId,
                buffer.getData());
            Consumer<Integer> requestConsumer = t -> {
                context.sendRequest(new OpcuaAPU(messageRequest))
                    .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                    .check(p -> p.getMessage() instanceof OpcuaMessageResponse)
                    .unwrap(p -> (OpcuaMessageResponse) p.getMessage())
                    .handle(opcuaMessageResponse -> {
                        LOGGER.trace("Got Close Session Response Connection Response" + opcuaMessageResponse.toString());
                        onDisconnectCloseSecureChannel(context);
                    });
            };

            channelTransactionManager.submit(requestConsumer, transactionId);
        } catch (ParseException e) {
            LOGGER.error("Failed to parse the Message Request");
        }
    }

    private void onDisconnectCloseSecureChannel(ConversationContext<OpcuaAPU> context) {

        int transactionId = channelTransactionManager.getTransactionIdentifier();

        RequestHeader requestHeader = new RequestHeader(new NodeId(authenticationToken),
            getCurrentDateTime(),
            0L,                                         //RequestHandle
            0L,
            NULL_STRING,
            REQUEST_TIMEOUT_LONG,
            NULL_EXTENSION_OBJECT);

        CloseSecureChannelRequest closeSecureChannelRequest = new CloseSecureChannelRequest(requestHeader);

        ExpandedNodeId expandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
            false,            //Server Index Specified
            new NodeIdFourByte((short) 0, Integer.valueOf(closeSecureChannelRequest.getIdentifier())),
            null,
            null);

        OpcuaCloseRequest closeRequest = new OpcuaCloseRequest(FINAL_CHUNK,
            channelId.get(),
            tokenId.get(),
            transactionId,
            transactionId,
            new ExtensionObject(
                expandedNodeId,
                null,
                closeSecureChannelRequest));

        Consumer<Integer> requestConsumer = t -> {
            context.sendRequest(new OpcuaAPU(closeRequest))
                .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                .check(p -> p.getMessage() instanceof OpcuaMessageResponse)
                .unwrap(p -> (OpcuaMessageResponse) p.getMessage())
                .handle(opcuaMessageResponse -> {
                    LOGGER.trace("Got Close Secure Channel Response" + opcuaMessageResponse.toString());
                });

            context.fireDisconnected();
        };

        channelTransactionManager.submit(requestConsumer, transactionId);
    }

    public void onDiscover(ConversationContext<OpcuaAPU> context) {
        // Only the TCP transport supports login.
        LOGGER.info("Opcua Driver running in ACTIVE mode, discovering endpoints");

        OpcuaHelloRequest hello = new OpcuaHelloRequest(FINAL_CHUNK,
            VERSION,
            DEFAULT_RECEIVE_BUFFER_SIZE,
            DEFAULT_SEND_BUFFER_SIZE,
            DEFAULT_MAX_MESSAGE_SIZE,
            DEFAULT_MAX_CHUNK_COUNT,
            this.endpoint);

        Consumer<Integer> requestConsumer = t -> {
            context.sendRequest(new OpcuaAPU(hello))
                .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                .check(p -> p.getMessage() instanceof OpcuaAcknowledgeResponse)
                .unwrap(p -> (OpcuaAcknowledgeResponse) p.getMessage())
                .handle(opcuaAcknowledgeResponse -> {
                    LOGGER.debug("Got Hello Response Connection Response");
                    onDiscoverOpenSecureChannel(context, opcuaAcknowledgeResponse);
                });
        };

        channelTransactionManager.submit(requestConsumer, 1);
    }

    public void onDiscoverOpenSecureChannel(ConversationContext<OpcuaAPU> context, OpcuaAcknowledgeResponse opcuaAcknowledgeResponse) {
        int transactionId = channelTransactionManager.getTransactionIdentifier();

        RequestHeader requestHeader = new RequestHeader(new NodeId(authenticationToken),
            getCurrentDateTime(),
            0L,                                         //RequestHandle
            0L,
            NULL_STRING,
            REQUEST_TIMEOUT_LONG,
            NULL_EXTENSION_OBJECT);

        OpenSecureChannelRequest openSecureChannelRequest = new OpenSecureChannelRequest(
            requestHeader,
            VERSION,
            SecurityTokenRequestType.securityTokenRequestTypeIssue,
            MessageSecurityMode.messageSecurityModeNone,
            NULL_BYTE_STRING,
            lifetime);

        ExpandedNodeId expandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
            false,            //Server Index Specified
            new NodeIdFourByte((short) 0, Integer.valueOf(openSecureChannelRequest.getIdentifier())),
            null,
            null);


        try {
            WriteBuffer buffer = new WriteBuffer(openSecureChannelRequest.getLengthInBytes(), true);
            ExtensionObjectIO.staticSerialize(buffer, new ExtensionObject(
                expandedNodeId,
                null,
                openSecureChannelRequest));

            OpcuaOpenRequest openRequest = new OpcuaOpenRequest(FINAL_CHUNK,
                0,
                SECURITY_POLICY_NONE,
                NULL_BYTE_STRING,
                NULL_BYTE_STRING,
                transactionId,
                transactionId,
                buffer.getData());

            Consumer<Integer> requestConsumer = t -> {
                context.sendRequest(new OpcuaAPU(openRequest))
                    .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                    .check(p -> p.getMessage() instanceof OpcuaOpenResponse)
                    .unwrap(p -> (OpcuaOpenResponse) p.getMessage())
                    .handle(opcuaOpenResponse -> {
                        try {
                            ExtensionObject message = ExtensionObjectIO.staticParse(new ReadBuffer(opcuaOpenResponse.getMessage(), true), false);
                            if (message.getBody() instanceof ServiceFault) {
                                ServiceFault fault = (ServiceFault) message.getBody();
                                LOGGER.error("Failed to connect to opc ua server for the following reason:- {}, {}", ((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode(), OpcuaStatusCodes.enumForValue(((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode()));
                            } else {
                                LOGGER.debug("Got Secure Response Connection Response");
                                try {
                                    onDiscoverGetEndpointsRequest(context, opcuaOpenResponse, (OpenSecureChannelResponse) message.getBody());
                                } catch (PlcConnectionException e) {
                                    LOGGER.error("Error occurred while connecting to OPC UA server");
                                }
                            }
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    });
            };

            channelTransactionManager.submit(requestConsumer, transactionId);
        } catch (ParseException e) {
            LOGGER.error("Unable to to Parse Create Session Request");
        }
    }

    public void onDiscoverGetEndpointsRequest(ConversationContext<OpcuaAPU> context, OpcuaOpenResponse opcuaOpenResponse, OpenSecureChannelResponse openSecureChannelResponse) throws PlcConnectionException {
        certificateThumbprint = opcuaOpenResponse.getReceiverCertificateThumbprint();
        tokenId.set((int) ((ChannelSecurityToken) openSecureChannelResponse.getSecurityToken()).getTokenId());
        channelId.set((int) ((ChannelSecurityToken) openSecureChannelResponse.getSecurityToken()).getChannelId());

        int transactionId = channelTransactionManager.getTransactionIdentifier();

        Integer nextSequenceNumber = opcuaOpenResponse.getSequenceNumber() + 1;
        Integer nextRequestId = opcuaOpenResponse.getRequestId() + 1;

        if (!(transactionId == nextSequenceNumber)) {
            LOGGER.error("Sequence number isn't as expected, we might have missed a packet. - " +  transactionId + " != " + nextSequenceNumber);
            throw new PlcConnectionException("Sequence number isn't as expected, we might have missed a packet. - " +  transactionId + " != " + nextSequenceNumber);
        }

        RequestHeader requestHeader = new RequestHeader(new NodeId(authenticationToken),
            getCurrentDateTime(),
            0L,
            0L,
            NULL_STRING,
            REQUEST_TIMEOUT_LONG,
            NULL_EXTENSION_OBJECT);

        GetEndpointsRequest endpointsRequest = new GetEndpointsRequest(
            requestHeader,
            this.endpoint,
            0,
            null,
            0,
            null);

        ExpandedNodeId expandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
            false,            //Server Index Specified
            new NodeIdFourByte((short) 0, Integer.valueOf(endpointsRequest.getIdentifier())),
            null,
            null);

        try {
            WriteBuffer buffer = new WriteBuffer(endpointsRequest.getLengthInBytes(), true);
            ExtensionObjectIO.staticSerialize(buffer, new ExtensionObject(
                expandedNodeId,
                null,
                endpointsRequest));

            OpcuaMessageRequest messageRequest = new OpcuaMessageRequest(FINAL_CHUNK,
                channelId.get(),
                tokenId.get(),
                nextSequenceNumber,
                nextRequestId,
                buffer.getData());

            Consumer<Integer> requestConsumer = t -> {
                context.sendRequest(new OpcuaAPU(messageRequest))
                    .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                    .check(p -> p.getMessage() instanceof OpcuaMessageResponse)
                    .unwrap(p -> (OpcuaMessageResponse) p.getMessage())
                    .handle(opcuaMessageResponse -> {
                        try {
                            ExtensionObject message = ExtensionObjectIO.staticParse(new ReadBuffer(opcuaMessageResponse.getMessage(), true), false);
                            if (message.getBody() instanceof ServiceFault) {
                                ServiceFault fault = (ServiceFault) message.getBody();
                                LOGGER.error("Failed to connect to opc ua server for the following reason:- {}, {}", ((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode(), OpcuaStatusCodes.enumForValue(((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode()));
                            } else {
                                LOGGER.debug("Got Create Session Response Connection Response");
                                GetEndpointsResponse response = (GetEndpointsResponse) message.getBody();

                                EndpointDescription[] endpoints = (EndpointDescription[]) response.getEndpoints();
                                for (EndpointDescription endpoint : endpoints) {
                                    LOGGER.info(endpoint.getEndpointUrl().getStringValue());
                                    LOGGER.info(endpoint.getSecurityPolicyUri().getStringValue());
                                    if (endpoint.getEndpointUrl().getStringValue().equals(this.endpoint.getStringValue()) && endpoint.getSecurityPolicyUri().getStringValue().equals(this.securityPolicy)) {
                                        LOGGER.info("Found OPC UA endpoint {}", this.endpoint.getStringValue());
                                        this.configuration.setSenderCertificate(endpoint.getServerCertificate().getStringValue());
                                    }
                                }

                                try {
                                    MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
                                    byte[] digest = messageDigest.digest(this.configuration.getSenderCertificate());
                                    this.configuration.setThumbprint(new PascalByteString(digest.length, digest));
                                } catch (NoSuchAlgorithmException e) {
                                    LOGGER.error("Failed to find hashing algorithm");
                                }
                                onDiscoverCloseSecureChannel(context, response);
                            }
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    });
            };

            channelTransactionManager.submit(requestConsumer, transactionId);
        } catch (ParseException e) {
            LOGGER.error("Unable to to Parse Create Session Request");
        }
    }

    private void onDiscoverCloseSecureChannel(ConversationContext<OpcuaAPU> context, GetEndpointsResponse message) {

        int transactionId = channelTransactionManager.getTransactionIdentifier();

        RequestHeader requestHeader = new RequestHeader(new NodeId(authenticationToken),
            getCurrentDateTime(),
            0L,                                         //RequestHandle
            0L,
            NULL_STRING,
            REQUEST_TIMEOUT_LONG,
            NULL_EXTENSION_OBJECT);

        CloseSecureChannelRequest closeSecureChannelRequest = new CloseSecureChannelRequest(requestHeader);

        ExpandedNodeId expandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
            false,            //Server Index Specified
            new NodeIdFourByte((short) 0, Integer.valueOf(closeSecureChannelRequest.getIdentifier())),
            null,
            null);

        OpcuaCloseRequest closeRequest = new OpcuaCloseRequest(FINAL_CHUNK,
            channelId.get(),
            tokenId.get(),
            transactionId,
            transactionId,
            new ExtensionObject(
                expandedNodeId,
                null,
                closeSecureChannelRequest));

        Consumer<Integer> requestConsumer = t -> {
            context.sendRequest(new OpcuaAPU(closeRequest))
                .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                .check(p -> p.getMessage() instanceof OpcuaMessageResponse)
                .unwrap(p -> (OpcuaMessageResponse) p.getMessage())
                .handle(opcuaMessageResponse -> {
                    LOGGER.trace("Got Close Secure Channel Response" + opcuaMessageResponse.toString());
                    // Send an event that connection setup is complete.
                    context.fireDiscovered(this.configuration);
                });
        };

        channelTransactionManager.submit(requestConsumer, transactionId);
    }

    private void keepAlive() {
        keepAlive = CompletableFuture.supplyAsync(() -> {
            while(true) {

                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    LOGGER.trace("Interrupted Exception");
                }

                int transactionId = channelTransactionManager.getTransactionIdentifier();

                RequestHeader requestHeader = new RequestHeader(new NodeId(authenticationToken),
                    getCurrentDateTime(),
                    0L,                                         //RequestHandle
                    0L,
                    NULL_STRING,
                    REQUEST_TIMEOUT_LONG,
                    NULL_EXTENSION_OBJECT);

                OpenSecureChannelRequest openSecureChannelRequest = null;
                if (this.isEncrypted) {
                    openSecureChannelRequest = new OpenSecureChannelRequest(
                        requestHeader,
                        VERSION,
                        SecurityTokenRequestType.securityTokenRequestTypeIssue,
                        MessageSecurityMode.messageSecurityModeSignAndEncrypt,
                        new PascalByteString(clientNonce.length, clientNonce),
                        lifetime);
                } else {
                    openSecureChannelRequest = new OpenSecureChannelRequest(
                        requestHeader,
                        VERSION,
                        SecurityTokenRequestType.securityTokenRequestTypeIssue,
                        MessageSecurityMode.messageSecurityModeNone,
                        NULL_BYTE_STRING,
                        lifetime);
                }

                ExpandedNodeId expandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
                    false,            //Server Index Specified
                    new NodeIdFourByte((short) 0, Integer.valueOf(openSecureChannelRequest.getIdentifier())),
                    null,
                    null);

                ExtensionObject extObject = new ExtensionObject(
                    expandedNodeId,
                    null,
                    openSecureChannelRequest);

                try {
                    WriteBuffer buffer = new WriteBuffer(extObject.getLengthInBytes(), true);
                    ExtensionObjectIO.staticSerialize(buffer, extObject);

                    OpcuaOpenRequest openRequest = new OpcuaOpenRequest(FINAL_CHUNK,
                        0,
                        new PascalString(this.securityPolicy),
                        this.publicCertificate,
                        this.thumbprint,
                        transactionId,
                        transactionId,
                        buffer.getData());

                    final OpcuaAPU apu;

                    if (this.isEncrypted) {
                        apu = OpcuaAPUIO.staticParse(encryptionHandler.encodeMessage(openRequest, buffer.getData()), false);
                    } else {
                        apu = new OpcuaAPU(openRequest);
                    }

                    Consumer<Integer> requestConsumer = t -> {
                        context.sendRequest(apu)
                            .expectResponse(OpcuaAPU.class, REQUEST_TIMEOUT)
                            .check(p -> p.getMessage() instanceof OpcuaOpenResponse)
                            .unwrap(p -> (OpcuaOpenResponse) p.getMessage())
                            .handle(opcuaOpenResponse -> {
                                try {
                                    if (this.isEncrypted) {
                                        opcuaOpenResponse = (OpcuaOpenResponse) OpcuaAPUIO.staticParse(encryptionHandler.decodeMessage(opcuaOpenResponse, opcuaOpenResponse.getMessage()), true).getMessage();
                                    }
                                    ReadBuffer readBuffer = new ReadBuffer(opcuaOpenResponse.getMessage(), true);
                                    ExtensionObject message = ExtensionObjectIO.staticParse(readBuffer, false);

                                    if (message.getBody() instanceof ServiceFault) {
                                        ServiceFault fault = (ServiceFault) message.getBody();
                                        LOGGER.error("Failed to connect to opc ua server for the following reason:- {}, {}", ((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode(), OpcuaStatusCodes.enumForValue(((ResponseHeader) fault.getResponseHeader()).getServiceResult().getStatusCode()));
                                    } else {
                                        LOGGER.debug("Got Secure Response Connection Response");
                                        OpenSecureChannelResponse openSecureChannelResponse = (OpenSecureChannelResponse) message.getBody();
                                        ChannelSecurityToken token = (ChannelSecurityToken) openSecureChannelResponse.getSecurityToken();
                                        certificateThumbprint = opcuaOpenResponse.getReceiverCertificateThumbprint();
                                        tokenId.set((int) token.getTokenId());
                                        channelId.set((int) token.getChannelId());
                                        lifetime = token.getRevisedLifetime();
                                    }
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
                            });
                    };
                    LOGGER.info("Submitting OpenSecureChannel with id of {}", transactionId);
                    channelTransactionManager.submit(requestConsumer, transactionId);
                } catch (ParseException e) {
                    LOGGER.error("Unable to to Parse Open Secure Request");
                }
            }
            }
        );
    }


    /**
     * Returns the next request handle
     *
     * @return the next sequential request handle
     */
    public int getRequestHandle() {
        int transactionId = requestHandleGenerator.getAndIncrement();
        if(requestHandleGenerator.get() == SecureChannelTransactionManager.DEFAULT_MAX_REQUEST_ID) {
            requestHandleGenerator.set(1);
        }
        return transactionId;
    }

    /**
     * Returns the authentication token for the current connection
     *
     * @return a NodeId Authentication token
     */
    public NodeId getAuthenticationToken() {
        return new NodeId(this.authenticationToken);
    }

    /**
     * Gets the Channel identifier for the current channel
     *
     * @return int representing the channel identifier
     */
    public int getChannelId() {
        return this.channelId.get();
    }

    /**
     * Gets the Token Identifier
     *
     * @return int representing the token identifier
     */
    public int getTokenId() {
        return this.tokenId.get();
    }



    /**
     * Creates an IdentityToken to authenticate with a server.
     * @param securityPolicy
     * @return returns an ExtensionObject with an IdentityToken.
     */
    private ExtensionObject getIdentityToken(String securityPolicy) {
        ExpandedNodeId extExpandedNodeId = null;
        ExtensionObject userIdentityToken = null;
        switch (securityPolicy) {
            case "anonymous":
                //If we aren't using authentication tell the server we would like to login anonymously
                AnonymousIdentityToken anonymousIdentityToken = new AnonymousIdentityToken();

                extExpandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
                    false,            //Server Index Specified
                    new NodeIdFourByte((short) 0, OpcuaNodeIdServices.AnonymousIdentityToken_Encoding_DefaultBinary.getValue()),
                    null,
                    null);

                return new ExtensionObject(
                    extExpandedNodeId,
                    new ExtensionObjectEncodingMask(false, false, true),
                    new UserIdentityToken(new PascalString("anonymous"), anonymousIdentityToken));
            case "username":
                //Encrypt the password using the server nonce and server public key
                byte[] passwordBytes = this.password.getBytes();
                ByteBuffer encodeableBuffer = ByteBuffer.allocate(4 + passwordBytes.length + this.senderNonce.length);
                encodeableBuffer.order(ByteOrder.LITTLE_ENDIAN);
                encodeableBuffer.putInt(passwordBytes.length + this.senderNonce.length);
                encodeableBuffer.put(passwordBytes);
                encodeableBuffer.put(this.senderNonce);
                byte[] encodeablePassword = new byte[4 + passwordBytes.length + this.senderNonce.length];
                encodeableBuffer.position(0);
                encodeableBuffer.get(encodeablePassword);

                byte[] encryptedPassword = encryptionHandler.encryptPassword(encodeablePassword);
                UserNameIdentityToken userNameIdentityToken =  new UserNameIdentityToken(
                    new PascalString(this.username),
                    new PascalByteString(encryptedPassword.length, encryptedPassword),
                    new PascalString(PASSWORD_ENCRYPTION_ALGORITHM)
                );

                extExpandedNodeId = new ExpandedNodeId(false,           //Namespace Uri Specified
                    false,            //Server Index Specified
                    new NodeIdFourByte((short) 0, OpcuaNodeIdServices.UserNameIdentityToken_Encoding_DefaultBinary.getValue()),
                    NULL_STRING,
                    1L);

                return new ExtensionObject(
                    extExpandedNodeId,
                    new ExtensionObjectEncodingMask(false, false, true),
                    new UserIdentityToken(new PascalString("username"), userNameIdentityToken));
        }
        return null;
    }

    public static long getCurrentDateTime() {
        return (System.currentTimeMillis() * 10000) + EPOCH_OFFSET;
    }
}
