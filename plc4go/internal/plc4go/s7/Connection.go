//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
package s7

import (
	"fmt"
	readWriteModel "github.com/apache/plc4x/plc4go/internal/plc4go/s7/readwrite/model"
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi"
	internalModel "github.com/apache/plc4x/plc4go/internal/plc4go/spi/model"
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi/plcerrors"
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi/transports"
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi/utils"
	"github.com/apache/plc4x/plc4go/pkg/plc4go"
	apiModel "github.com/apache/plc4x/plc4go/pkg/plc4go/model"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"reflect"
	"strings"
	"sync"
	"time"
)

type ConnectionMetadata struct {
}

func (m ConnectionMetadata) GetConnectionAttributes() map[string]string {
	return map[string]string{}
}

func (m ConnectionMetadata) CanRead() bool {
	return true
}

func (m ConnectionMetadata) CanWrite() bool {
	return true
}

func (m ConnectionMetadata) CanSubscribe() bool {
	return false
}

func (m ConnectionMetadata) CanBrowse() bool {
	return false
}

type TpduGenerator struct {
	currentTpduId uint16
	lock          sync.Mutex
}

func (t *TpduGenerator) getAndIncrement() uint16 {
	t.lock.Lock()
	defer t.lock.Unlock()
	// If we've reached the max value for a 16 bit transaction identifier, reset back to 1
	if t.currentTpduId >= 0xFFFF {
		t.currentTpduId = 1
	}
	result := t.currentTpduId
	t.currentTpduId += 1
	return result
}

// TODO: maybe we can use a DefaultConnection struct here with delegates
type Connection struct {
	tpduGenerator TpduGenerator
	messageCodec  spi.MessageCodec
	configuration Configuration
	driverContext DriverContext
	fieldHandler  spi.PlcFieldHandler
	valueHandler  spi.PlcValueHandler
	defaultTtl    time.Duration
	tm            *spi.RequestTransactionManager
}

func NewConnection(messageCodec spi.MessageCodec, configuration Configuration, driverContext DriverContext, fieldHandler spi.PlcFieldHandler, tm *spi.RequestTransactionManager) Connection {
	return Connection{
		tpduGenerator: TpduGenerator{currentTpduId: 10},
		messageCodec:  messageCodec,
		configuration: configuration,
		driverContext: driverContext,
		fieldHandler:  fieldHandler,
		valueHandler:  NewValueHandler(),
		defaultTtl:    time.Second * 10,
		tm:            tm,
	}
}

func (m *Connection) Connect() <-chan plc4go.PlcConnectionConnectResult {
	log.Trace().Msg("Connecting")
	ch := make(chan plc4go.PlcConnectionConnectResult)
	go func() {
		err := m.messageCodec.Connect()
		if err != nil {
			ch <- plc4go.NewPlcConnectionConnectResult(m, err)
		}

		// Only on active connections we do a connection
		if m.driverContext.PassiveMode {
			log.Info().Msg("S7 Driver running in PASSIVE mode.")
			ch <- plc4go.NewPlcConnectionConnectResult(m, nil)
			return
		}
		// Only the TCP transport supports login.
		log.Info().Msg("S7 Driver running in ACTIVE mode.")
		log.Debug().Msg("Sending COTP Connection Request")
		// Open the session on ISO Transport Protocol first.

		result := make(chan *readWriteModel.COTPPacketConnectionResponse)
		errorResult := make(chan error)
		err = m.messageCodec.SendRequest(
			readWriteModel.NewTPKTPacket(m.createCOTPConnectionRequest()),
			func(message interface{}) bool {
				tpktPacket := readWriteModel.CastTPKTPacket(message)
				if tpktPacket == nil {
					return false
				}
				cotpPacketConnectionResponse := readWriteModel.CastCOTPPacketConnectionResponse(tpktPacket.Payload)
				return cotpPacketConnectionResponse != nil
			},
			func(message interface{}) error {
				tpktPacket := readWriteModel.CastTPKTPacket(message)
				cotpPacketConnectionResponse := readWriteModel.CastCOTPPacketConnectionResponse(tpktPacket.Payload)
				result <- cotpPacketConnectionResponse
				return nil
			},
			func(err error) error {
				// If this is a timeout, do a check if the connection requires a reconnection
				if _, isTimeout := err.(plcerrors.TimeoutError); isTimeout {
					log.Warn().Msg("Timeout during Connection establishing, closing channel...")
					m.Close()
				}
				errorResult <- errors.Wrap(err, "got error processing request")
				return nil
			},
			m.defaultTtl,
		)
		if err != nil {
			ch <- plc4go.NewPlcConnectionConnectResult(m, err)
		}
		select {
		case cotpPacketConnectionResponse := <-result:
			log.Debug().Msg("Got COTP Connection Response")
			log.Debug().Msg("Sending S7 Connection Request")

			// Send an S7 login message.
			result2 := make(chan *readWriteModel.S7ParameterSetupCommunication)
			errorResult2 := make(chan error)
			err = m.messageCodec.SendRequest(
				m.createS7ConnectionRequest(cotpPacketConnectionResponse),
				func(message interface{}) bool {
					tpktPacket := readWriteModel.CastTPKTPacket(message)
					if tpktPacket == nil {
						return false
					}
					cotpPacketData := readWriteModel.CastCOTPPacketData(tpktPacket.Payload)
					if cotpPacketData == nil {
						return false
					}
					messageResponseData := readWriteModel.CastS7MessageResponseData(cotpPacketData.Parent.Payload)
					if messageResponseData == nil {
						return false
					}
					return readWriteModel.CastS7ParameterSetupCommunication(messageResponseData.Parent.Parameter) != nil
				},
				func(message interface{}) error {
					tpktPacket := readWriteModel.CastTPKTPacket(message)
					cotpPacketData := readWriteModel.CastCOTPPacketData(tpktPacket.Payload)
					messageResponseData := readWriteModel.CastS7MessageResponseData(cotpPacketData.Parent.Payload)
					setupCommunication := readWriteModel.CastS7ParameterSetupCommunication(messageResponseData.Parent.Parameter)
					result2 <- setupCommunication
					return nil
				},
				func(err error) error {
					// If this is a timeout, do a check if the connection requires a reconnection
					if _, isTimeout := err.(plcerrors.TimeoutError); isTimeout {
						log.Warn().Msg("Timeout during Connection establishing, closing channel...")
						m.Close()
					}
					errorResult2 <- errors.Wrap(err, "got error processing request")
					return nil
				},
				m.defaultTtl,
			)
			if err != nil {
				ch <- plc4go.NewPlcConnectionConnectResult(m, err)
			}
			select {
			case setupCommunication := <-result2:
				log.Debug().Msg("Got S7 Connection Response")
				// Save some data from the response.
				m.driverContext.MaxAmqCaller = setupCommunication.MaxAmqCaller
				m.driverContext.MaxAmqCallee = setupCommunication.MaxAmqCallee
				m.driverContext.PduSize = setupCommunication.PduLength

				// Update the number of concurrent requests to the negotiated number.
				// I have never seen anything else than equal values for caller and
				// callee, but if they were different, we're only limiting the outgoing
				// requests.
				m.tm.SetNumberOfConcurrentRequests(int(m.driverContext.MaxAmqCallee))

				// If the controller type is explicitly set, were finished with the login
				// process. If it's set to ANY, we have to query the serial number information
				// in order to detect the type of PLC.
				if m.driverContext.ControllerType != ControllerType_ANY {
					// Send an event that connection setup is complete.
					ch <- plc4go.NewPlcConnectionConnectResult(m, nil)
					return
				}

				// Prepare a message to request the remote to identify itself.
				log.Debug().Msg("Sending S7 Identification Request")
				result3 := make(chan *readWriteModel.S7PayloadUserData)
				errorResult3 := make(chan error)
				err = m.messageCodec.SendRequest(
					m.createIdentifyRemoteMessage(),
					func(message interface{}) bool {
						tpktPacket := readWriteModel.CastTPKTPacket(message)
						if tpktPacket == nil {
							return false
						}
						cotpPacketData := readWriteModel.CastCOTPPacketData(tpktPacket.Payload)
						if cotpPacketData == nil {
							return false
						}
						messageUserData := readWriteModel.CastS7MessageUserData(cotpPacketData.Parent.Payload)
						if messageUserData == nil {
							return false
						}
						return readWriteModel.CastS7PayloadUserData(messageUserData.Parent.Payload) != nil
					},
					func(message interface{}) error {
						tpktPacket := readWriteModel.CastTPKTPacket(message)
						cotpPacketData := readWriteModel.CastCOTPPacketData(tpktPacket.Payload)
						messageUserData := readWriteModel.CastS7MessageUserData(cotpPacketData.Parent.Payload)
						result3 <- readWriteModel.CastS7PayloadUserData(messageUserData.Parent.Payload)
						return nil
					},
					func(err error) error {
						// If this is a timeout, do a check if the connection requires a reconnection
						if _, isTimeout := err.(plcerrors.TimeoutError); isTimeout {
							log.Warn().Msg("Timeout during Connection establishing, closing channel...")
							m.Close()
						}
						errorResult3 <- errors.Wrap(err, "got error processing request")
						return nil
					},
					m.defaultTtl,
				)
				if err != nil {
					ch <- plc4go.NewPlcConnectionConnectResult(m, err)
				}
				select {
				case payloadUserData := <-result3:
					log.Debug().Msg("Got S7 Identification Response")
					m.extractControllerTypeAndFireConnected(payloadUserData, ch)
				case err := <-errorResult3:
					ch <- plc4go.NewPlcConnectionConnectResult(nil, errors.Wrap(err, "Error during connection"))
				}
			case err := <-errorResult2:
				ch <- plc4go.NewPlcConnectionConnectResult(nil, errors.Wrap(err, "Error during connection"))
			}
		case err := <-errorResult:
			ch <- plc4go.NewPlcConnectionConnectResult(nil, errors.Wrap(err, "Error during connection"))
		}
	}()
	return ch
}

func (m *Connection) extractControllerTypeAndFireConnected(payloadUserData *readWriteModel.S7PayloadUserData, ch chan<- plc4go.PlcConnectionConnectResult) {
	// TODO: how do we handle the case if there no items at all? Should we assume it a successful or failure
	for _, item := range payloadUserData.Items {
		switch item.Child.(type) {
		case *readWriteModel.S7PayloadUserDataItemCpuFunctionReadSzlResponse:
			readSzlResponseItem := item.Child.(*readWriteModel.S7PayloadUserDataItemCpuFunctionReadSzlResponse)
			for _, readSzlResponseItemItem := range readSzlResponseItem.Items {
				if readSzlResponseItemItem.ItemIndex != 0x0001 {
					continue
				}
				articleNumber := string(utils.Int8ArrayToByteArray(readSzlResponseItemItem.Mlfb))
				var controllerType ControllerType
				if !strings.HasPrefix(articleNumber, "6ES7 ") {
					controllerType = ControllerType_ANY
				}
				blankIndex := strings.Index(articleNumber, " ")
				model := articleNumber[blankIndex+1 : blankIndex+2]
				switch model {
				case "2":
					controllerType = ControllerType_S7_1200
				case "5":
					controllerType = ControllerType_S7_1500
				case "3":
					controllerType = ControllerType_S7_300
				case "4":
					controllerType = ControllerType_S7_400
				default:
					log.Info().Msgf("Looking up unknown article number %s", articleNumber)
					controllerType = ControllerType_ANY
				}
				m.driverContext.ControllerType = controllerType

				// Send an event that connection setup is complete.
				ch <- plc4go.NewPlcConnectionConnectResult(m, nil)
			}
		}
	}
}

func (m Connection) createIdentifyRemoteMessage() *readWriteModel.TPKTPacket {
	identifyRemoteMessage := readWriteModel.NewS7MessageUserData(
		1,
		readWriteModel.NewS7ParameterUserData(
			[]*readWriteModel.S7ParameterUserDataItem{
				readWriteModel.NewS7ParameterUserDataItemCPUFunctions(
					0x11,
					0x4,
					0x4,
					0x01,
					0x00,
					nil,
					nil,
					nil),
			},
		),
		readWriteModel.NewS7PayloadUserData(
			[]*readWriteModel.S7PayloadUserDataItem{
				readWriteModel.NewS7PayloadUserDataItemCpuFunctionReadSzlRequest(
					readWriteModel.DataTransportErrorCode_OK,
					readWriteModel.DataTransportSize_OCTET_STRING,
					readWriteModel.NewSzlId(
						readWriteModel.SzlModuleTypeClass_CPU,
						0x00,
						readWriteModel.SzlSublist_MODULE_IDENTIFICATION,
					),
					0x0000,
				),
			},
		),
	)
	cotpPacketData := readWriteModel.NewCOTPPacketData(true, 2, nil, identifyRemoteMessage)
	return readWriteModel.NewTPKTPacket(cotpPacketData)
}

func (m Connection) createS7ConnectionRequest(cotpPacketConnectionResponse *readWriteModel.COTPPacketConnectionResponse) *readWriteModel.TPKTPacket {
	for _, parameter := range cotpPacketConnectionResponse.Parent.Parameters {
		switch parameter.Child.(type) {
		case *readWriteModel.COTPParameterCalledTsap:
			cotpParameterCalledTsap := parameter.Child.(*readWriteModel.COTPParameterCalledTsap)
			m.driverContext.CalledTsapId = cotpParameterCalledTsap.TsapId
		case *readWriteModel.COTPParameterCallingTsap:
			cotpParameterCallingTsap := parameter.Child.(*readWriteModel.COTPParameterCallingTsap)
			if cotpParameterCallingTsap.TsapId != m.driverContext.CallingTsapId {
				m.driverContext.CallingTsapId = cotpParameterCallingTsap.TsapId
				log.Warn().Msgf("Switching calling TSAP id to '%x'", m.driverContext.CallingTsapId)
			}
		case *readWriteModel.COTPParameterTpduSize:
			cotpParameterTpduSize := parameter.Child.(*readWriteModel.COTPParameterTpduSize)
			m.driverContext.CotpTpduSize = cotpParameterTpduSize.TpduSize
		default:
			log.Warn().Msgf("Got unknown parameter type '%v'", reflect.TypeOf(parameter))
		}
	}

	s7ParameterSetupCommunication := readWriteModel.NewS7ParameterSetupCommunication(
		m.driverContext.MaxAmqCaller, m.driverContext.MaxAmqCallee, m.driverContext.PduSize,
	)
	s7Message := readWriteModel.NewS7Message(0, s7ParameterSetupCommunication, nil)
	cotpPacketData := readWriteModel.NewCOTPPacketData(true, 1, nil, s7Message)
	return readWriteModel.NewTPKTPacket(cotpPacketData)
}

func (m Connection) createCOTPConnectionRequest() *readWriteModel.COTPPacket {
	return readWriteModel.NewCOTPPacketConnectionRequest(
		0x0000,
		0x000F,
		readWriteModel.COTPProtocolClass_CLASS_0,
		[]*readWriteModel.COTPParameter{
			readWriteModel.NewCOTPParameterCalledTsap(m.driverContext.CalledTsapId),
			readWriteModel.NewCOTPParameterCallingTsap(m.driverContext.CallingTsapId),
			readWriteModel.NewCOTPParameterTpduSize(m.driverContext.CotpTpduSize),
		},
		nil,
	)
}

func (m Connection) BlockingClose() {
	log.Trace().Msg("Closing blocked")
	closeResults := m.Close()
	select {
	case <-closeResults:
		return
	case <-time.After(time.Second * 5):
		return
	}
}

func (m *Connection) Close() <-chan plc4go.PlcConnectionCloseResult {
	log.Trace().Msg("Close")
	// TODO: Implement ...
	ch := make(chan plc4go.PlcConnectionCloseResult)
	go func() {
		ch <- plc4go.NewPlcConnectionCloseResult(m, nil)
	}()
	return ch
}

func (m Connection) IsConnected() bool {
	panic("implement me")
}

func (m Connection) Ping() <-chan plc4go.PlcConnectionPingResult {
	panic("Not implemented")
}

func (m Connection) GetMetadata() apiModel.PlcConnectionMetadata {
	return ConnectionMetadata{}
}

func (m Connection) ReadRequestBuilder() apiModel.PlcReadRequestBuilder {
	return internalModel.NewDefaultPlcReadRequestBuilder(m.fieldHandler, NewReader(&m.tpduGenerator, m.messageCodec, m.tm))
}

func (m Connection) WriteRequestBuilder() apiModel.PlcWriteRequestBuilder {
	return internalModel.NewDefaultPlcWriteRequestBuilder(
		m.fieldHandler, m.valueHandler, NewWriter(&m.tpduGenerator, m.messageCodec, m.tm))
}

func (m Connection) SubscriptionRequestBuilder() apiModel.PlcSubscriptionRequestBuilder {
	panic("implement me")
}

func (m Connection) UnsubscriptionRequestBuilder() apiModel.PlcUnsubscriptionRequestBuilder {
	panic("implement me")
}

func (m Connection) BrowseRequestBuilder() apiModel.PlcBrowseRequestBuilder {
	panic("implement me")
}

func (m Connection) GetTransportInstance() transports.TransportInstance {
	if mc, ok := m.messageCodec.(spi.TransportInstanceExposer); ok {
		return mc.GetTransportInstance()
	}
	return nil
}

func (m Connection) GetPlcFieldHandler() spi.PlcFieldHandler {
	return m.fieldHandler
}

func (m Connection) GetPlcValueHandler() spi.PlcValueHandler {
	return m.valueHandler
}

func (m Connection) String() string {
	return fmt.Sprintf("s7.Connection")
}
