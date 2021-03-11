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
package model

import (
	"encoding/xml"
	"errors"
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi/utils"
	"io"
	"reflect"
	"strings"
)

// The data-structure of this message
type AdsData struct {
	Child IAdsDataChild
	IAdsData
	IAdsDataParent
}

// The corresponding interface
type IAdsData interface {
	CommandId() CommandId
	Response() bool
	LengthInBytes() uint16
	LengthInBits() uint16
	Serialize(io utils.WriteBuffer) error
	xml.Marshaler
}

type IAdsDataParent interface {
	SerializeParent(io utils.WriteBuffer, child IAdsData, serializeChildFunction func() error) error
	GetTypeName() string
}

type IAdsDataChild interface {
	Serialize(io utils.WriteBuffer) error
	InitializeParent(parent *AdsData)
	GetTypeName() string
	IAdsData
}

func NewAdsData() *AdsData {
	return &AdsData{}
}

func CastAdsData(structType interface{}) *AdsData {
	castFunc := func(typ interface{}) *AdsData {
		if casted, ok := typ.(AdsData); ok {
			return &casted
		}
		if casted, ok := typ.(*AdsData); ok {
			return casted
		}
		return nil
	}
	return castFunc(structType)
}

func (m *AdsData) GetTypeName() string {
	return "AdsData"
}

func (m *AdsData) LengthInBits() uint16 {
	lengthInBits := uint16(0)

	// Length of sub-type elements will be added by sub-type...
	lengthInBits += m.Child.LengthInBits()

	return lengthInBits
}

func (m *AdsData) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func AdsDataParse(io *utils.ReadBuffer, commandId *CommandId, response bool) (*AdsData, error) {

	// Switch Field (Depending on the discriminator values, passes the instantiation to a sub-type)
	var _parent *AdsData
	var typeSwitchError error
	switch {
	case *commandId == CommandId_INVALID && response == false:
		_parent, typeSwitchError = AdsInvalidRequestParse(io)
	case *commandId == CommandId_INVALID && response == true:
		_parent, typeSwitchError = AdsInvalidResponseParse(io)
	case *commandId == CommandId_ADS_READ_DEVICE_INFO && response == false:
		_parent, typeSwitchError = AdsReadDeviceInfoRequestParse(io)
	case *commandId == CommandId_ADS_READ_DEVICE_INFO && response == true:
		_parent, typeSwitchError = AdsReadDeviceInfoResponseParse(io)
	case *commandId == CommandId_ADS_READ && response == false:
		_parent, typeSwitchError = AdsReadRequestParse(io)
	case *commandId == CommandId_ADS_READ && response == true:
		_parent, typeSwitchError = AdsReadResponseParse(io)
	case *commandId == CommandId_ADS_WRITE && response == false:
		_parent, typeSwitchError = AdsWriteRequestParse(io)
	case *commandId == CommandId_ADS_WRITE && response == true:
		_parent, typeSwitchError = AdsWriteResponseParse(io)
	case *commandId == CommandId_ADS_READ_STATE && response == false:
		_parent, typeSwitchError = AdsReadStateRequestParse(io)
	case *commandId == CommandId_ADS_READ_STATE && response == true:
		_parent, typeSwitchError = AdsReadStateResponseParse(io)
	case *commandId == CommandId_ADS_WRITE_CONTROL && response == false:
		_parent, typeSwitchError = AdsWriteControlRequestParse(io)
	case *commandId == CommandId_ADS_WRITE_CONTROL && response == true:
		_parent, typeSwitchError = AdsWriteControlResponseParse(io)
	case *commandId == CommandId_ADS_ADD_DEVICE_NOTIFICATION && response == false:
		_parent, typeSwitchError = AdsAddDeviceNotificationRequestParse(io)
	case *commandId == CommandId_ADS_ADD_DEVICE_NOTIFICATION && response == true:
		_parent, typeSwitchError = AdsAddDeviceNotificationResponseParse(io)
	case *commandId == CommandId_ADS_DELETE_DEVICE_NOTIFICATION && response == false:
		_parent, typeSwitchError = AdsDeleteDeviceNotificationRequestParse(io)
	case *commandId == CommandId_ADS_DELETE_DEVICE_NOTIFICATION && response == true:
		_parent, typeSwitchError = AdsDeleteDeviceNotificationResponseParse(io)
	case *commandId == CommandId_ADS_DEVICE_NOTIFICATION && response == false:
		_parent, typeSwitchError = AdsDeviceNotificationRequestParse(io)
	case *commandId == CommandId_ADS_DEVICE_NOTIFICATION && response == true:
		_parent, typeSwitchError = AdsDeviceNotificationResponseParse(io)
	case *commandId == CommandId_ADS_READ_WRITE && response == false:
		_parent, typeSwitchError = AdsReadWriteRequestParse(io)
	case *commandId == CommandId_ADS_READ_WRITE && response == true:
		_parent, typeSwitchError = AdsReadWriteResponseParse(io)
	}
	if typeSwitchError != nil {
		return nil, errors.New("Error parsing sub-type for type-switch. " + typeSwitchError.Error())
	}

	// Finish initializing
	_parent.Child.InitializeParent(_parent)
	return _parent, nil
}

func (m *AdsData) Serialize(io utils.WriteBuffer) error {
	return m.Child.Serialize(io)
}

func (m *AdsData) SerializeParent(io utils.WriteBuffer, child IAdsData, serializeChildFunction func() error) error {

	// Switch field (Depending on the discriminator values, passes the serialization to a sub-type)
	_typeSwitchErr := serializeChildFunction()
	if _typeSwitchErr != nil {
		return errors.New("Error serializing sub-type field " + _typeSwitchErr.Error())
	}

	return nil
}

func (m *AdsData) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var token xml.Token
	var err error
	for {
		token, err = d.Token()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		switch token.(type) {
		case xml.StartElement:
			tok := token.(xml.StartElement)
			switch tok.Name.Local {
			default:
				switch start.Attr[0].Value {
				case "org.apache.plc4x.java.ads.readwrite.AdsInvalidRequest":
					var dt *AdsInvalidRequest
					if m.Child != nil {
						dt = m.Child.(*AdsInvalidRequest)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsInvalidResponse":
					var dt *AdsInvalidResponse
					if m.Child != nil {
						dt = m.Child.(*AdsInvalidResponse)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsReadDeviceInfoRequest":
					var dt *AdsReadDeviceInfoRequest
					if m.Child != nil {
						dt = m.Child.(*AdsReadDeviceInfoRequest)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsReadDeviceInfoResponse":
					var dt *AdsReadDeviceInfoResponse
					if m.Child != nil {
						dt = m.Child.(*AdsReadDeviceInfoResponse)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsReadRequest":
					var dt *AdsReadRequest
					if m.Child != nil {
						dt = m.Child.(*AdsReadRequest)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsReadResponse":
					var dt *AdsReadResponse
					if m.Child != nil {
						dt = m.Child.(*AdsReadResponse)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsWriteRequest":
					var dt *AdsWriteRequest
					if m.Child != nil {
						dt = m.Child.(*AdsWriteRequest)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsWriteResponse":
					var dt *AdsWriteResponse
					if m.Child != nil {
						dt = m.Child.(*AdsWriteResponse)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsReadStateRequest":
					var dt *AdsReadStateRequest
					if m.Child != nil {
						dt = m.Child.(*AdsReadStateRequest)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsReadStateResponse":
					var dt *AdsReadStateResponse
					if m.Child != nil {
						dt = m.Child.(*AdsReadStateResponse)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsWriteControlRequest":
					var dt *AdsWriteControlRequest
					if m.Child != nil {
						dt = m.Child.(*AdsWriteControlRequest)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsWriteControlResponse":
					var dt *AdsWriteControlResponse
					if m.Child != nil {
						dt = m.Child.(*AdsWriteControlResponse)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsAddDeviceNotificationRequest":
					var dt *AdsAddDeviceNotificationRequest
					if m.Child != nil {
						dt = m.Child.(*AdsAddDeviceNotificationRequest)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsAddDeviceNotificationResponse":
					var dt *AdsAddDeviceNotificationResponse
					if m.Child != nil {
						dt = m.Child.(*AdsAddDeviceNotificationResponse)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsDeleteDeviceNotificationRequest":
					var dt *AdsDeleteDeviceNotificationRequest
					if m.Child != nil {
						dt = m.Child.(*AdsDeleteDeviceNotificationRequest)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsDeleteDeviceNotificationResponse":
					var dt *AdsDeleteDeviceNotificationResponse
					if m.Child != nil {
						dt = m.Child.(*AdsDeleteDeviceNotificationResponse)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsDeviceNotificationRequest":
					var dt *AdsDeviceNotificationRequest
					if m.Child != nil {
						dt = m.Child.(*AdsDeviceNotificationRequest)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsDeviceNotificationResponse":
					var dt *AdsDeviceNotificationResponse
					if m.Child != nil {
						dt = m.Child.(*AdsDeviceNotificationResponse)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsReadWriteRequest":
					var dt *AdsReadWriteRequest
					if m.Child != nil {
						dt = m.Child.(*AdsReadWriteRequest)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.ads.readwrite.AdsReadWriteResponse":
					var dt *AdsReadWriteResponse
					if m.Child != nil {
						dt = m.Child.(*AdsReadWriteResponse)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				}
			}
		}
	}
}

func (m *AdsData) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	className := reflect.TypeOf(m.Child).String()
	className = "org.apache.plc4x.java.ads.readwrite." + className[strings.LastIndex(className, ".")+1:]
	if err := e.EncodeToken(xml.StartElement{Name: start.Name, Attr: []xml.Attr{
		{Name: xml.Name{Local: "className"}, Value: className},
	}}); err != nil {
		return err
	}
	marshaller, ok := m.Child.(xml.Marshaler)
	if !ok {
		return errors.New("child is not castable to Marshaler")
	}
	if err := marshaller.MarshalXML(e, start); err != nil {
		return err
	}
	if err := e.EncodeToken(xml.EndElement{Name: start.Name}); err != nil {
		return err
	}
	return nil
}
