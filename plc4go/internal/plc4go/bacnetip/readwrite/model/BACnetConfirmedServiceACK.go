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
type BACnetConfirmedServiceACK struct {
	Child IBACnetConfirmedServiceACKChild
	IBACnetConfirmedServiceACK
	IBACnetConfirmedServiceACKParent
}

// The corresponding interface
type IBACnetConfirmedServiceACK interface {
	ServiceChoice() uint8
	LengthInBytes() uint16
	LengthInBits() uint16
	Serialize(io utils.WriteBuffer) error
	xml.Marshaler
}

type IBACnetConfirmedServiceACKParent interface {
	SerializeParent(io utils.WriteBuffer, child IBACnetConfirmedServiceACK, serializeChildFunction func() error) error
	GetTypeName() string
}

type IBACnetConfirmedServiceACKChild interface {
	Serialize(io utils.WriteBuffer) error
	InitializeParent(parent *BACnetConfirmedServiceACK)
	GetTypeName() string
	IBACnetConfirmedServiceACK
}

func NewBACnetConfirmedServiceACK() *BACnetConfirmedServiceACK {
	return &BACnetConfirmedServiceACK{}
}

func CastBACnetConfirmedServiceACK(structType interface{}) *BACnetConfirmedServiceACK {
	castFunc := func(typ interface{}) *BACnetConfirmedServiceACK {
		if casted, ok := typ.(BACnetConfirmedServiceACK); ok {
			return &casted
		}
		if casted, ok := typ.(*BACnetConfirmedServiceACK); ok {
			return casted
		}
		return nil
	}
	return castFunc(structType)
}

func (m *BACnetConfirmedServiceACK) GetTypeName() string {
	return "BACnetConfirmedServiceACK"
}

func (m *BACnetConfirmedServiceACK) LengthInBits() uint16 {
	lengthInBits := uint16(0)

	// Discriminator Field (serviceChoice)
	lengthInBits += 8

	// Length of sub-type elements will be added by sub-type...
	lengthInBits += m.Child.LengthInBits()

	return lengthInBits
}

func (m *BACnetConfirmedServiceACK) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func BACnetConfirmedServiceACKParse(io *utils.ReadBuffer) (*BACnetConfirmedServiceACK, error) {

	// Discriminator Field (serviceChoice) (Used as input to a switch field)
	serviceChoice, _serviceChoiceErr := io.ReadUint8(8)
	if _serviceChoiceErr != nil {
		return nil, errors.New("Error parsing 'serviceChoice' field " + _serviceChoiceErr.Error())
	}

	// Switch Field (Depending on the discriminator values, passes the instantiation to a sub-type)
	var _parent *BACnetConfirmedServiceACK
	var typeSwitchError error
	switch {
	case serviceChoice == 0x03:
		_parent, typeSwitchError = BACnetConfirmedServiceACKGetAlarmSummaryParse(io)
	case serviceChoice == 0x04:
		_parent, typeSwitchError = BACnetConfirmedServiceACKGetEnrollmentSummaryParse(io)
	case serviceChoice == 0x1D:
		_parent, typeSwitchError = BACnetConfirmedServiceACKGetEventInformationParse(io)
	case serviceChoice == 0x06:
		_parent, typeSwitchError = BACnetConfirmedServiceACKAtomicReadFileParse(io)
	case serviceChoice == 0x07:
		_parent, typeSwitchError = BACnetConfirmedServiceACKAtomicWriteFileParse(io)
	case serviceChoice == 0x0A:
		_parent, typeSwitchError = BACnetConfirmedServiceACKCreateObjectParse(io)
	case serviceChoice == 0x0C:
		_parent, typeSwitchError = BACnetConfirmedServiceACKReadPropertyParse(io)
	case serviceChoice == 0x0E:
		_parent, typeSwitchError = BACnetConfirmedServiceACKReadPropertyMultipleParse(io)
	case serviceChoice == 0x1A:
		_parent, typeSwitchError = BACnetConfirmedServiceACKReadRangeParse(io)
	case serviceChoice == 0x12:
		_parent, typeSwitchError = BACnetConfirmedServiceACKConfirmedPrivateTransferParse(io)
	case serviceChoice == 0x15:
		_parent, typeSwitchError = BACnetConfirmedServiceACKVTOpenParse(io)
	case serviceChoice == 0x17:
		_parent, typeSwitchError = BACnetConfirmedServiceACKVTDataParse(io)
	case serviceChoice == 0x18:
		_parent, typeSwitchError = BACnetConfirmedServiceACKRemovedAuthenticateParse(io)
	case serviceChoice == 0x0D:
		_parent, typeSwitchError = BACnetConfirmedServiceACKRemovedReadPropertyConditionalParse(io)
	}
	if typeSwitchError != nil {
		return nil, errors.New("Error parsing sub-type for type-switch. " + typeSwitchError.Error())
	}

	// Finish initializing
	_parent.Child.InitializeParent(_parent)
	return _parent, nil
}

func (m *BACnetConfirmedServiceACK) Serialize(io utils.WriteBuffer) error {
	return m.Child.Serialize(io)
}

func (m *BACnetConfirmedServiceACK) SerializeParent(io utils.WriteBuffer, child IBACnetConfirmedServiceACK, serializeChildFunction func() error) error {

	// Discriminator Field (serviceChoice) (Used as input to a switch field)
	serviceChoice := uint8(child.ServiceChoice())
	_serviceChoiceErr := io.WriteUint8(8, (serviceChoice))
	if _serviceChoiceErr != nil {
		return errors.New("Error serializing 'serviceChoice' field " + _serviceChoiceErr.Error())
	}

	// Switch field (Depending on the discriminator values, passes the serialization to a sub-type)
	_typeSwitchErr := serializeChildFunction()
	if _typeSwitchErr != nil {
		return errors.New("Error serializing sub-type field " + _typeSwitchErr.Error())
	}

	return nil
}

func (m *BACnetConfirmedServiceACK) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
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
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKGetAlarmSummary":
					var dt *BACnetConfirmedServiceACKGetAlarmSummary
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKGetAlarmSummary)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKGetEnrollmentSummary":
					var dt *BACnetConfirmedServiceACKGetEnrollmentSummary
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKGetEnrollmentSummary)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKGetEventInformation":
					var dt *BACnetConfirmedServiceACKGetEventInformation
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKGetEventInformation)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKAtomicReadFile":
					var dt *BACnetConfirmedServiceACKAtomicReadFile
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKAtomicReadFile)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKAtomicWriteFile":
					var dt *BACnetConfirmedServiceACKAtomicWriteFile
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKAtomicWriteFile)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKCreateObject":
					var dt *BACnetConfirmedServiceACKCreateObject
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKCreateObject)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKReadProperty":
					var dt *BACnetConfirmedServiceACKReadProperty
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKReadProperty)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKReadPropertyMultiple":
					var dt *BACnetConfirmedServiceACKReadPropertyMultiple
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKReadPropertyMultiple)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKReadRange":
					var dt *BACnetConfirmedServiceACKReadRange
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKReadRange)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKConfirmedPrivateTransfer":
					var dt *BACnetConfirmedServiceACKConfirmedPrivateTransfer
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKConfirmedPrivateTransfer)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKVTOpen":
					var dt *BACnetConfirmedServiceACKVTOpen
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKVTOpen)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKVTData":
					var dt *BACnetConfirmedServiceACKVTData
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKVTData)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKRemovedAuthenticate":
					var dt *BACnetConfirmedServiceACKRemovedAuthenticate
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKRemovedAuthenticate)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.bacnetip.readwrite.BACnetConfirmedServiceACKRemovedReadPropertyConditional":
					var dt *BACnetConfirmedServiceACKRemovedReadPropertyConditional
					if m.Child != nil {
						dt = m.Child.(*BACnetConfirmedServiceACKRemovedReadPropertyConditional)
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

func (m *BACnetConfirmedServiceACK) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	className := reflect.TypeOf(m.Child).String()
	className = "org.apache.plc4x.java.bacnetip.readwrite." + className[strings.LastIndex(className, ".")+1:]
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
