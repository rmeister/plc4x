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
type ComObjectTable struct {
	Child IComObjectTableChild
	IComObjectTable
	IComObjectTableParent
}

// The corresponding interface
type IComObjectTable interface {
	FirmwareType() FirmwareType
	LengthInBytes() uint16
	LengthInBits() uint16
	Serialize(io utils.WriteBuffer) error
	xml.Marshaler
}

type IComObjectTableParent interface {
	SerializeParent(io utils.WriteBuffer, child IComObjectTable, serializeChildFunction func() error) error
	GetTypeName() string
}

type IComObjectTableChild interface {
	Serialize(io utils.WriteBuffer) error
	InitializeParent(parent *ComObjectTable)
	GetTypeName() string
	IComObjectTable
}

func NewComObjectTable() *ComObjectTable {
	return &ComObjectTable{}
}

func CastComObjectTable(structType interface{}) *ComObjectTable {
	castFunc := func(typ interface{}) *ComObjectTable {
		if casted, ok := typ.(ComObjectTable); ok {
			return &casted
		}
		if casted, ok := typ.(*ComObjectTable); ok {
			return casted
		}
		return nil
	}
	return castFunc(structType)
}

func (m *ComObjectTable) GetTypeName() string {
	return "ComObjectTable"
}

func (m *ComObjectTable) LengthInBits() uint16 {
	lengthInBits := uint16(0)

	// Length of sub-type elements will be added by sub-type...
	lengthInBits += m.Child.LengthInBits()

	return lengthInBits
}

func (m *ComObjectTable) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func ComObjectTableParse(io *utils.ReadBuffer, firmwareType *FirmwareType) (*ComObjectTable, error) {

	// Switch Field (Depending on the discriminator values, passes the instantiation to a sub-type)
	var _parent *ComObjectTable
	var typeSwitchError error
	switch {
	case *firmwareType == FirmwareType_SYSTEM_1:
		_parent, typeSwitchError = ComObjectTableRealisationType1Parse(io)
	case *firmwareType == FirmwareType_SYSTEM_2:
		_parent, typeSwitchError = ComObjectTableRealisationType2Parse(io)
	case *firmwareType == FirmwareType_SYSTEM_300:
		_parent, typeSwitchError = ComObjectTableRealisationType6Parse(io)
	}
	if typeSwitchError != nil {
		return nil, errors.New("Error parsing sub-type for type-switch. " + typeSwitchError.Error())
	}

	// Finish initializing
	_parent.Child.InitializeParent(_parent)
	return _parent, nil
}

func (m *ComObjectTable) Serialize(io utils.WriteBuffer) error {
	return m.Child.Serialize(io)
}

func (m *ComObjectTable) SerializeParent(io utils.WriteBuffer, child IComObjectTable, serializeChildFunction func() error) error {

	// Switch field (Depending on the discriminator values, passes the serialization to a sub-type)
	_typeSwitchErr := serializeChildFunction()
	if _typeSwitchErr != nil {
		return errors.New("Error serializing sub-type field " + _typeSwitchErr.Error())
	}

	return nil
}

func (m *ComObjectTable) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
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
				case "org.apache.plc4x.java.knxnetip.readwrite.ComObjectTableRealisationType1":
					var dt *ComObjectTableRealisationType1
					if m.Child != nil {
						dt = m.Child.(*ComObjectTableRealisationType1)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.knxnetip.readwrite.ComObjectTableRealisationType2":
					var dt *ComObjectTableRealisationType2
					if m.Child != nil {
						dt = m.Child.(*ComObjectTableRealisationType2)
					}
					if err := d.DecodeElement(&dt, &tok); err != nil {
						return err
					}
					if m.Child == nil {
						dt.Parent = m
						m.Child = dt
					}
				case "org.apache.plc4x.java.knxnetip.readwrite.ComObjectTableRealisationType6":
					var dt *ComObjectTableRealisationType6
					if m.Child != nil {
						dt = m.Child.(*ComObjectTableRealisationType6)
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

func (m *ComObjectTable) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	className := reflect.TypeOf(m.Child).String()
	className = "org.apache.plc4x.java.knxnetip.readwrite." + className[strings.LastIndex(className, ".")+1:]
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
