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
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi/utils"
	"github.com/pkg/errors"
	"io"
)

// Code generated by build-utils. DO NOT EDIT.

// The data-structure of this message
type BACnetTagApplicationDouble struct {
	Value  float64
	Parent *BACnetTag
}

// The corresponding interface
type IBACnetTagApplicationDouble interface {
	LengthInBytes() uint16
	LengthInBits() uint16
	Serialize(io utils.WriteBuffer) error
	xml.Marshaler
	xml.Unmarshaler
}

///////////////////////////////////////////////////////////
// Accessors for discriminator values.
///////////////////////////////////////////////////////////
func (m *BACnetTagApplicationDouble) ContextSpecificTag() uint8 {
	return 0
}

func (m *BACnetTagApplicationDouble) InitializeParent(parent *BACnetTag, typeOrTagNumber uint8, lengthValueType uint8, extTagNumber *uint8, extLength *uint8) {
	m.Parent.TypeOrTagNumber = typeOrTagNumber
	m.Parent.LengthValueType = lengthValueType
	m.Parent.ExtTagNumber = extTagNumber
	m.Parent.ExtLength = extLength
}

func NewBACnetTagApplicationDouble(value float64, typeOrTagNumber uint8, lengthValueType uint8, extTagNumber *uint8, extLength *uint8) *BACnetTag {
	child := &BACnetTagApplicationDouble{
		Value:  value,
		Parent: NewBACnetTag(typeOrTagNumber, lengthValueType, extTagNumber, extLength),
	}
	child.Parent.Child = child
	return child.Parent
}

func CastBACnetTagApplicationDouble(structType interface{}) *BACnetTagApplicationDouble {
	castFunc := func(typ interface{}) *BACnetTagApplicationDouble {
		if casted, ok := typ.(BACnetTagApplicationDouble); ok {
			return &casted
		}
		if casted, ok := typ.(*BACnetTagApplicationDouble); ok {
			return casted
		}
		if casted, ok := typ.(BACnetTag); ok {
			return CastBACnetTagApplicationDouble(casted.Child)
		}
		if casted, ok := typ.(*BACnetTag); ok {
			return CastBACnetTagApplicationDouble(casted.Child)
		}
		return nil
	}
	return castFunc(structType)
}

func (m *BACnetTagApplicationDouble) GetTypeName() string {
	return "BACnetTagApplicationDouble"
}

func (m *BACnetTagApplicationDouble) LengthInBits() uint16 {
	return m.LengthInBitsConditional(false)
}

func (m *BACnetTagApplicationDouble) LengthInBitsConditional(lastItem bool) uint16 {
	lengthInBits := uint16(m.Parent.ParentLengthInBits())

	// Simple field (value)
	lengthInBits += 64

	return lengthInBits
}

func (m *BACnetTagApplicationDouble) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func BACnetTagApplicationDoubleParse(io *utils.ReadBuffer, lengthValueType uint8, extLength uint8) (*BACnetTag, error) {

	// Simple Field (value)
	value, _valueErr := io.ReadFloat64(true, 11, 52)
	if _valueErr != nil {
		return nil, errors.Wrap(_valueErr, "Error parsing 'value' field")
	}

	// Create a partially initialized instance
	_child := &BACnetTagApplicationDouble{
		Value:  value,
		Parent: &BACnetTag{},
	}
	_child.Parent.Child = _child
	return _child.Parent, nil
}

func (m *BACnetTagApplicationDouble) Serialize(io utils.WriteBuffer) error {
	ser := func() error {

		// Simple Field (value)
		value := float64(m.Value)
		_valueErr := io.WriteFloat64(64, (value))
		if _valueErr != nil {
			return errors.Wrap(_valueErr, "Error serializing 'value' field")
		}

		return nil
	}
	return m.Parent.SerializeParent(io, m, ser)
}

func (m *BACnetTagApplicationDouble) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var token xml.Token
	var err error
	foundContent := false
	token = start
	for {
		switch token.(type) {
		case xml.StartElement:
			foundContent = true
			tok := token.(xml.StartElement)
			switch tok.Name.Local {
			case "value":
				var data float64
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.Value = data
			}
		}
		token, err = d.Token()
		if err != nil {
			if err == io.EOF && foundContent {
				return nil
			}
			return err
		}
	}
}

func (m *BACnetTagApplicationDouble) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeElement(m.Value, xml.StartElement{Name: xml.Name{Local: "value"}}); err != nil {
		return err
	}
	return nil
}

func (m BACnetTagApplicationDouble) String() string {
	return string(m.Box("BACnetTagApplicationDouble", utils.DefaultWidth*2))
}

func (m BACnetTagApplicationDouble) Box(name string, width int) utils.AsciiBox {
	if name == "" {
		name = "BACnetTagApplicationDouble"
	}
	boxes := make([]utils.AsciiBox, 0)
	boxes = append(boxes, utils.BoxAnything("Value", m.Value, width-2))
	return utils.BoxBox(name, utils.AlignBoxes(boxes, width-2), 0)
}
