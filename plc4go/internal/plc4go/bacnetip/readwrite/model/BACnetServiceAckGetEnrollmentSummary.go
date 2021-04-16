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
	"io"
)

// Code generated by build-utils. DO NOT EDIT.

// The data-structure of this message
type BACnetServiceAckGetEnrollmentSummary struct {
	Parent *BACnetServiceAck
}

// The corresponding interface
type IBACnetServiceAckGetEnrollmentSummary interface {
	LengthInBytes() uint16
	LengthInBits() uint16
	Serialize(io utils.WriteBuffer) error
	xml.Marshaler
	xml.Unmarshaler
}

///////////////////////////////////////////////////////////
// Accessors for discriminator values.
///////////////////////////////////////////////////////////
func (m *BACnetServiceAckGetEnrollmentSummary) ServiceChoice() uint8 {
	return 0x04
}

func (m *BACnetServiceAckGetEnrollmentSummary) InitializeParent(parent *BACnetServiceAck) {
}

func NewBACnetServiceAckGetEnrollmentSummary() *BACnetServiceAck {
	child := &BACnetServiceAckGetEnrollmentSummary{
		Parent: NewBACnetServiceAck(),
	}
	child.Parent.Child = child
	return child.Parent
}

func CastBACnetServiceAckGetEnrollmentSummary(structType interface{}) *BACnetServiceAckGetEnrollmentSummary {
	castFunc := func(typ interface{}) *BACnetServiceAckGetEnrollmentSummary {
		if casted, ok := typ.(BACnetServiceAckGetEnrollmentSummary); ok {
			return &casted
		}
		if casted, ok := typ.(*BACnetServiceAckGetEnrollmentSummary); ok {
			return casted
		}
		if casted, ok := typ.(BACnetServiceAck); ok {
			return CastBACnetServiceAckGetEnrollmentSummary(casted.Child)
		}
		if casted, ok := typ.(*BACnetServiceAck); ok {
			return CastBACnetServiceAckGetEnrollmentSummary(casted.Child)
		}
		return nil
	}
	return castFunc(structType)
}

func (m *BACnetServiceAckGetEnrollmentSummary) GetTypeName() string {
	return "BACnetServiceAckGetEnrollmentSummary"
}

func (m *BACnetServiceAckGetEnrollmentSummary) LengthInBits() uint16 {
	return m.LengthInBitsConditional(false)
}

func (m *BACnetServiceAckGetEnrollmentSummary) LengthInBitsConditional(lastItem bool) uint16 {
	lengthInBits := uint16(m.Parent.ParentLengthInBits())

	return lengthInBits
}

func (m *BACnetServiceAckGetEnrollmentSummary) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func BACnetServiceAckGetEnrollmentSummaryParse(io *utils.ReadBuffer) (*BACnetServiceAck, error) {

	// Create a partially initialized instance
	_child := &BACnetServiceAckGetEnrollmentSummary{
		Parent: &BACnetServiceAck{},
	}
	_child.Parent.Child = _child
	return _child.Parent, nil
}

func (m *BACnetServiceAckGetEnrollmentSummary) Serialize(io utils.WriteBuffer) error {
	ser := func() error {

		return nil
	}
	return m.Parent.SerializeParent(io, m, ser)
}

func (m *BACnetServiceAckGetEnrollmentSummary) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
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

func (m *BACnetServiceAckGetEnrollmentSummary) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	return nil
}

func (m BACnetServiceAckGetEnrollmentSummary) String() string {
	return string(m.Box("BACnetServiceAckGetEnrollmentSummary", utils.DefaultWidth*2))
}

func (m BACnetServiceAckGetEnrollmentSummary) Box(name string, width int) utils.AsciiBox {
	if name == "" {
		name = "BACnetServiceAckGetEnrollmentSummary"
	}
	boxes := make([]utils.AsciiBox, 0)
	return utils.BoxBox(name, utils.AlignBoxes(boxes, width-2), 0)
}
