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

type KnxInterfaceObjectType uint16

type IKnxInterfaceObjectType interface {
	Code() string
	Name() string
	Serialize(io utils.WriteBuffer) error
	xml.Marshaler
	xml.Unmarshaler
}

const (
	KnxInterfaceObjectType_OT_UNKNOWN                        KnxInterfaceObjectType = 0
	KnxInterfaceObjectType_OT_GENERAL                        KnxInterfaceObjectType = 1
	KnxInterfaceObjectType_OT_DEVICE                         KnxInterfaceObjectType = 2
	KnxInterfaceObjectType_OT_ADDRESS_TABLE                  KnxInterfaceObjectType = 3
	KnxInterfaceObjectType_OT_ASSOCIATION_TABLE              KnxInterfaceObjectType = 4
	KnxInterfaceObjectType_OT_APPLICATION_PROGRAM            KnxInterfaceObjectType = 5
	KnxInterfaceObjectType_OT_INTERACE_PROGRAM               KnxInterfaceObjectType = 6
	KnxInterfaceObjectType_OT_EIBOBJECT_ASSOCIATATION_TABLE  KnxInterfaceObjectType = 7
	KnxInterfaceObjectType_OT_ROUTER                         KnxInterfaceObjectType = 8
	KnxInterfaceObjectType_OT_LTE_ADDRESS_ROUTING_TABLE      KnxInterfaceObjectType = 9
	KnxInterfaceObjectType_OT_CEMI_SERVER                    KnxInterfaceObjectType = 10
	KnxInterfaceObjectType_OT_GROUP_OBJECT_TABLE             KnxInterfaceObjectType = 11
	KnxInterfaceObjectType_OT_POLLING_MASTER                 KnxInterfaceObjectType = 12
	KnxInterfaceObjectType_OT_KNXIP_PARAMETER                KnxInterfaceObjectType = 13
	KnxInterfaceObjectType_OT_FILE_SERVER                    KnxInterfaceObjectType = 14
	KnxInterfaceObjectType_OT_SECURITY                       KnxInterfaceObjectType = 15
	KnxInterfaceObjectType_OT_RF_MEDIUM                      KnxInterfaceObjectType = 16
	KnxInterfaceObjectType_OT_INDOOR_BRIGHTNESS_SENSOR       KnxInterfaceObjectType = 17
	KnxInterfaceObjectType_OT_INDOOR_LUMINANCE_SENSOR        KnxInterfaceObjectType = 18
	KnxInterfaceObjectType_OT_LIGHT_SWITCHING_ACTUATOR_BASIC KnxInterfaceObjectType = 19
	KnxInterfaceObjectType_OT_DIMMING_ACTUATOR_BASIC         KnxInterfaceObjectType = 20
	KnxInterfaceObjectType_OT_DIMMING_SENSOR_BASIC           KnxInterfaceObjectType = 21
	KnxInterfaceObjectType_OT_SWITCHING_SENSOR_BASIC         KnxInterfaceObjectType = 22
	KnxInterfaceObjectType_OT_SUNBLIND_ACTUATOR_BASIC        KnxInterfaceObjectType = 23
	KnxInterfaceObjectType_OT_SUNBLIND_SENSOR_BASIC          KnxInterfaceObjectType = 24
)

var KnxInterfaceObjectTypeValues []KnxInterfaceObjectType

func init() {
	KnxInterfaceObjectTypeValues = []KnxInterfaceObjectType{
		KnxInterfaceObjectType_OT_UNKNOWN,
		KnxInterfaceObjectType_OT_GENERAL,
		KnxInterfaceObjectType_OT_DEVICE,
		KnxInterfaceObjectType_OT_ADDRESS_TABLE,
		KnxInterfaceObjectType_OT_ASSOCIATION_TABLE,
		KnxInterfaceObjectType_OT_APPLICATION_PROGRAM,
		KnxInterfaceObjectType_OT_INTERACE_PROGRAM,
		KnxInterfaceObjectType_OT_EIBOBJECT_ASSOCIATATION_TABLE,
		KnxInterfaceObjectType_OT_ROUTER,
		KnxInterfaceObjectType_OT_LTE_ADDRESS_ROUTING_TABLE,
		KnxInterfaceObjectType_OT_CEMI_SERVER,
		KnxInterfaceObjectType_OT_GROUP_OBJECT_TABLE,
		KnxInterfaceObjectType_OT_POLLING_MASTER,
		KnxInterfaceObjectType_OT_KNXIP_PARAMETER,
		KnxInterfaceObjectType_OT_FILE_SERVER,
		KnxInterfaceObjectType_OT_SECURITY,
		KnxInterfaceObjectType_OT_RF_MEDIUM,
		KnxInterfaceObjectType_OT_INDOOR_BRIGHTNESS_SENSOR,
		KnxInterfaceObjectType_OT_INDOOR_LUMINANCE_SENSOR,
		KnxInterfaceObjectType_OT_LIGHT_SWITCHING_ACTUATOR_BASIC,
		KnxInterfaceObjectType_OT_DIMMING_ACTUATOR_BASIC,
		KnxInterfaceObjectType_OT_DIMMING_SENSOR_BASIC,
		KnxInterfaceObjectType_OT_SWITCHING_SENSOR_BASIC,
		KnxInterfaceObjectType_OT_SUNBLIND_ACTUATOR_BASIC,
		KnxInterfaceObjectType_OT_SUNBLIND_SENSOR_BASIC,
	}
}

func (e KnxInterfaceObjectType) Code() string {
	switch e {
	case 0:
		{ /* '0' */
			return "U"
		}
	case 1:
		{ /* '1' */
			return "G"
		}
	case 10:
		{ /* '10' */
			return "8"
		}
	case 11:
		{ /* '11' */
			return "9"
		}
	case 12:
		{ /* '12' */
			return "10"
		}
	case 13:
		{ /* '13' */
			return "11"
		}
	case 14:
		{ /* '14' */
			return "13"
		}
	case 15:
		{ /* '15' */
			return "17"
		}
	case 16:
		{ /* '16' */
			return "19"
		}
	case 17:
		{ /* '17' */
			return "409"
		}
	case 18:
		{ /* '18' */
			return "410"
		}
	case 19:
		{ /* '19' */
			return "417"
		}
	case 2:
		{ /* '2' */
			return "0"
		}
	case 20:
		{ /* '20' */
			return "418"
		}
	case 21:
		{ /* '21' */
			return "420"
		}
	case 22:
		{ /* '22' */
			return "421"
		}
	case 23:
		{ /* '23' */
			return "800"
		}
	case 24:
		{ /* '24' */
			return "801"
		}
	case 3:
		{ /* '3' */
			return "1"
		}
	case 4:
		{ /* '4' */
			return "2"
		}
	case 5:
		{ /* '5' */
			return "3"
		}
	case 6:
		{ /* '6' */
			return "4"
		}
	case 7:
		{ /* '7' */
			return "5"
		}
	case 8:
		{ /* '8' */
			return "6"
		}
	case 9:
		{ /* '9' */
			return "7"
		}
	default:
		{
			return ""
		}
	}
}

func (e KnxInterfaceObjectType) Name() string {
	switch e {
	case 0:
		{ /* '0' */
			return "Unknown Interface Object Type"
		}
	case 1:
		{ /* '1' */
			return "General Interface Object Type"
		}
	case 10:
		{ /* '10' */
			return "cEMI Server Object"
		}
	case 11:
		{ /* '11' */
			return "Group Object Table Object"
		}
	case 12:
		{ /* '12' */
			return "Polling Master"
		}
	case 13:
		{ /* '13' */
			return "KNXnet/IP Parameter Object"
		}
	case 14:
		{ /* '14' */
			return "File Server Object"
		}
	case 15:
		{ /* '15' */
			return "Security Object"
		}
	case 16:
		{ /* '16' */
			return "RF Medium Object"
		}
	case 17:
		{ /* '17' */
			return "Indoor Brightness Sensor"
		}
	case 18:
		{ /* '18' */
			return "Indoor Luminance Sensor"
		}
	case 19:
		{ /* '19' */
			return "Light Switching Actuator Basic"
		}
	case 2:
		{ /* '2' */
			return "Device Object"
		}
	case 20:
		{ /* '20' */
			return "Dimming Actuator Basic"
		}
	case 21:
		{ /* '21' */
			return "Dimming   Sensor Basic"
		}
	case 22:
		{ /* '22' */
			return "Switching Sensor Basic"
		}
	case 23:
		{ /* '23' */
			return "Sunblind Actuator Basic"
		}
	case 24:
		{ /* '24' */
			return "Sunblind Sensor Basic"
		}
	case 3:
		{ /* '3' */
			return "Addresstable Object"
		}
	case 4:
		{ /* '4' */
			return "Associationtable Object"
		}
	case 5:
		{ /* '5' */
			return "Applicationprogram Object"
		}
	case 6:
		{ /* '6' */
			return "Interfaceprogram Object"
		}
	case 7:
		{ /* '7' */
			return "KNX-Object Associationtable Object"
		}
	case 8:
		{ /* '8' */
			return "Router Object"
		}
	case 9:
		{ /* '9' */
			return "LTE Address Routing Table Object"
		}
	default:
		{
			return ""
		}
	}
}
func KnxInterfaceObjectTypeByValue(value uint16) KnxInterfaceObjectType {
	switch value {
	case 0:
		return KnxInterfaceObjectType_OT_UNKNOWN
	case 1:
		return KnxInterfaceObjectType_OT_GENERAL
	case 10:
		return KnxInterfaceObjectType_OT_CEMI_SERVER
	case 11:
		return KnxInterfaceObjectType_OT_GROUP_OBJECT_TABLE
	case 12:
		return KnxInterfaceObjectType_OT_POLLING_MASTER
	case 13:
		return KnxInterfaceObjectType_OT_KNXIP_PARAMETER
	case 14:
		return KnxInterfaceObjectType_OT_FILE_SERVER
	case 15:
		return KnxInterfaceObjectType_OT_SECURITY
	case 16:
		return KnxInterfaceObjectType_OT_RF_MEDIUM
	case 17:
		return KnxInterfaceObjectType_OT_INDOOR_BRIGHTNESS_SENSOR
	case 18:
		return KnxInterfaceObjectType_OT_INDOOR_LUMINANCE_SENSOR
	case 19:
		return KnxInterfaceObjectType_OT_LIGHT_SWITCHING_ACTUATOR_BASIC
	case 2:
		return KnxInterfaceObjectType_OT_DEVICE
	case 20:
		return KnxInterfaceObjectType_OT_DIMMING_ACTUATOR_BASIC
	case 21:
		return KnxInterfaceObjectType_OT_DIMMING_SENSOR_BASIC
	case 22:
		return KnxInterfaceObjectType_OT_SWITCHING_SENSOR_BASIC
	case 23:
		return KnxInterfaceObjectType_OT_SUNBLIND_ACTUATOR_BASIC
	case 24:
		return KnxInterfaceObjectType_OT_SUNBLIND_SENSOR_BASIC
	case 3:
		return KnxInterfaceObjectType_OT_ADDRESS_TABLE
	case 4:
		return KnxInterfaceObjectType_OT_ASSOCIATION_TABLE
	case 5:
		return KnxInterfaceObjectType_OT_APPLICATION_PROGRAM
	case 6:
		return KnxInterfaceObjectType_OT_INTERACE_PROGRAM
	case 7:
		return KnxInterfaceObjectType_OT_EIBOBJECT_ASSOCIATATION_TABLE
	case 8:
		return KnxInterfaceObjectType_OT_ROUTER
	case 9:
		return KnxInterfaceObjectType_OT_LTE_ADDRESS_ROUTING_TABLE
	}
	return 0
}

func KnxInterfaceObjectTypeByName(value string) KnxInterfaceObjectType {
	switch value {
	case "OT_UNKNOWN":
		return KnxInterfaceObjectType_OT_UNKNOWN
	case "OT_GENERAL":
		return KnxInterfaceObjectType_OT_GENERAL
	case "OT_CEMI_SERVER":
		return KnxInterfaceObjectType_OT_CEMI_SERVER
	case "OT_GROUP_OBJECT_TABLE":
		return KnxInterfaceObjectType_OT_GROUP_OBJECT_TABLE
	case "OT_POLLING_MASTER":
		return KnxInterfaceObjectType_OT_POLLING_MASTER
	case "OT_KNXIP_PARAMETER":
		return KnxInterfaceObjectType_OT_KNXIP_PARAMETER
	case "OT_FILE_SERVER":
		return KnxInterfaceObjectType_OT_FILE_SERVER
	case "OT_SECURITY":
		return KnxInterfaceObjectType_OT_SECURITY
	case "OT_RF_MEDIUM":
		return KnxInterfaceObjectType_OT_RF_MEDIUM
	case "OT_INDOOR_BRIGHTNESS_SENSOR":
		return KnxInterfaceObjectType_OT_INDOOR_BRIGHTNESS_SENSOR
	case "OT_INDOOR_LUMINANCE_SENSOR":
		return KnxInterfaceObjectType_OT_INDOOR_LUMINANCE_SENSOR
	case "OT_LIGHT_SWITCHING_ACTUATOR_BASIC":
		return KnxInterfaceObjectType_OT_LIGHT_SWITCHING_ACTUATOR_BASIC
	case "OT_DEVICE":
		return KnxInterfaceObjectType_OT_DEVICE
	case "OT_DIMMING_ACTUATOR_BASIC":
		return KnxInterfaceObjectType_OT_DIMMING_ACTUATOR_BASIC
	case "OT_DIMMING_SENSOR_BASIC":
		return KnxInterfaceObjectType_OT_DIMMING_SENSOR_BASIC
	case "OT_SWITCHING_SENSOR_BASIC":
		return KnxInterfaceObjectType_OT_SWITCHING_SENSOR_BASIC
	case "OT_SUNBLIND_ACTUATOR_BASIC":
		return KnxInterfaceObjectType_OT_SUNBLIND_ACTUATOR_BASIC
	case "OT_SUNBLIND_SENSOR_BASIC":
		return KnxInterfaceObjectType_OT_SUNBLIND_SENSOR_BASIC
	case "OT_ADDRESS_TABLE":
		return KnxInterfaceObjectType_OT_ADDRESS_TABLE
	case "OT_ASSOCIATION_TABLE":
		return KnxInterfaceObjectType_OT_ASSOCIATION_TABLE
	case "OT_APPLICATION_PROGRAM":
		return KnxInterfaceObjectType_OT_APPLICATION_PROGRAM
	case "OT_INTERACE_PROGRAM":
		return KnxInterfaceObjectType_OT_INTERACE_PROGRAM
	case "OT_EIBOBJECT_ASSOCIATATION_TABLE":
		return KnxInterfaceObjectType_OT_EIBOBJECT_ASSOCIATATION_TABLE
	case "OT_ROUTER":
		return KnxInterfaceObjectType_OT_ROUTER
	case "OT_LTE_ADDRESS_ROUTING_TABLE":
		return KnxInterfaceObjectType_OT_LTE_ADDRESS_ROUTING_TABLE
	}
	return 0
}

func CastKnxInterfaceObjectType(structType interface{}) KnxInterfaceObjectType {
	castFunc := func(typ interface{}) KnxInterfaceObjectType {
		if sKnxInterfaceObjectType, ok := typ.(KnxInterfaceObjectType); ok {
			return sKnxInterfaceObjectType
		}
		return 0
	}
	return castFunc(structType)
}

func (m KnxInterfaceObjectType) LengthInBits() uint16 {
	return 16
}

func (m KnxInterfaceObjectType) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func KnxInterfaceObjectTypeParse(io *utils.ReadBuffer) (KnxInterfaceObjectType, error) {
	val, err := io.ReadUint16(16)
	if err != nil {
		return 0, nil
	}
	return KnxInterfaceObjectTypeByValue(val), nil
}

func (e KnxInterfaceObjectType) Serialize(io utils.WriteBuffer) error {
	err := io.WriteUint16(16, uint16(e))
	return err
}

func (m *KnxInterfaceObjectType) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
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
		case xml.CharData:
			tok := token.(xml.CharData)
			*m = KnxInterfaceObjectTypeByName(string(tok))
		}
	}
}

func (m KnxInterfaceObjectType) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeElement(m.String(), start); err != nil {
		return err
	}
	return nil
}

func (e KnxInterfaceObjectType) String() string {
	switch e {
	case KnxInterfaceObjectType_OT_UNKNOWN:
		return "OT_UNKNOWN"
	case KnxInterfaceObjectType_OT_GENERAL:
		return "OT_GENERAL"
	case KnxInterfaceObjectType_OT_CEMI_SERVER:
		return "OT_CEMI_SERVER"
	case KnxInterfaceObjectType_OT_GROUP_OBJECT_TABLE:
		return "OT_GROUP_OBJECT_TABLE"
	case KnxInterfaceObjectType_OT_POLLING_MASTER:
		return "OT_POLLING_MASTER"
	case KnxInterfaceObjectType_OT_KNXIP_PARAMETER:
		return "OT_KNXIP_PARAMETER"
	case KnxInterfaceObjectType_OT_FILE_SERVER:
		return "OT_FILE_SERVER"
	case KnxInterfaceObjectType_OT_SECURITY:
		return "OT_SECURITY"
	case KnxInterfaceObjectType_OT_RF_MEDIUM:
		return "OT_RF_MEDIUM"
	case KnxInterfaceObjectType_OT_INDOOR_BRIGHTNESS_SENSOR:
		return "OT_INDOOR_BRIGHTNESS_SENSOR"
	case KnxInterfaceObjectType_OT_INDOOR_LUMINANCE_SENSOR:
		return "OT_INDOOR_LUMINANCE_SENSOR"
	case KnxInterfaceObjectType_OT_LIGHT_SWITCHING_ACTUATOR_BASIC:
		return "OT_LIGHT_SWITCHING_ACTUATOR_BASIC"
	case KnxInterfaceObjectType_OT_DEVICE:
		return "OT_DEVICE"
	case KnxInterfaceObjectType_OT_DIMMING_ACTUATOR_BASIC:
		return "OT_DIMMING_ACTUATOR_BASIC"
	case KnxInterfaceObjectType_OT_DIMMING_SENSOR_BASIC:
		return "OT_DIMMING_SENSOR_BASIC"
	case KnxInterfaceObjectType_OT_SWITCHING_SENSOR_BASIC:
		return "OT_SWITCHING_SENSOR_BASIC"
	case KnxInterfaceObjectType_OT_SUNBLIND_ACTUATOR_BASIC:
		return "OT_SUNBLIND_ACTUATOR_BASIC"
	case KnxInterfaceObjectType_OT_SUNBLIND_SENSOR_BASIC:
		return "OT_SUNBLIND_SENSOR_BASIC"
	case KnxInterfaceObjectType_OT_ADDRESS_TABLE:
		return "OT_ADDRESS_TABLE"
	case KnxInterfaceObjectType_OT_ASSOCIATION_TABLE:
		return "OT_ASSOCIATION_TABLE"
	case KnxInterfaceObjectType_OT_APPLICATION_PROGRAM:
		return "OT_APPLICATION_PROGRAM"
	case KnxInterfaceObjectType_OT_INTERACE_PROGRAM:
		return "OT_INTERACE_PROGRAM"
	case KnxInterfaceObjectType_OT_EIBOBJECT_ASSOCIATATION_TABLE:
		return "OT_EIBOBJECT_ASSOCIATATION_TABLE"
	case KnxInterfaceObjectType_OT_ROUTER:
		return "OT_ROUTER"
	case KnxInterfaceObjectType_OT_LTE_ADDRESS_ROUTING_TABLE:
		return "OT_LTE_ADDRESS_ROUTING_TABLE"
	}
	return ""
}
