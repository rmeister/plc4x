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

#include <stdio.h>
#include <plc4c/spi/evaluation_helper.h>
#include "s7_message.h"

// Array of discriminator values that match the enum type constants.
// (The order is identical to the enum constants so we can use the
// enum constant to directly access a given types discriminator values)
const plc4c_s7_read_write_s7_message_discriminator plc4c_s7_read_write_s7_message_discriminators[] = {
  {/* plc4c_s7_read_write_s7_message_request */
   .messageType = 0x01},
  {/* plc4c_s7_read_write_s7_message_response */
   .messageType = 0x02},
  {/* plc4c_s7_read_write_s7_message_response_data */
   .messageType = 0x03},
  {/* plc4c_s7_read_write_s7_message_user_data */
   .messageType = 0x07}

};

// Function returning the discriminator values for a given type constant.
plc4c_s7_read_write_s7_message_discriminator plc4c_s7_read_write_s7_message_get_discriminator(plc4c_s7_read_write_s7_message_type type) {
  return plc4c_s7_read_write_s7_message_discriminators[type];
}

// Create an empty NULL-struct
static const plc4c_s7_read_write_s7_message plc4c_s7_read_write_s7_message_null_const;

plc4c_s7_read_write_s7_message plc4c_s7_read_write_s7_message_null() {
  return plc4c_s7_read_write_s7_message_null_const;
}


// Constant values.
static const uint8_t PLC4C_S7_READ_WRITE_S7_MESSAGE_PROTOCOL_ID_const = 0x32;
uint8_t PLC4C_S7_READ_WRITE_S7_MESSAGE_PROTOCOL_ID() {
  return PLC4C_S7_READ_WRITE_S7_MESSAGE_PROTOCOL_ID_const;
}

// Parse function.
plc4c_return_code plc4c_s7_read_write_s7_message_parse(plc4c_spi_read_buffer* io, plc4c_s7_read_write_s7_message** _message) {
  uint16_t startPos = plc4c_spi_read_get_pos(io);
  uint16_t curPos;
  plc4c_return_code _res = OK;

  // Allocate enough memory to contain this data structure.
  (*_message) = malloc(sizeof(plc4c_s7_read_write_s7_message));
  if(*_message == NULL) {
    return NO_MEMORY;
  }

  // Const Field (protocolId)
  uint8_t protocolId = 0;
  _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) &protocolId);
  if(_res != OK) {
    return _res;
  }
  if(protocolId != PLC4C_S7_READ_WRITE_S7_MESSAGE_PROTOCOL_ID()) {
    return PARSE_ERROR;
    // throw new ParseException("Expected constant value " + PLC4C_S7_READ_WRITE_S7_MESSAGE_PROTOCOL_ID + " but got " + protocolId);
  }
        // Discriminator Field (messageType)

  // Discriminator Field (messageType) (Used as input to a switch field)
  uint8_t messageType = 0;
  _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) &messageType);
  if(_res != OK) {
    return _res;
  }

  // Reserved Field (Compartmentalized so the "reserved" variable can't leak)
  {
    uint16_t _reserved = 0;
    _res = plc4c_spi_read_unsigned_short(io, 16, (uint16_t*) &_reserved);
    if(_res != OK) {
      return _res;
    }
    if(_reserved != 0x0000) {
      printf("Expected constant value '%d' but got '%d' for reserved field.", 0x0000, _reserved);
    }
  }

  // Simple Field (tpduReference)
  uint16_t tpduReference = 0;
  _res = plc4c_spi_read_unsigned_short(io, 16, (uint16_t*) &tpduReference);
  if(_res != OK) {
    return _res;
  }
  (*_message)->tpdu_reference = tpduReference;

  // Implicit Field (parameterLength) (Used for parsing, but it's value is not stored as it's implicitly given by the objects content)
  uint16_t parameterLength = 0;
  _res = plc4c_spi_read_unsigned_short(io, 16, (uint16_t*) &parameterLength);
  if(_res != OK) {
    return _res;
  }

  // Implicit Field (payloadLength) (Used for parsing, but it's value is not stored as it's implicitly given by the objects content)
  uint16_t payloadLength = 0;
  _res = plc4c_spi_read_unsigned_short(io, 16, (uint16_t*) &payloadLength);
  if(_res != OK) {
    return _res;
  }

  // Switch Field (Depending on the discriminator values, passes the instantiation to a sub-type)
  if(messageType == 0x01) { /* S7MessageRequest */
    (*_message)->_type = plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_request;
  } else 
  if(messageType == 0x02) { /* S7MessageResponse */
    (*_message)->_type = plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_response;
                    
    // Simple Field (errorClass)
    uint8_t errorClass = 0;
    _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) &errorClass);
    if(_res != OK) {
      return _res;
    }
    (*_message)->s7_message_response_error_class = errorClass;


                    
    // Simple Field (errorCode)
    uint8_t errorCode = 0;
    _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) &errorCode);
    if(_res != OK) {
      return _res;
    }
    (*_message)->s7_message_response_error_code = errorCode;

  } else 
  if(messageType == 0x03) { /* S7MessageResponseData */
    (*_message)->_type = plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_response_data;
                    
    // Simple Field (errorClass)
    uint8_t errorClass = 0;
    _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) &errorClass);
    if(_res != OK) {
      return _res;
    }
    (*_message)->s7_message_response_data_error_class = errorClass;


                    
    // Simple Field (errorCode)
    uint8_t errorCode = 0;
    _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) &errorCode);
    if(_res != OK) {
      return _res;
    }
    (*_message)->s7_message_response_data_error_code = errorCode;

  } else 
  if(messageType == 0x07) { /* S7MessageUserData */
    (*_message)->_type = plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_user_data;
  }

  // Optional Field (parameter) (Can be skipped, if a given expression evaluates to false)
  plc4c_s7_read_write_s7_parameter* parameter = NULL;
  if((parameterLength) > (0)) {
    parameter = malloc(sizeof(plc4c_s7_read_write_s7_parameter));
    if(parameter == NULL) {
      return NO_MEMORY;
    }
    _res = plc4c_s7_read_write_s7_parameter_parse(io, messageType, &parameter);
    if(_res != OK) {
      return _res;
    }
    (*_message)->parameter = parameter;
  } else {
    (*_message)->parameter = NULL;
  }

  // Optional Field (payload) (Can be skipped, if a given expression evaluates to false)
  plc4c_s7_read_write_s7_payload* payload = NULL;
  if((payloadLength) > (0)) {
    payload = malloc(sizeof(plc4c_s7_read_write_s7_payload));
    if(payload == NULL) {
      return NO_MEMORY;
    }
    _res = plc4c_s7_read_write_s7_payload_parse(io, messageType, parameter, &payload);
    if(_res != OK) {
      return _res;
    }
    (*_message)->payload = payload;
  } else {
    (*_message)->payload = NULL;
  }

  return OK;
}

plc4c_return_code plc4c_s7_read_write_s7_message_serialize(plc4c_spi_write_buffer* io, plc4c_s7_read_write_s7_message* _message) {
  plc4c_return_code _res = OK;

  // Const Field (protocolId)
  plc4c_spi_write_unsigned_byte(io, 8, PLC4C_S7_READ_WRITE_S7_MESSAGE_PROTOCOL_ID());

  // Discriminator Field (messageType)
  plc4c_spi_write_unsigned_byte(io, 8, plc4c_s7_read_write_s7_message_get_discriminator(_message->_type).messageType);

  // Reserved Field
  _res = plc4c_spi_write_unsigned_short(io, 16, 0x0000);
  if(_res != OK) {
    return _res;
  }

  // Simple Field (tpduReference)
  _res = plc4c_spi_write_unsigned_short(io, 16, _message->tpdu_reference);
  if(_res != OK) {
    return _res;
  }

  // Implicit Field (parameterLength) (Used for parsing, but it's value is not stored as it's implicitly given by the objects content)
  _res = plc4c_spi_write_unsigned_short(io, 16, (((_message->parameter) != (NULL)) ? plc4c_s7_read_write_s7_parameter_length_in_bytes(_message->parameter) : 0));
  if(_res != OK) {
    return _res;
  }

  // Implicit Field (payloadLength) (Used for parsing, but it's value is not stored as it's implicitly given by the objects content)
  _res = plc4c_spi_write_unsigned_short(io, 16, (((_message->payload) != (NULL)) ? plc4c_s7_read_write_s7_payload_length_in_bytes(_message->payload) : 0));
  if(_res != OK) {
    return _res;
  }

  // Switch Field (Depending of the current type, serialize the sub-type elements)
  switch(_message->_type) {
    case plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_request: {

      break;
    }
    case plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_response: {

      // Simple Field (errorClass)
      _res = plc4c_spi_write_unsigned_byte(io, 8, _message->s7_message_response_error_class);
      if(_res != OK) {
        return _res;
      }

      // Simple Field (errorCode)
      _res = plc4c_spi_write_unsigned_byte(io, 8, _message->s7_message_response_error_code);
      if(_res != OK) {
        return _res;
      }

      break;
    }
    case plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_response_data: {

      // Simple Field (errorClass)
      _res = plc4c_spi_write_unsigned_byte(io, 8, _message->s7_message_response_data_error_class);
      if(_res != OK) {
        return _res;
      }

      // Simple Field (errorCode)
      _res = plc4c_spi_write_unsigned_byte(io, 8, _message->s7_message_response_data_error_code);
      if(_res != OK) {
        return _res;
      }

      break;
    }
    case plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_user_data: {

      break;
    }
  }

  // Optional Field (parameter)
  if(_message->parameter != NULL) {
    _res = plc4c_s7_read_write_s7_parameter_serialize(io, _message->parameter);
    if(_res != OK) {
      return _res;
    }
  }

  // Optional Field (payload)
  if(_message->payload != NULL) {
    _res = plc4c_s7_read_write_s7_payload_serialize(io, _message->payload);
    if(_res != OK) {
      return _res;
    }
  }

  return OK;
}

uint16_t plc4c_s7_read_write_s7_message_length_in_bytes(plc4c_s7_read_write_s7_message* _message) {
  return plc4c_s7_read_write_s7_message_length_in_bits(_message) / 8;
}

uint16_t plc4c_s7_read_write_s7_message_length_in_bits(plc4c_s7_read_write_s7_message* _message) {
  uint16_t lengthInBits = 0;

  // Const Field (protocolId)
  lengthInBits += 8;

        // Discriminator Field (messageType)
                lengthInBits += 8;

  // Reserved Field (reserved)
  lengthInBits += 16;

  // Simple field (tpduReference)
  lengthInBits += 16;

  // Implicit Field (parameterLength)
  lengthInBits += 16;

  // Implicit Field (payloadLength)
  lengthInBits += 16;

  // Depending of the current type, add the length of sub-type elements ...
  switch(_message->_type) {
    case plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_request: {

      break;
    }
    case plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_response: {

      // Simple field (errorClass)
      lengthInBits += 8;


      // Simple field (errorCode)
      lengthInBits += 8;

      break;
    }
    case plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_response_data: {

      // Simple field (errorClass)
      lengthInBits += 8;


      // Simple field (errorCode)
      lengthInBits += 8;

      break;
    }
    case plc4c_s7_read_write_s7_message_type_plc4c_s7_read_write_s7_message_user_data: {

      break;
    }
  }

  // Optional Field (parameter)
  if(_message->parameter != NULL) {
    lengthInBits += plc4c_s7_read_write_s7_parameter_length_in_bits(_message->parameter);
  }

  // Optional Field (payload)
  if(_message->payload != NULL) {
    lengthInBits += plc4c_s7_read_write_s7_payload_length_in_bits(_message->payload);
  }

  return lengthInBits;
}

