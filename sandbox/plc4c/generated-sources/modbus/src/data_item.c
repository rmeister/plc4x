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
#include <string.h>
#include <time.h>
#include <plc4c/data.h>
#include <plc4c/utils/list.h>
#include <plc4c/spi/evaluation_helper.h>

#include "data_item.h"

// Parse function.
plc4c_return_code plc4c_modbus_read_write_data_item_parse(plc4c_spi_read_buffer* io, plc4c_modbus_read_write_modbus_data_type dataType, uint16_t numberOfValues, plc4c_data** data_item) {
    uint16_t startPos = plc4c_spi_read_get_pos(io);
    uint16_t curPos;
    plc4c_return_code _res = OK;

        if((dataType == plc4c_modbus_read_write_modbus_data_type_BOOL) && (numberOfValues == 1)) { /* BOOL */

                // Reserved Field (Compartmentalized so the "reserved" variable can't leak)
                {
                    uint8_t _reserved = 0;
                    _res = plc4c_spi_read_unsigned_byte(io, 7, (uint8_t*) &_reserved);
                    if(_res != OK) {
                        return _res;
                    }
                    if(_reserved != 0x00) {
                      printf("Expected constant value '%d' but got '%d' for reserved field.", 0x00, _reserved);
                    }
                }

                // Simple Field (value)
                bool value = false;
                _res = plc4c_spi_read_bit(io, (bool*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_bool_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_BOOL) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            bool* _val = malloc(sizeof(bool) * 1);
            _res = plc4c_spi_read_bit(io, (bool*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_bool_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_BYTE) && (numberOfValues == 1)) { /* BitString */

                // Simple Field (value)
                uint8_t value = 0;
                _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                *data_item = plc4c_data_create_uint8_t_bit_string_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_BYTE) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            uint8_t* _val = malloc(sizeof(uint8_t) * 1);
            _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_uint8_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_WORD) && (numberOfValues == 1)) { /* BitString */

                // Simple Field (value)
                uint16_t value = 0;
                _res = plc4c_spi_read_unsigned_short(io, 16, (uint16_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                *data_item = plc4c_data_create_uint16_t_bit_string_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_WORD) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            uint16_t* _val = malloc(sizeof(uint16_t) * 1);
            _res = plc4c_spi_read_unsigned_short(io, 16, (uint16_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_uint16_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_DWORD) && (numberOfValues == 1)) { /* BitString */

                // Simple Field (value)
                uint32_t value = 0;
                _res = plc4c_spi_read_unsigned_int(io, 32, (uint32_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                *data_item = plc4c_data_create_uint32_t_bit_string_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_DWORD) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            uint32_t* _val = malloc(sizeof(uint32_t) * 1);
            _res = plc4c_spi_read_unsigned_int(io, 32, (uint32_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_uint32_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_LWORD) && (numberOfValues == 1)) { /* BitString */

                // Simple Field (value)
                uint64_t value = 0;
                _res = plc4c_spi_read_unsigned_long(io, 64, (uint64_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                *data_item = plc4c_data_create_uint64_t_bit_string_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_LWORD) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            uint64_t* _val = malloc(sizeof(uint64_t) * 1);
            _res = plc4c_spi_read_unsigned_long(io, 64, (uint64_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_uint64_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_SINT) && (numberOfValues == 1)) { /* SINT */

                // Simple Field (value)
                int8_t value = 0;
                _res = plc4c_spi_read_signed_byte(io, 8, (int8_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_int8_t_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_SINT) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            int8_t* _val = malloc(sizeof(int8_t) * 1);
            _res = plc4c_spi_read_signed_byte(io, 8, (int8_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_int8_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_INT) && (numberOfValues == 1)) { /* INT */

                // Simple Field (value)
                int16_t value = 0;
                _res = plc4c_spi_read_signed_short(io, 16, (int16_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_int16_t_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_INT) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            int16_t* _val = malloc(sizeof(int16_t) * 1);
            _res = plc4c_spi_read_signed_short(io, 16, (int16_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_int16_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_DINT) && (numberOfValues == 1)) { /* DINT */

                // Simple Field (value)
                int32_t value = 0;
                _res = plc4c_spi_read_signed_int(io, 32, (int32_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_int32_t_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_DINT) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            int32_t* _val = malloc(sizeof(int32_t) * 1);
            _res = plc4c_spi_read_signed_int(io, 32, (int32_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_int32_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_LINT) && (numberOfValues == 1)) { /* LINT */

                // Simple Field (value)
                int64_t value = 0;
                _res = plc4c_spi_read_signed_long(io, 64, (int64_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_int64_t_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_LINT) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            int64_t* _val = malloc(sizeof(int64_t) * 1);
            _res = plc4c_spi_read_signed_long(io, 64, (int64_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_int64_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_USINT) && (numberOfValues == 1)) { /* USINT */

                // Simple Field (value)
                uint8_t value = 0;
                _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_uint8_t_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_USINT) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            uint8_t* _val = malloc(sizeof(uint8_t) * 1);
            _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_uint8_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_UINT) && (numberOfValues == 1)) { /* UINT */

                // Simple Field (value)
                uint16_t value = 0;
                _res = plc4c_spi_read_unsigned_short(io, 16, (uint16_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_uint16_t_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_UINT) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            uint16_t* _val = malloc(sizeof(uint16_t) * 1);
            _res = plc4c_spi_read_unsigned_short(io, 16, (uint16_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_uint16_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_UDINT) && (numberOfValues == 1)) { /* UDINT */

                // Simple Field (value)
                uint32_t value = 0;
                _res = plc4c_spi_read_unsigned_int(io, 32, (uint32_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_uint32_t_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_UDINT) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            uint32_t* _val = malloc(sizeof(uint32_t) * 1);
            _res = plc4c_spi_read_unsigned_int(io, 32, (uint32_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_uint32_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_ULINT) && (numberOfValues == 1)) { /* ULINT */

                // Simple Field (value)
                uint64_t value = 0;
                _res = plc4c_spi_read_unsigned_long(io, 64, (uint64_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_uint64_t_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_ULINT) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            uint64_t* _val = malloc(sizeof(uint64_t) * 1);
            _res = plc4c_spi_read_unsigned_long(io, 64, (uint64_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_uint64_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_REAL) && (numberOfValues == 1)) { /* REAL */

                // Simple Field (value)
                float value = 0.0f;
                _res = plc4c_spi_read_float(io, 32, (float*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_float_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_REAL) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            float* _val = malloc(sizeof(float) * 1);
            _res = plc4c_spi_read_float(io, 32, (float*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_float_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_LREAL) && (numberOfValues == 1)) { /* LREAL */

                // Simple Field (value)
                double value = 0.0f;
                _res = plc4c_spi_read_double(io, 64, (double*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_double_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_LREAL) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            double* _val = malloc(sizeof(double) * 1);
            _res = plc4c_spi_read_double(io, 64, (double*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_double_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_CHAR) && (numberOfValues == 1)) { /* CHAR */

                // Simple Field (value)
                uint8_t value = 0;
                _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_uint8_t_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_CHAR) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            uint8_t* _val = malloc(sizeof(uint8_t) * 1);
            _res = plc4c_spi_read_unsigned_byte(io, 8, (uint8_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_uint8_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    } else         if((dataType == plc4c_modbus_read_write_modbus_data_type_WCHAR) && (numberOfValues == 1)) { /* WCHAR */

                // Simple Field (value)
                uint16_t value = 0;
                _res = plc4c_spi_read_unsigned_short(io, 16, (uint16_t*) &value);
                if(_res != OK) {
                    return _res;
                }

                    // Hurz
                *data_item = plc4c_data_create_uint16_t_data(value);

    } else         if(dataType == plc4c_modbus_read_write_modbus_data_type_WCHAR) { /* List */

        // Array field (value)
        // Count array
        plc4c_list* value;
        plc4c_utils_list_create(&value);
        int itemCount = (int) numberOfValues;
        for(int curItem = 0; curItem < itemCount; curItem++) {
            uint16_t* _val = malloc(sizeof(uint16_t) * 1);
            _res = plc4c_spi_read_unsigned_short(io, 16, (uint16_t*) _val);
            if(_res != OK) {
                return _res;
            }
            plc4c_data* _item = plc4c_data_create_uint16_t_data(*_val);
            plc4c_utils_list_insert_head_value(value, _item);
        }
        *data_item = plc4c_data_create_list_data(*value);

    }

  return OK;
}

plc4c_return_code plc4c_modbus_read_write_data_item_serialize(plc4c_spi_write_buffer* io, plc4c_data** data_item) {
  plc4c_return_code _res = OK;

  return OK;
}

uint16_t plc4c_modbus_read_write_data_item_length_in_bytes(plc4c_data* data_item) {
  return plc4c_modbus_read_write_data_item_length_in_bits(data_item) / 8;
}

uint16_t plc4c_modbus_read_write_data_item_length_in_bits(plc4c_data* data_item) {
  uint16_t lengthInBits = 0;

  return lengthInBits;
}

