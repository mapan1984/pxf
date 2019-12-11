package org.greenplum.pxf.api.io;

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


import org.greenplum.pxf.api.UnsupportedTypeException;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Supported Data Types and OIDs (GPDB Data Type identifiers).
 * There's a one-to-one match between a Data Type and it's corresponding OID.
 */
public enum DataType {
    BOOLEAN(16) {
        @Override
        public Boolean convertToNativeType(String value) {
            return Boolean.parseBoolean(value);
        }
    },
    BYTEA(17) {
        @Override
        public byte[] convertToNativeType(String value) {
            return value.getBytes();
        }
    },
    BIGINT(20) {
        @Override
        public Long convertToNativeType(String value) {
            return Long.parseLong(value);
        }
    },
    SMALLINT(21) {
        @Override
        public Integer convertToNativeType(String value) {
            return Integer.parseInt(value);
        }
    },
    INTEGER(23) {
        @Override
        public Integer convertToNativeType(String value) {
            return Integer.parseInt(value);
        }
    },
    TEXT(25) {
        @Override
        public String convertToNativeType(String value) {
            return value;
        }
    },
    REAL(700) {
        @Override
        public Float convertToNativeType(String value) {
            return Float.parseFloat(value);
        }
    },
    FLOAT8(701) {
        @Override
        public Double convertToNativeType(String value) {
            return Double.parseDouble(value);
        }
    },
    /**
     * char(length), blank-padded string, fixed storage length
     */
    BPCHAR(1042) {
        @Override
        public String convertToNativeType(String value) {
            return value;
        }
    },
    /**
     * varchar(length), non-blank-padded string, variable storage length
     */
    VARCHAR(1043) {
        @Override
        public String convertToNativeType(String value) {
            return value;
        }
    },
    DATE(1082) {
        @Override
        public Date convertToNativeType(String value) {
            return Date.valueOf(value);
        }
    },
    TIME(1083) {
        @Override
        public Time convertToNativeType(String value) {
            return Time.valueOf(value);
        }
    },
    TIMESTAMP(1114) {
        @Override
        public Object convertToNativeType(String value) {
            return Timestamp.valueOf(value);
        }
    },
    TIMESTAMP_WITH_TIME_ZONE(1184),
    NUMERIC(1700),

    INT2ARRAY(1005),
    INT4ARRAY(1007),
    INT8ARRAY(1016),
    BOOLARRAY(1000),
    TEXTARRAY(1009),
    FLOAT4ARRAY(1021),
    FLOAT8ARRAY(1022),

    UNSUPPORTED_TYPE(-1);

    private static final Map<Integer, DataType> lookup = new HashMap<>();
    private static final Set<Integer> notText = new HashSet<>();

    static {

        INT2ARRAY.typeElem = SMALLINT;
        INT4ARRAY.typeElem = INTEGER;
        INT8ARRAY.typeElem = BIGINT;
        BOOLARRAY.typeElem = BOOLEAN;
        TEXTARRAY.typeElem = TEXT;

        notText.add(BIGINT.OID);
        notText.add(BOOLEAN.OID);
        notText.add(BYTEA.OID);
        notText.add(FLOAT8.OID);
        notText.add(INTEGER.OID);
        notText.add(REAL.OID);
        notText.add(SMALLINT.OID);

        for (DataType dt : EnumSet.allOf(DataType.class)) {
            lookup.put(dt.getOID(), dt);
        }
    }

    private final int OID;
    private DataType typeElem;

    DataType(int OID) {
        this.OID = OID;
    }

    /**
     * Utility method for converting an {@link #OID} to a {@link #DataType}.
     *
     * @param OID the oid to be converted
     * @return the corresponding DataType if exists, else returns {@link #UNSUPPORTED_TYPE}
     */
    public static DataType get(int OID) {
        DataType type = lookup.get(OID);
        return type == null ? UNSUPPORTED_TYPE : type;
    }

    public static boolean isArrayType(int OID) {
        DataType type = lookup.get(OID);
        return type != null && type.typeElem != null;
    }

    public static boolean isTextForm(int OID) {
        return !notText.contains(OID);
    }

    public int getOID() {
        return OID;
    }

    public DataType getTypeElem() {
        return typeElem;
    }

    /**
     * Converts the string value to the java type of the data type
     *
     * @param value the value
     * @return the value in the java type
     */
    public Object convertToNativeType(String value) {
        throw new UnsupportedTypeException(String.format("DataType %s unsupported", this));
    }
}
