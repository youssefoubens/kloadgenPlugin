package com.sngular.kloadgen.serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class AvroJsonDecimalFixer3 {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Cache for schema analysis results
    private static final Map<String, SchemaInfo> schemaCache = new ConcurrentHashMap<>();

    // Data structures to cache schema analysis
    private static class SchemaInfo {
        final Map<String, FieldInfo> decimalFields;
        final Map<String, Schema> recordFieldSchemas;
        final Schema arrayElementSchema;
        final Schema mapValueSchema;
        final boolean isDecimalType;

        SchemaInfo(Map<String, FieldInfo> decimalFields,
                   Map<String, Schema> recordFieldSchemas,
                   Schema arrayElementSchema,
                   Schema mapValueSchema,
                   boolean isDecimalType) {
            this.decimalFields = decimalFields != null ?
                    Collections.unmodifiableMap(decimalFields) : Collections.emptyMap();
            this.recordFieldSchemas = recordFieldSchemas != null ?
                    Collections.unmodifiableMap(recordFieldSchemas) : Collections.emptyMap();
            this.arrayElementSchema = arrayElementSchema;
            this.mapValueSchema = mapValueSchema;
            this.isDecimalType = isDecimalType;
        }
    }

    private static class FieldInfo {
        final int scale;
        final boolean isUnion;

        FieldInfo(int scale, boolean isUnion) {
            this.scale = scale;
            this.isUnion = isUnion;
        }
    }

    public static String fixDecimals(String json, Schema schema) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        processNode(root, schema);
        return objectMapper.writeValueAsString(root);
    }

    private static void processNode(JsonNode node, Schema schema) {
        if (schema == null || node == null) {
            return;
        }

        // Get cached schema info
        SchemaInfo schemaInfo = getOrComputeSchemaInfo(schema);

        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;

            // Process decimal fields first (most common case for your schema)
            for (Map.Entry<String, FieldInfo> entry : schemaInfo.decimalFields.entrySet()) {
                String fieldName = entry.getKey();
                FieldInfo fieldInfo = entry.getValue();

                JsonNode fieldValue = obj.get(fieldName);
                if (fieldValue != null && !fieldValue.isNull()) {
                    try {
                        processDecimalFieldOptimized(obj, fieldName, fieldValue, fieldInfo);
                    } catch (Exception e) {
                        System.err.println("Failed to process decimal field '" + fieldName + "': " + e.getMessage());
                    }
                }
            }

            // Process nested objects
            for (Map.Entry<String, Schema> entry : schemaInfo.recordFieldSchemas.entrySet()) {
                String fieldName = entry.getKey();
                Schema fieldSchema = entry.getValue();

                JsonNode fieldValue = obj.get(fieldName);
                if (fieldValue != null && !fieldValue.isNull()) {
                    processNode(fieldValue, fieldSchema);
                }
            }

            // Handle map types
            if (schemaInfo.mapValueSchema != null) {
                for (Iterator<Map.Entry<String, JsonNode>> it = obj.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> mapEntry = it.next();
                    if (!schemaInfo.decimalFields.containsKey(mapEntry.getKey()) &&
                            !schemaInfo.recordFieldSchemas.containsKey(mapEntry.getKey())) {
                        processNode(mapEntry.getValue(), schemaInfo.mapValueSchema);
                    }
                }
            }

        } else if (node.isArray() && schemaInfo.arrayElementSchema != null) {
            // Process array elements
            for (int i = 0; i < node.size(); i++) {
                processNode(node.get(i), schemaInfo.arrayElementSchema);
            }
        }
    }

    private static SchemaInfo getOrComputeSchemaInfo(Schema schema) {
        String schemaKey = schema.toString(); // Use schema string as key
        return schemaCache.computeIfAbsent(schemaKey, k -> analyzeSchema(schema));
    }

    private static SchemaInfo analyzeSchema(Schema schema) {
        List<Schema> possibleSchemas = getAllPossibleSchemas(schema);

        Map<String, FieldInfo> decimalFields = new HashMap<>();
        Map<String, Schema> recordFieldSchemas = new HashMap<>();
        Schema arrayElementSchema = null;
        Schema mapValueSchema = null;
        boolean isDecimalType = false;

        for (Schema s : possibleSchemas) {
            switch (s.getType()) {
                case RECORD:
                    // Analyze record fields
                    for (Schema.Field field : s.getFields()) {
                        String fieldName = field.name();
                        Schema fieldSchema = field.schema();

                        // Check if this field is a decimal
                        FieldInfo decimalInfo = analyzeDecimalField(fieldSchema);
                        if (decimalInfo != null) {
                            decimalFields.put(fieldName, decimalInfo);
                        } else {
                            // Store schema for nested processing
                            recordFieldSchemas.put(fieldName, fieldSchema);
                        }
                    }
                    break;

                case ARRAY:
                    arrayElementSchema = s.getElementType();
                    break;

                case MAP:
                    mapValueSchema = s.getValueType();
                    break;

                case BYTES:
                    LogicalType logicalType = s.getLogicalType();
                    if (logicalType != null && "decimal".equals(logicalType.getName())) {
                        isDecimalType = true;
                    }
                    break;

                default:
                    // Other primitive types - no special handling needed
                    break;
            }
        }

        return new SchemaInfo(decimalFields, recordFieldSchemas, arrayElementSchema, mapValueSchema, isDecimalType);
    }

    private static FieldInfo analyzeDecimalField(Schema fieldSchema) {
        List<Schema> possibleSchemas = getAllPossibleSchemas(fieldSchema);
        boolean isUnion = fieldSchema.getType() == Schema.Type.UNION;

        for (Schema s : possibleSchemas) {
            LogicalType logicalType = s.getLogicalType();
            if (logicalType != null && "decimal".equals(logicalType.getName())) {
                LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                return new FieldInfo(decimalType.getScale(), isUnion);
            }
        }
        return null;
    }

    private static void processDecimalFieldOptimized(ObjectNode obj, String key, JsonNode value, FieldInfo fieldInfo) {
        String bytesValue = null;

        // Handle different JSON representations of decimal bytes
        if (fieldInfo.isUnion && value.has("bytes")) {
            // Union type with bytes wrapper: {"bytes": "value"}
            bytesValue = value.get("bytes").asText();
        } else if (value.isTextual()) {
            // Direct type: just the string value
            bytesValue = value.asText();
        } else if (value.isBinary()) {
            // Direct bytes as binary node
            try {
                bytesValue = new String(value.binaryValue());
            } catch (Exception e) {
                System.err.println("Failed to read binary value: " + e.getMessage());
                return;
            }
        }

        if (bytesValue != null) {
            try {
                // Decode the bytes
                byte[] decoded = decodeBytes(bytesValue);

                // Convert to BigDecimal using cached scale
                BigDecimal decimal = new BigDecimal(new BigInteger(decoded), fieldInfo.scale);
                obj.put(key, decimal);
            } catch (Exception e) {
                System.err.println("Failed to convert decimal for field '" + key + "': " + e.getMessage());
            }
        }
    }

    private static List<Schema> getAllPossibleSchemas(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes();
        } else {
            return List.of(schema);
        }
    }

    private static byte[] decodeBytes(String bytesValue) {
        // Handle different encoding formats
        try {
            // Method 1: Try base64 decoding first
            return java.util.Base64.getDecoder().decode(bytesValue);
        } catch (Exception e1) {
            try {
                // Method 2: Handle raw binary data encoded as string
                byte[] result = new byte[bytesValue.length()];
                for (int i = 0; i < bytesValue.length(); i++) {
                    result[i] = (byte) bytesValue.charAt(i);
                }
                return result;
            } catch (Exception e2) {
                // Method 3: Fall back to ISO-8859-1 encoding
                try {
                    return bytesValue.getBytes("ISO-8859-1");
                } catch (Exception e3) {
                    // Method 4: Final fallback to UTF-8
                    return bytesValue.getBytes();
                }
            }
        }
    }

    // Method to clear cache if needed (useful for testing or memory management)
    public static void clearCache() {
        schemaCache.clear();
    }

    // Method to get cache statistics (useful for monitoring)
    public static int getCacheSize() {
        return schemaCache.size();
    }
}