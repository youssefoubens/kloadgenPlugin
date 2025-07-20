package com.sngular.kloadgen.serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AvroJsonDecimalFixer2 {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String fixDecimals(String json, Schema schema) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        processNode(root, schema);
        return objectMapper.writeValueAsString(root);
    }

    private static void processNode(JsonNode node, Schema schema) {
        // Handle null schema or node
        if (schema == null || node == null) {
            return;
        }

        // Get all possible schemas (handles unions and single types)
        List<Schema> possibleSchemas = getAllPossibleSchemas(schema);

        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;

            // Try to find a matching record schema
            Schema recordSchema = findRecordSchema(possibleSchemas);

            if (recordSchema != null) {
                // Process as record
                for (Iterator<Map.Entry<String, JsonNode>> it = obj.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    String key = entry.getKey();
                    JsonNode value = entry.getValue();

                    Schema.Field field = recordSchema.getField(key);
                    if (field == null) continue;

                    Schema fieldSchema = field.schema();

                    // Check if this field contains a decimal and process accordingly
                    if (isDecimalField(fieldSchema)) {
                        try {
                            if (processDecimalField(obj, key, value, fieldSchema)) {
                                continue; // Skip further processing if decimal was processed
                            }
                        } catch (Exception e) {
                            System.err.println("Failed to process decimal field '" + key + "': " + e.getMessage());
                        }
                    }

                    // Recursively process nested objects/arrays
                    processNode(value, fieldSchema);
                }
            } else {
                // Try to find a map schema
                Schema mapSchema = findMapSchema(possibleSchemas);
                if (mapSchema != null) {
                    Schema valueSchema = mapSchema.getValueType();
                    for (Iterator<Map.Entry<String, JsonNode>> it = obj.fields(); it.hasNext(); ) {
                        Map.Entry<String, JsonNode> entry = it.next();
                        processNode(entry.getValue(), valueSchema);
                    }
                }
            }
        } else if (node.isArray()) {
            // Try to find an array schema
            Schema arraySchema = findArraySchema(possibleSchemas);
            if (arraySchema != null) {
                Schema elementSchema = arraySchema.getElementType();
                for (int i = 0; i < node.size(); i++) {
                    processNode(node.get(i), elementSchema);
                }
            }
        }
        // For primitive types, do nothing
    }

    private static List<Schema> getAllPossibleSchemas(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes();
        } else {
            return List.of(schema);
        }
    }

    private static Schema findRecordSchema(List<Schema> schemas) {
        return schemas.stream()
                .filter(s -> s.getType() == Schema.Type.RECORD)
                .findFirst()
                .orElse(null);
    }

    private static Schema findArraySchema(List<Schema> schemas) {
        return schemas.stream()
                .filter(s -> s.getType() == Schema.Type.ARRAY)
                .findFirst()
                .orElse(null);
    }

    private static Schema findMapSchema(List<Schema> schemas) {
        return schemas.stream()
                .filter(s -> s.getType() == Schema.Type.MAP)
                .findFirst()
                .orElse(null);
    }

    private static boolean isDecimalField(Schema schema) {
        // Check all possible schemas in case of union
        List<Schema> possibleSchemas = getAllPossibleSchemas(schema);

        for (Schema s : possibleSchemas) {
            LogicalType logicalType = s.getLogicalType();
            if (logicalType != null && "decimal".equals(logicalType.getName())) {
                return true;
            }
        }
        return false;
    }

    private static boolean processDecimalField(ObjectNode obj, String key, JsonNode value, Schema fieldSchema) {
        // Find the decimal schema from all possible schemas
        List<Schema> possibleSchemas = getAllPossibleSchemas(fieldSchema);

        for (Schema s : possibleSchemas) {
            LogicalType logicalType = s.getLogicalType();
            if (logicalType != null && "decimal".equals(logicalType.getName())) {

                String bytesValue = null;

                // Handle different JSON representations of decimal bytes:
                // Case 1: Union type with bytes wrapper: {"bytes": "value"}
                if (value.has("bytes")) {
                    bytesValue = value.get("bytes").asText();
                }
                // Case 2: Direct type: just the string value
                else if (value.isTextual()) {
                    bytesValue = value.asText();
                }
                // Case 3: Direct bytes as binary node
                else if (value.isBinary()) {
                    try {
                        bytesValue = new String(value.binaryValue());
                    } catch (Exception e) {
                        System.err.println("Failed to read binary value: " + e.getMessage());
                        continue;
                    }
                }

                if (bytesValue != null) {
                    // Decode the bytes
                    byte[] decoded = decodeBytes(bytesValue);

                    // Get scale from the decimal logical type
                    LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                    int scale = decimalType.getScale();

                    // Convert to BigDecimal
                    BigDecimal decimal = new BigDecimal(new BigInteger(decoded), scale);
                    obj.put(key, decimal);
                    return true; // Successfully processed decimal
                }
            }
        }
        return false; // Not processed as decimal
    }

    private static byte[] decodeBytes(String bytesValue) {
        // Handle different encoding formats
        try {
            // Method 1: Try base64 decoding first
            return java.util.Base64.getDecoder().decode(bytesValue);
        } catch (Exception e1) {
            try {
                // Method 2: Handle raw binary data encoded as string
                // For direct decimal types, bytes are often stored as raw binary in the string
                // We need to convert each character to its byte value
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
}