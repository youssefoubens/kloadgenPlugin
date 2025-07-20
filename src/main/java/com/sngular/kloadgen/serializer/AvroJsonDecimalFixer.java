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
import java.util.Map;

public class AvroJsonDecimalFixer {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String fixDecimals(String json, Schema schema) throws Exception {
        JsonNode root = objectMapper.readTree(json);
        processNode(root, schema);
        return objectMapper.writeValueAsString(root);
    }

    private static void processNode(JsonNode node, Schema schema) {
        if (node.isObject()) {
            ObjectNode obj = (ObjectNode) node;
            for (Iterator<Map.Entry<String, JsonNode>> it = obj.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                String key = entry.getKey();
                JsonNode value = entry.getValue();

                Schema.Field field = schema.getField(key);
                if (field == null) continue;

                Schema effectiveSchema = unwrapUnion(field.schema());
                LogicalType logicalType = effectiveSchema.getLogicalType();

                // Check if it's a decimal logical type
                if (logicalType != null && "decimal".equals(logicalType.getName()) && value.has("bytes")) {
                    String escaped = value.get("bytes").asText();

                    // Decode the bytes (you may need to adjust this based on your encoding)
                    byte[] decoded = escaped.getBytes(); // This might need proper base64 decoding

                    // Get scale from the decimal logical type
                    LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                    int scale = decimalType.getScale();

                    // Convert to BigDecimal
                    BigDecimal decimal = new BigDecimal(new BigInteger(decoded), scale);
                    obj.put(key, decimal);
                } else if (value.isObject() || value.isArray()) {
                    processNode(value, effectiveSchema);
                }
            }
        } else if (node.isArray() && schema.getType() == Schema.Type.ARRAY) {
            for (int i = 0; i < node.size(); i++) {
                processNode(node.get(i), schema.getElementType());
            }
        }
    }

    private static Schema unwrapUnion(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) return s;
            }
        }
        return schema;
    }
}