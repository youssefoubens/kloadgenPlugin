/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  License, v. 2.0. If a copy of the MPL was not distributed with this
 *  file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.sngular.kloadgen.serializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Map;

/**
 * Serializer for GenericRecord producing Schema Registry compatible Avro binary.
 * This fixes "Invalid magic byte" errors when consuming with Avro deserializers.
 */
@Slf4j
public class GenericAvroRecordBinarySerializer<T extends GenericRecord> implements Serializer<T> {

  private KafkaAvroSerializer innerSerializer;

  public GenericAvroRecordBinarySerializer() {
    this.innerSerializer = new KafkaAvroSerializer();
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    innerSerializer.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, T data) {
    try {
      return innerSerializer.serialize(topic, data);
    } catch (Exception e) {
      log.error("Serialization error for data: {}", data, e);
      return null;
    }
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T data) {
    try {
      return innerSerializer.serialize(topic, headers, data);
    } catch (Exception e) {
      log.error("Serialization error for data: {}", data, e);
      return null;
    }
  }

  @Override
  public void close() {
    innerSerializer.close();
  }
}
