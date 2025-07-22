/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  * License, v. 2.0. If a copy of the MPL was not distributed with this
 *  * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.sngular.kloadgen.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class GenericAvroRecordSerializer<T extends GenericRecord> implements Serializer<T> {

  public GenericAvroRecordSerializer() {
    AvroSerializersUtil.setupLogicalTypesConversion();
  }

  @Override
  public final byte[] serialize(final String topic, final T data) {
    final DatumWriter<T> writer = new GenericDatumWriter<>(data.getSchema());
    try (var stream = new ByteArrayOutputStream()) {
      var jsonEncoder = EncoderFactory.get().jsonEncoder(data.getSchema(), stream);
      writer.write(data, jsonEncoder);
      jsonEncoder.flush();
      String rawJson = stream.toString(StandardCharsets.UTF_8);
      String fixedJson = AvroJsonDecimalFixer3.fixDecimals(rawJson, data.getSchema());
      return fixedJson.getBytes(StandardCharsets.UTF_8);
    } catch (final IOException ex) {
      log.error("Serialization error:" + ex.getMessage(), ex);
      return new byte[0];
    } catch (final Exception ex) {
      log.error("Decimal fixing error:" + ex.getMessage(), ex);
      return new byte[0];
    }
  }

  @Override
  public final byte[] serialize(final String topic, final Headers headers, final T data) {
    return serialize(topic, data);
  }
}