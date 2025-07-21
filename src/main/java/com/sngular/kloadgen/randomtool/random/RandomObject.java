/*
 *  This Source Code Form is subject to the terms of the Mozilla Public
 *  * License, v. 2.0. If a copy of the MPL was not distributed with this
 *  * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.sngular.kloadgen.randomtool.random;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.logging.Logger;


import com.github.curiousoddman.rgxgen.RgxGen;
import com.sngular.kloadgen.exception.KLoadGenException;
import com.sngular.kloadgen.model.ConstraintTypeEnum;
import com.sngular.kloadgen.randomtool.util.ValidTypeConstants;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

public final class RandomObject {

  private final Random rand = new Random();
  private static final Logger logger = Logger.getLogger(RandomObject.class.getName());

  public boolean isTypeValid(final String type) {
    return ValidTypeConstants.VALID_OBJECT_TYPES.contains(type);
  }

  public Object generateRandom(
          final String fieldType, final Integer valueLength, final List<String> fieldValueList,
          final Map<ConstraintTypeEnum, String> constraints) {

    final String fixFieldType = StringUtils.defaultString(fieldType, "string");

    // DEBUG: Log input parameters
    logger.info(String.format("generateRandom called with: fieldType=%s, valueLength=%s, fieldValueList=%s, constraints=%s",
            fixFieldType, valueLength, fieldValueList, constraints));

    Object result = switch (fixFieldType.toLowerCase()) {
      case ValidTypeConstants.STRING -> getStringValueOrRandom(valueLength, fieldValueList, constraints);
      case ValidTypeConstants.INT -> {
        try {
          BigInteger bigInt = getIntegerValueOrRandom(valueLength, fieldValueList, constraints);
          logger.info("Generated BigInteger for INT: " + bigInt);
          yield bigInt.intValueExact();
        } catch (final ArithmeticException exception) {
          logger.warning("ArithmeticException in INT generation, returning MAX_VALUE");
          yield Integer.MAX_VALUE;
        }
      }
      case ValidTypeConstants.LONG -> {
        try {
          BigInteger bigInt = getIntegerValueOrRandom(valueLength, fieldValueList, constraints);
          logger.info("Generated BigInteger for LONG: " + bigInt);
          yield bigInt.longValueExact();
        } catch (final ArithmeticException exception) {
          logger.warning("ArithmeticException in LONG generation, returning MAX_VALUE");
          yield Long.MAX_VALUE;
        }
      }
      case ValidTypeConstants.SHORT -> {
        try {
          BigInteger bigInt = getIntegerValueOrRandom(valueLength, fieldValueList, constraints);
          logger.info("Generated BigInteger for SHORT: " + bigInt);
          yield bigInt.shortValueExact();
        } catch (final ArithmeticException exception) {
          logger.warning("ArithmeticException in SHORT generation, returning MAX_VALUE");
          yield Short.MAX_VALUE;
        }
      }
      case ValidTypeConstants.DOUBLE -> {
        try {
          BigDecimal bigDecimal = getDecimalValueOrRandom(valueLength, fieldValueList, constraints);
          logger.info("Generated BigDecimal for DOUBLE: " + bigDecimal);
          yield bigDecimal.doubleValue();
        } catch (final ArithmeticException exception) {
          logger.warning("ArithmeticException in DOUBLE generation, returning MAX_VALUE");
          yield Double.MAX_VALUE;
        }
      }
      case ValidTypeConstants.NUMBER, ValidTypeConstants.FLOAT -> {
        try {
          BigDecimal bigDecimal = getDecimalValueOrRandom(valueLength, fieldValueList, constraints);
          logger.info("Generated BigDecimal for FLOAT/NUMBER: " + bigDecimal);
          yield bigDecimal.floatValue();
        } catch (final ArithmeticException exception) {
          logger.warning("ArithmeticException in FLOAT/NUMBER generation, returning MAX_VALUE");
          yield Float.MAX_VALUE;
        }
      }
      case ValidTypeConstants.BYTES -> getBytesValueOrRandom(valueLength, fieldValueList);
      case ValidTypeConstants.TIMESTAMP, ValidTypeConstants.LONG_TIMESTAMP, ValidTypeConstants.STRING_TIMESTAMP -> getTimestampValueOrRandom(fieldType, fieldValueList);
      case ValidTypeConstants.BOOLEAN -> getBooleanValueOrRandom(fieldValueList);
      case ValidTypeConstants.ENUM -> getEnumValueOrRandom(fieldValueList);
      case ValidTypeConstants.INT_DATE -> getDateValueOrRandom(fieldValueList);
      case ValidTypeConstants.INT_TIME_MILLIS -> getTimeMillisValueOrRandom(fieldValueList);
      case ValidTypeConstants.LONG_TIME_MICROS -> getTimeMicrosValueOrRandom(fieldValueList);
      case ValidTypeConstants.LONG_TIMESTAMP_MILLIS -> getTimestampMillisValueOrRandom(fieldValueList);
      case ValidTypeConstants.LONG_TIMESTAMP_MICROS -> getTimestampMicrosValueOrRandom(fieldValueList);
      case ValidTypeConstants.LONG_LOCAL_TIMESTAMP_MILLIS -> getLocalTimestampMillisValueOrRandom(fieldValueList);
      case ValidTypeConstants.LONG_LOCAL_TIMESTAMP_MICROS -> getLocalTimestampMicrosValueOrRandom(fieldValueList);
      case ValidTypeConstants.UUID, ValidTypeConstants.STRING_UUID -> getUUIDValueOrRandom(fieldValueList);
      case ValidTypeConstants.BYTES_DECIMAL -> getBytesDecimalValueOrRandom(fieldValueList, constraints);
      case ValidTypeConstants.FIXED_DECIMAL -> getFixedDecimalValueOrRandom(fieldValueList, constraints);
      case ValidTypeConstants.INT_YEAR, ValidTypeConstants.INT_MONTH, ValidTypeConstants.INT_DAY -> getDateValueOrRandom(fieldType, fieldValueList);
      case ValidTypeConstants.INT_HOURS, ValidTypeConstants.INT_MINUTES, ValidTypeConstants.INT_SECONDS, ValidTypeConstants.INT_NANOS -> getTimeOfDayValueOrRandom(fieldType, fieldValueList);
      default -> {
        logger.warning("Unknown field type: " + fixFieldType + ", returning as-is");
        yield fieldType;
      }
    };

    // DEBUG: Log the result and its type
    logger.info(String.format("generateRandom returning: value=%s, type=%s, class=%s",
            result, fixFieldType, result != null ? result.getClass().getName() : "null"));

    return result;
  }

  private String getBytesValueOrRandom(final Integer valueLength, final List<String> fieldValueList) {
    logger.info("getBytesValueOrRandom called with valueLength=" + valueLength + ", fieldValueList=" + fieldValueList);

    if (!fieldValueList.isEmpty()) {
      String result = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      logger.info("Returning bytes value from list: " + result);
      return result;
    }

    // Generate Base64-encoded byte array for better JSON serialization
    int length = valueLength != null && valueLength > 0 ? valueLength : 8; // default length of 8 bytes
    byte[] byteArray = new byte[length];
    rand.nextBytes(byteArray);
    String result = java.util.Base64.getEncoder().encodeToString(byteArray);
    logger.info("Generated random bytes (Base64): " + result);
    return result;
  }

  private BigInteger getIntegerValueOrRandom(final Integer valueLength, final List<String> fieldValueList, final Map<ConstraintTypeEnum, String> constraints) {
    logger.info("getIntegerValueOrRandom called with valueLength=" + valueLength + ", fieldValueList=" + fieldValueList + ", constraints=" + constraints);

    final BigInteger value;

    if (!fieldValueList.isEmpty()) {
      String stringValue = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      value = new BigInteger(stringValue);
      logger.info("Parsed integer from field list: " + stringValue + " -> " + value);
    } else {
      final Number minimum = calculateMinimum(valueLength, constraints);
      Number maximum;
      if (valueLength == 0) {
        maximum = 1000;
        final int num = rand.nextInt((Integer) maximum);
        value = new BigInteger(String.valueOf(num));
        logger.info("Generated random integer (valueLength=0): " + value);
      } else {
        maximum = calculateMaximum(valueLength, constraints);

        if (constraints.containsKey(ConstraintTypeEnum.MULTIPLE_OF)) {
          final int multipleOf = Integer.parseInt(constraints.get(ConstraintTypeEnum.MULTIPLE_OF));
          maximum = maximum.intValue() > multipleOf ? maximum.intValue() / multipleOf : maximum;
          value = BigInteger.valueOf(RandomUtils.nextLong(minimum.longValue(), maximum.longValue()) * multipleOf);
          logger.info("Generated integer with multipleOf constraint: " + value);
        } else {
          value = BigInteger.valueOf(RandomUtils.nextLong(minimum.longValue(), maximum.longValue()));
          logger.info("Generated random integer in range: " + value);
        }
      }
    }
    return value;
  }

  private BigDecimal getDecimalValueOrRandom(final Integer valueLength, final List<String> fieldValueList, final Map<ConstraintTypeEnum, String> constraints) {
    logger.info("getDecimalValueOrRandom called with valueLength=" + valueLength + ", fieldValueList=" + fieldValueList + ", constraints=" + constraints);

    final BigDecimal value;

    if (!fieldValueList.isEmpty()) {
      String stringValue = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      value = new BigDecimal(stringValue);
      logger.info("Parsed decimal from field list: " + stringValue + " -> " + value);
    } else {
      final Number minimum = calculateMinimum(valueLength - 1, constraints);

      Number maximum;
      maximum = calculateMaximum(valueLength - 1, constraints);

      if (constraints.containsKey(ConstraintTypeEnum.MULTIPLE_OF)) {
        final int multipleOf = Integer.parseInt(constraints.get(ConstraintTypeEnum.MULTIPLE_OF));
        maximum = maximum.intValue() > multipleOf ? maximum.intValue() / multipleOf : maximum;
        value = BigDecimal.valueOf(RandomUtils.nextDouble(minimum.doubleValue(), maximum.doubleValue()) * multipleOf);
        logger.info("Generated decimal with multipleOf constraint: " + value);
      } else {
        value = new BigDecimal(BigInteger.valueOf(new Random().nextInt(100001)), 2);
        logger.info("Generated random decimal: " + value);
      }
    }
    return value;
  }

  private BigDecimal getBytesDecimalValueOrRandom(
          final List<String> fieldValueList,
          final Map<ConstraintTypeEnum, String> constraints) {

    logger.info("*** BYTES_DECIMAL DEBUG START ***");
    logger.info("getBytesDecimalValueOrRandom called with fieldValueList=" + fieldValueList + ", constraints=" + constraints);

    final BigDecimal result;

    if (!fieldValueList.isEmpty()) {
      // Parse decimal value directly from field values (e.g., "2.25" -> 2.25)
      String decimalStr = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      logger.info("Using decimal from field list: '" + decimalStr + "'");

      try {
        result = new BigDecimal(decimalStr);
        logger.info("Successfully parsed BigDecimal: " + result + " (scale=" + result.scale() + ", precision=" + result.precision() + ")");
      } catch (NumberFormatException e) {
        logger.severe("Failed to parse decimal string '" + decimalStr + "': " + e.getMessage());
        throw e;
      }
    } else {
      logger.info("Field list is empty, generating random bytes_decimal");

      // Generate random decimal with constraints
      int scale = 10; // default scale for bytes_decimal
      int precision = 20; // default precision

      // Try to get from constraints
      if (Objects.nonNull(constraints.get(ConstraintTypeEnum.PRECISION))) {
        precision = Integer.parseInt(constraints.get(ConstraintTypeEnum.PRECISION));
        logger.info("Got precision from constraints: " + precision);
        if (Objects.nonNull(constraints.get(ConstraintTypeEnum.SCALE))) {
          scale = Integer.parseInt(constraints.get(ConstraintTypeEnum.SCALE));
          logger.info("Got scale from constraints: " + scale);
        }
      } else {
        logger.info("No precision in constraints, using defaults: precision=" + precision + ", scale=" + scale);
      }

      if (precision <= 0) {
        precision = 10;
        logger.warning("Invalid precision, reset to: " + precision);
      }
      if (scale < 0 || scale > precision) {
        scale = Math.min(2, precision);
        logger.warning("Invalid scale, reset to: " + scale);
      }

      result = BigDecimal.valueOf(randomNumberWithLength(precision), scale);
      logger.info("Generated random BigDecimal: " + result + " (scale=" + result.scale() + ", precision=" + result.precision() + ")");
    }

    logger.info("*** BYTES_DECIMAL RETURNING: " + result + " (class=" + result.getClass().getName() + ") ***");
    logger.info("*** BYTES_DECIMAL DEBUG END ***");

    return result;
  }

  private BigDecimal getFixedDecimalValueOrRandom(
          final List<String> fieldValueList,
          final Map<ConstraintTypeEnum, String> constraints) {

    logger.info("*** FIXED_DECIMAL DEBUG START ***");
    logger.info("getFixedDecimalValueOrRandom called with fieldValueList=" + fieldValueList + ", constraints=" + constraints);

    final BigDecimal result;
    int scale = 10; // reasonable default
    int precision = 20; // reasonable default

    // Try to get from constraints first
    if (Objects.nonNull(constraints.get(ConstraintTypeEnum.PRECISION))) {
      precision = Integer.parseInt(constraints.get(ConstraintTypeEnum.PRECISION));
      logger.info("Got precision from constraints: " + precision);
      if (Objects.nonNull(constraints.get(ConstraintTypeEnum.SCALE))) {
        scale = Integer.parseInt(constraints.get(ConstraintTypeEnum.SCALE));
        logger.info("Got scale from constraints: " + scale);
      }
    } else {
      // If no precision in constraints, this might be a union type issue
      logger.warning("No decimal precision found in constraints, using defaults: precision=" + precision + ", scale=" + scale);
    }

    if (precision <= 0) {
      precision = 10; // fallback instead of throwing exception
      logger.warning("Invalid precision, reset to: " + precision);
    }
    if (scale < 0 || scale > precision) {
      scale = Math.min(2, precision); // fallback instead of throwing exception
      logger.warning("Invalid scale, reset to: " + scale);
    }

    if (fieldValueList.isEmpty()) {
      result = BigDecimal.valueOf(randomNumberWithLength(precision), scale);
      logger.info("Generated random BigDecimal: " + result);
    } else {
      String decimalStr = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      logger.info("Using decimal from field list: '" + decimalStr + "'");
      result = new BigDecimal(decimalStr);
    }

    logger.info("*** FIXED_DECIMAL RETURNING: " + result + " (class=" + result.getClass().getName() + ") ***");
    logger.info("*** FIXED_DECIMAL DEBUG END ***");

    return result;
  }

  private String getStringValueOrRandom(
          final Integer valueLength, final List<String> fieldValueList,
          final Map<ConstraintTypeEnum, String> constraints) {
    logger.info("getStringValueOrRandom called with valueLength=" + valueLength + ", fieldValueList=" + fieldValueList + ", constraints=" + constraints);

    String value;
    if (!fieldValueList.isEmpty() && !StringUtils.isEmpty(fieldValueList.get(0))) {
      value = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      logger.info("Using string from field list: '" + value + "'");
    } else {
      if (constraints.containsKey(ConstraintTypeEnum.REGEX)) {
        final RgxGen rxGenerator = new RgxGen(constraints.get(ConstraintTypeEnum.REGEX));
        value = rxGenerator.generate();
        if (valueLength > 0 || constraints.containsKey(ConstraintTypeEnum.MAXIMUM_VALUE)) {
          value = value.substring(0, getMaxLength(valueLength, constraints.get(ConstraintTypeEnum.MAXIMUM_VALUE)));
        }
        logger.info("Generated string from regex: '" + value + "'");
      } else {
        value = RandomStringUtils.randomAlphabetic(valueLength == 0 ? RandomUtils.nextInt(1, 20) : valueLength);
        logger.info("Generated random alphabetic string: '" + value + "'");
      }
    }
    return value;
  }

  private int getMaxLength(final Integer valueLength, final String maxValueStr) {
    int maxValue = Integer.parseInt(StringUtils.defaultIfEmpty(maxValueStr, "0"));
    if (valueLength > 0 && maxValue == 0) {
      maxValue = valueLength;
    }
    logger.info("Calculated max length: " + maxValue);
    return maxValue;
  }

  private Object getTimestampValueOrRandom(final String type, final List<String> fieldValueList) {
    logger.info("getTimestampValueOrRandom called with type=" + type + ", fieldValueList=" + fieldValueList);

    final LocalDateTime value;
    if (!fieldValueList.isEmpty()) {
      String dateTimeStr = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      value = LocalDateTime.parse(dateTimeStr);
      logger.info("Parsed timestamp from field list: " + dateTimeStr + " -> " + value);
    } else {
      value = LocalDateTime.now();
      logger.info("Generated current timestamp: " + value);
    }
    Object resultValue = value;
    if ("longTimestamp".equalsIgnoreCase(type)) {
      resultValue = value.toInstant(ZoneOffset.UTC).toEpochMilli();
      logger.info("Converted to long timestamp: " + resultValue);
    } else if ("stringTimestamp".equalsIgnoreCase(type)) {
      resultValue = value.toString();
      logger.info("Converted to string timestamp: " + resultValue);
    }
    return resultValue;
  }

  @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
  private UUID getUUIDValueOrRandom(final List<String> fieldValueList) {
    logger.info("getUUIDValueOrRandom called with fieldValueList=" + fieldValueList);

    UUID value = UUID.randomUUID();
    if (!fieldValueList.isEmpty()) {
      String uuidStr = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      value = UUID.fromString(uuidStr);
      logger.info("Parsed UUID from field list: " + uuidStr + " -> " + value);
    } else {
      logger.info("Generated random UUID: " + value);
    }
    return value;
  }

  private Boolean getBooleanValueOrRandom(final List<String> fieldValueList) {
    logger.info("getBooleanValueOrRandom called with fieldValueList=" + fieldValueList);

    boolean value = RandomUtils.nextBoolean();
    if (!fieldValueList.isEmpty()) {
      String boolStr = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      value = Boolean.parseBoolean(boolStr);
      logger.info("Parsed boolean from field list: " + boolStr + " -> " + value);
    } else {
      logger.info("Generated random boolean: " + value);
    }
    return value;
  }

  private String getEnumValueOrRandom(final List<String> fieldValueList) {
    logger.info("getEnumValueOrRandom called with fieldValueList=" + fieldValueList);

    final String value;
    if (!fieldValueList.isEmpty()) {
      value = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      logger.info("Selected enum value: " + value);
    } else {
      logger.severe("Empty enum field value list!");
      throw new KLoadGenException("Wrong enums values, problem in the parsing process");
    }
    return value;
  }

  private Number calculateMaximum(final int valueLength, final Map<ConstraintTypeEnum, String> constraints) {
    final Number maximum;
    if (constraints.containsKey(ConstraintTypeEnum.MAXIMUM_VALUE)) {
      if (constraints.containsKey(ConstraintTypeEnum.EXCLUDED_MAXIMUM_VALUE)) {
        maximum = Long.parseLong(constraints.get(ConstraintTypeEnum.EXCLUDED_MAXIMUM_VALUE)) - 1L;
        logger.info("Calculated excluded maximum: " + maximum);
      } else {
        maximum = Long.parseLong(constraints.get(ConstraintTypeEnum.MAXIMUM_VALUE));
        logger.info("Using maximum from constraints: " + maximum);
      }
    } else {
      maximum = new BigDecimal(StringUtils.rightPad("9", valueLength, '0'));
      logger.info("Calculated maximum from valueLength: " + maximum);
    }
    return maximum;
  }

  private Number calculateMinimum(final int valueLength, final Map<ConstraintTypeEnum, String> constraints) {
    final Number minimum;
    if (constraints.containsKey(ConstraintTypeEnum.MINIMUM_VALUE)) {
      if (constraints.containsKey(ConstraintTypeEnum.EXCLUDED_MINIMUM_VALUE)) {
        minimum = Long.parseLong(constraints.get(ConstraintTypeEnum.EXCLUDED_MINIMUM_VALUE)) - 1;
        logger.info("Calculated excluded minimum: " + minimum);
      } else {
        minimum = Long.parseLong(constraints.get(ConstraintTypeEnum.MINIMUM_VALUE));
        logger.info("Using minimum from constraints: " + minimum);
      }
    } else {
      minimum = Long.parseLong(StringUtils.rightPad("1", valueLength, '0'));
      logger.info("Calculated minimum from valueLength: " + minimum);
    }
    return minimum;
  }

  private Integer getDateValueOrRandom(final String fieldType, final List<String> fieldValueList) {
    logger.info("getDateValueOrRandom called with fieldType=" + fieldType + ", fieldValueList=" + fieldValueList);

    final LocalDate localDate = getDateValueOrRandom(fieldValueList);
    final int result;

    if ("int_year".equalsIgnoreCase(fieldType)) {
      result = localDate.getYear();
      logger.info("Extracted year: " + result);
    } else if ("int_month".equalsIgnoreCase(fieldType)) {
      result = localDate.getMonthValue();
      logger.info("Extracted month: " + result);
    } else if ("int_day".equalsIgnoreCase(fieldType)) {
      result = localDate.getDayOfMonth();
      logger.info("Extracted day: " + result);
    } else {
      logger.severe("Unsupported date field type: " + fieldType);
      throw new KLoadGenException("FieldType wrong or not supported");
    }
    return result;
  }

  private static LocalDate getDateValueOrRandom(final List<String> fieldValueList) {
    final LocalDate resultDate;
    final long minDay = (int) LocalDate.of(1900, 1, 1).toEpochDay();
    final long maxDay = (int) LocalDate.of(2100, 1, 1).toEpochDay();
    final long randomDay = minDay + RandomUtils.nextLong(0, maxDay - minDay);
    if (fieldValueList.isEmpty()) {
      resultDate = LocalDate.ofEpochDay(randomDay);
    } else {
      String dateStr = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      resultDate = LocalDate.parse(dateStr);
    }
    return resultDate;
  }

  private Integer getTimeOfDayValueOrRandom(final String fieldType, final List<String> fieldValueList) {
    logger.info("getTimeOfDayValueOrRandom called with fieldType=" + fieldType + ", fieldValueList=" + fieldValueList);

    final LocalTime localTime = getRandomLocalTime(fieldValueList);
    final int result;

    if ("int_hours".equalsIgnoreCase(fieldType)) {
      result = localTime.getHour();
      logger.info("Extracted hours: " + result);
    } else if ("int_minutes".equalsIgnoreCase(fieldType)) {
      result = localTime.getMinute();
      logger.info("Extracted minutes: " + result);
    } else if ("int_seconds".equalsIgnoreCase(fieldType)) {
      result = localTime.getSecond();
      logger.info("Extracted seconds: " + result);
    } else if ("int_nanos".equalsIgnoreCase(fieldType)) {
      result = localTime.getNano();
      logger.info("Extracted nanos: " + result);
    } else {
      logger.severe("Unsupported time field type: " + fieldType);
      throw new KLoadGenException("FieldType wrong or not supported");
    }
    return result;
  }

  private static LocalTime getRandomLocalTime(final List<String> fieldValueList) {
    final long nanoMin = 0;
    final long nanoMax = 24L * 60L * 60L * 1_000_000_000L - 1L;
    final LocalTime result;
    if (fieldValueList.isEmpty()) {
      result = LocalTime.ofNanoOfDay(RandomUtils.nextLong(nanoMin, nanoMax));
    } else {
      result = getLocalTime(fieldValueList);
    }
    return result;
  }

  private static LocalTime getLocalTime(final List<String> fieldValueList) {
    final String fieldValue = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
    final Pattern pattern = Pattern.compile("([+|-]\\d{2}:\\d{2})");
    final Matcher matcher = pattern.matcher(fieldValue);
    final LocalTime result;
    if (matcher.find()) {
      final String offSet = matcher.group(1);
      final DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_TIME;
      final LocalTime localtime = LocalTime.parse(fieldValue, formatter);

      final int hours = Integer.parseInt(offSet.substring(2, 3));
      final int minutes = Integer.parseInt(offSet.substring(5, 6));

      if (offSet.startsWith("-")) {
        result = localtime.minusHours(hours).minusMinutes(minutes);
      } else {
        result = localtime.plusHours(hours).plusMinutes(minutes);
      }
    } else {
      result = LocalTime.parse(fieldValue);
    }
    return result;
  }

  private static LocalTime getTimeMillisValueOrRandom(final List<String> fieldValueList) {
    return getRandomLocalTime(fieldValueList);
  }

  private static LocalTime getTimeMicrosValueOrRandom(final List<String> fieldValueList) {
    return getRandomLocalTime(fieldValueList);
  }

  private static LocalDateTime getRandomLocalDateTime(final List<String> fieldValueList) {
    final LocalDateTime value;
    final long minDay = LocalDateTime.of(1900, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC);
    final long maxDay = LocalDateTime.of(2100, 1, 1, 0, 0).toEpochSecond(ZoneOffset.UTC);
    final long randomSeconds = minDay + RandomUtils.nextLong(0, maxDay - minDay);

    if (fieldValueList.isEmpty()) {
      value = LocalDateTime.ofEpochSecond(randomSeconds, RandomUtils.nextInt(0, 1_000_000_000 - 1), ZoneOffset.UTC);
    } else {
      String dateTimeStr = fieldValueList.get(RandomUtils.nextInt(0, fieldValueList.size())).trim();
      value = LocalDateTime.parse(dateTimeStr);
    }
    return value;
  }

  private static Instant getTimestampMillisValueOrRandom(final List<String> fieldValueList) {
    return getRandomLocalDateTime(fieldValueList).toInstant(ZoneOffset.UTC);
  }

  private static Instant getTimestampMicrosValueOrRandom(final List<String> fieldValueList) {
    return getRandomLocalDateTime(fieldValueList).toInstant(ZoneOffset.UTC);
  }

  private static LocalDateTime getLocalTimestampMillisValueOrRandom(final List<String> fieldValueList) {
    return getRandomLocalDateTime(fieldValueList);
  }

  private static LocalDateTime getLocalTimestampMicrosValueOrRandom(final List<String> fieldValueList) {
    return getRandomLocalDateTime(fieldValueList);
  }

  private static long randomNumberWithLength(final int n) {
    final long min = (long) Math.pow(10, n - 1.0);
    return RandomUtils.nextLong(min, min * 10);
  }

}