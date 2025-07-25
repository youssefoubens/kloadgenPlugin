

<p align="center">
<a href="#-summary">Summary</a> · 
<a href="#-getting-started">Getting started</a> · 
<a href="#-usage">Usage</a> · 
<a href="#-technical-design">Technical design</a> · 
<a href="#-support">Support</a> · 
<a href="#-special-thanks">Special thanks</a> 
</p> 

## 📜 Summary

KLoadGen is a Kafka load generator plugin for JMeter designed to work with AVRO, JSON Schema, and PROTOBUF structures for sending Kafka messages. It connects to the Schema Registry server, retrieves the subject to send, and generates a random message every time.

**Maintainer**: Youssef Ouben Said  
**Compatible with**: JMeter 5.6.2



## 🚀 Getting Started

### Prerequisites
- Java 8 or higher
- Apache JMeter 5.6.2
- Kafka cluster (optional for local testing)
- Schema Registry (for AVRO/PROTOBUF testing)

### Quick Installation

#### Option 1: JMeter Plugin Manager
1. Launch JMeter 5.6.2
2. Go to Options > Plugins Manager
3. Search for "KLoadGen"
4. Install and restart JMeter

#### Option 2: Manual Installation
1. Download the latest JAR from [Maven Central](https://mvnrepository.com/artifact/com.sngular/kloadgen)
2. Copy to `JMETER_HOME/lib/ext` directory
3. Restart JMeter

## 🧑🏻‍💻 Usage

### Basic Configuration

#### Producer Setup
```properties
kafka.bootstrap.servers=localhost:9092
schema.registry.url=http://localhost:8081
key.serializer=org.apache.kafka.common.serialization.StringSerializer
value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer