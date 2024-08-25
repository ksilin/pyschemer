import requests
import copy
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import (
    DeserializingConsumer,
    SerializingProducer,
)
from confluent_kafka.admin import AdminClient
from confluent_kafka.error import ValueSerializationError, KafkaException
from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
    SchemaRegistryError,
)
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer
from confluent_kafka import DeserializingConsumer, KafkaError, Message

def delete_kafka_topics(admin_client, topics):
    # Delete topics
    fs = admin_client.delete_topics(topics, operation_timeout=30)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} deleted")
        except Exception as e:
            print(f"Failed to delete topic {topic}: {e}")


def create_kafka_topics(admin_client, topics):
    # Create topics
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]
    fs = admin_client.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

def clear_schema_registry_subjects(schema_registry_conf, subjects):
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    for subject in subjects:
        try:
            deleted_versions = schema_registry_client.delete_subject(subject_name=subject, permanent=True)
            print(f"Deleted following schema versions for subject {subject}: {deleted_versions}")
        except SchemaRegistryError as e:
            if e.error_code == 40401:  # Schema Registry code for subject not found
                print(f"Subject '{subject}' not found. Skipping deletion.")
            else:
                print(f"Failed to delete subject '{subject}': {e}")

def create_test_producer(producer_conf, schema_registry_conf, schema_string, serializer_class, to_dict=None, serializer_conf=None):
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    value_serializer = serializer_class(schema_str=schema_string, schema_registry_client=schema_registry_client, to_dict=to_dict, conf=serializer_conf)
    producer_config = copy.deepcopy(producer_conf)
    producer_config['key.serializer'] = StringSerializer('utf_8')
    producer_config['value.serializer'] = value_serializer

    return SerializingProducer(producer_config)

def create_test_producer_json_sr(producer_conf, schema_registry_conf, schema_string, to_dict=None, serializer_conf=None):
    return create_test_producer(producer_conf, schema_registry_conf, schema_string, JSONSerializer, to_dict, serializer_conf)

def create_test_producer_avro(producer_conf, schema_registry_conf, schema_string, to_dict=None, serializer_conf=None):
    return create_test_producer(producer_conf, schema_registry_conf, schema_string, AvroSerializer, to_dict, serializer_conf)


def create_test_consumer(client_conf, schema_registry_conf, schema_string, group_id, topic, deserializer_class, from_dict=None):
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    value_deserializer = deserializer_class(schema_str=schema_string, schema_registry_client=schema_registry_client, from_dict=from_dict)

    consumer_config = copy.deepcopy(client_conf)
    consumer_config['group.id'] = group_id
    consumer_config['auto.offset.reset'] = 'earliest'
    consumer_config['key.deserializer'] = StringDeserializer('utf_8')
    consumer_config['value.deserializer'] = value_deserializer

    consumer = DeserializingConsumer(consumer_config)
    consumer.subscribe([topic])
    return consumer

def create_test_consumer_json_sr(client_conf, schema_registry_conf, schema_string, group_id, topic, from_dict=None):
    return create_test_consumer(client_conf, schema_registry_conf, schema_string, group_id, topic, JSONDeserializer, from_dict)

def create_test_consumer_avro(client_conf, schema_registry_conf, schema_string, group_id, topic, from_dict=None):
    return create_test_consumer(client_conf, schema_registry_conf, schema_string, group_id, topic, AvroDeserializer, from_dict)

def delete_consumer_groups(client_conf, group_ids):
    admin_client = AdminClient(client_conf)
    try:
        delete_results = admin_client.delete_consumer_groups(group_ids)

        for group, future in delete_results.items():
            try:
                future.result()  # The result() call will block until the group is deleted
                print(f"Consumer group '{group}' successfully deleted.")
            except KafkaException as e:
                print(f"Failed to delete consumer group '{group}': {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
            
def try_except(lambda_try, lambda_except, exception):
    try:
        return lambda_try()
    except exception:
        return lambda_except()
    

def acked(err: KafkaError, msg: Message):
    if err is not None:
        print(f"Failed to deliver message: {str(msg)}: {str(err)}")
    else:
        print(f"Message produced: {msg.value()}")
        print('%% %s [%d] at offset %d with key: %s\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  msg.key().decode("utf8")))

