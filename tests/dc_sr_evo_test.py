import copy
import json
import logging

import pytest
import requests
from confluent_kafka import (
    DeserializingConsumer,
    KafkaError,
    Message,
    SerializingProducer,
)
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import ValueSerializationError
from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
    SchemaRegistryError,
)
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

from src.py_json_sr_evo.person import Person

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = "localhost:39092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPICS = ["test-topic-evo", "test-topic-evo-open"]

schema = {
  "$id": "https://example.com/person.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Person",
  "type": "object",
  "properties": {
    "firstName": {
      "type": "string",
      "description": "The person's first name."
    },
    "lastName": {
      "type": "string",
      "description": "The person's last name."
    },
    "age": {
      "description": "Age in years which must be equal to or greater than zero.",
      "type": "integer",
      "minimum": 0
    },
  },
  "additionalProperties": { "type": "string" }
}

schema_closed = copy.deepcopy(schema)
schema_closed["additionalProperties"] = False

schema_age_removed = copy.deepcopy(schema)
del schema_age_removed["properties"]["age"]

schema_job_added = copy.deepcopy(schema)
schema_job_added["properties"]["job"] = {
      "type": "string",
      "description": "The person's job."
    }

schema_closed_age_removed = copy.deepcopy(schema_closed)
del schema_closed_age_removed["properties"]["age"]

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

def clear_schema_registry_subjects(schema_registry_url, topics):
    for topic in topics:
        subject_url = f"{schema_registry_url}/subjects/{topic}-value"
        response = requests.delete(subject_url)
        if response.status_code == 200:
            print(f"Deleted schema registry subject for topic {topic}")
        else:
            print(f"Failed to delete schema registry subject for topic {topic}: {response.status_code} {response.text}")

@pytest.fixture(scope="session", autouse=True)
def setup_kafka_and_schema_registry():
    # Clear Kafka topics
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    delete_kafka_topics(admin_client, TOPICS)
    create_kafka_topics(admin_client, TOPICS)

    # Clear Schema Registry subjects
    clear_schema_registry_subjects(SCHEMA_REGISTRY_URL, TOPICS)

    yield


def person_to_dict(person: Person) -> dict:
    return {
        "firstName": person.firstName,
        "lastName": person.lastName,
        "age": person.age
    }

def create_test_producer(kafka_broker, schema_registry_url, schema_string):# to_dict = person_to_dict):
    schema_registry_conf = {'url': schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    value_serializer = JSONSerializer(schema_str=schema_string, schema_registry_client=schema_registry_client)#, to_dict=person_to_dict)

    producer_conf = {
        'bootstrap.servers': kafka_broker,
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': value_serializer
    }
    return SerializingProducer(producer_conf)


# Define the from_dict function
def from_dict(obj, ctx):
    return obj


def person_from_dict(obj, ctx):
    return Person(obj["firstName"], obj["lastName"], obj.get("age",0))

def create_test_consumer(kafka_broker, schema_registry_url, schema_string, group_id, topic, from_dict = from_dict):
    schema_registry_conf = {'url': schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    value_deserializer = JSONDeserializer(schema_string, from_dict, schema_registry_client)

    consumer_conf = {
        'bootstrap.servers': kafka_broker,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': value_deserializer
    }
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])
    return consumer

def acked(err: KafkaError, msg: Message):
    if err is not None:
        print(f"Failed to deliver message: {str(msg)}: {str(err)}")
    else:
        print(f"Message produced: {msg.value()}")
        print('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  msg.key().decode("utf8")))


def try_except(lambda_try, lambda_except, exception):
    try:
        return lambda_try()
    except exception:
        return lambda_except()

def test_produce_evo_closed():

    schema_stringV1 = json.dumps(schema_closed)
    schema_stringV2 = json.dumps(schema_closed_age_removed)
    
    person = Person("Phineas", "Crumb", 123)

    producerV1 = create_test_producer(KAFKA_BROKER, SCHEMA_REGISTRY_URL, schema_stringV1)

    producerV1.produce('test-topic-evo', key='key1', value=person_to_dict(person), on_delivery=acked)
    producerV1.flush()
    
    consumer = create_test_consumer(KAFKA_BROKER, SCHEMA_REGISTRY_URL, schema_stringV1, 'test-group', 'test-topic-evo', person_from_dict)
    msg = consumer.poll(timeout=10.0)

    print("consumed message:")
    print(msg.value())

    assert msg is not None
    assert msg.key() == 'key1'
    assert msg.value() == person
    
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    compat = try_except(lambda: schema_registry_client.get_compatibility(subject_name="test-topic-evo-value"), lambda: schema_registry_client.get_compatibility(), SchemaRegistryError)
    print(f"compat: {compat}")
    
    v2_compatible = schema_registry_client.test_compatibility(subject_name="test-topic-evo-value", schema=Schema(schema_stringV2, schema_type="JSON"))
    print(f"v2 compatible: {v2_compatible}")
    
    producerV2 = create_test_producer(KAFKA_BROKER, SCHEMA_REGISTRY_URL, schema_stringV2)

    with pytest.raises(ValueSerializationError, match="Schema being registered is incompatible with an earlier schema for subject"):
        producerV2.produce('test-topic-evo', key='key1', value=person_to_dict(person), on_delivery=acked)

def test_produce_evo_open():

    schema_stringV1 = json.dumps(schema)
    schema_stringV2 = json.dumps(schema_age_removed)
    schema_stringV3 = json.dumps(schema_job_added)
    
    person = Person("Phineas", "Crumb", 123)

    producerV1 = create_test_producer(KAFKA_BROKER, SCHEMA_REGISTRY_URL, schema_stringV1)

    producerV1.produce('test-topic-evo-open', key='key1', value=person_to_dict(person), on_delivery=acked)
    producerV1.flush()
    
    consumer = create_test_consumer(KAFKA_BROKER, SCHEMA_REGISTRY_URL, schema_stringV1, 'test-group', 'test-topic-evo-open', person_from_dict)
    msg = consumer.poll(timeout=10.0)

    print("consumed V1 message:")
    print(msg.value())

    assert msg is not None
    assert msg.key() == 'key1'
    assert msg.value() == person
    
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    compat = try_except(lambda: schema_registry_client.get_compatibility(subject_name="test-topic-evo-open-value"), lambda: schema_registry_client.get_compatibility(), SchemaRegistryError)
    print(f"compat: {compat}")
    
    v2_compatible = schema_registry_client.test_compatibility(subject_name="test-topic-evo-open-value", schema=Schema(schema_stringV2, schema_type="JSON"))
    print(f"v2 compatible: {v2_compatible}")
    
    producerV2 = create_test_producer(KAFKA_BROKER, SCHEMA_REGISTRY_URL, schema_stringV2)

    producerV2.produce('test-topic-evo-open', key='key1', value=person_to_dict(person), on_delivery=acked)
    producerV2.flush()
    
    msg2 = consumer.poll(timeout=10.0)

    print("consumed V2 message:")
    print(msg2.value())
    
    v3_compatible = schema_registry_client.test_compatibility(subject_name="test-topic-evo-open-value", schema=Schema(schema_stringV3, schema_type="JSON"))
    print(f"v3 compatible: {v3_compatible}")
    
    producerV3 = create_test_producer(KAFKA_BROKER, SCHEMA_REGISTRY_URL, schema_stringV3)

    producerV3.produce('test-topic-evo-open', key='key1', value=person_to_dict(person), on_delivery=acked)
    producerV3.flush()
    
    msg3 = consumer.poll(timeout=10.0)

    print("consumed V3 message:")
    print(msg3.value())

    consumer.close()