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
TOPICS = ["si-topic-evo"]

def load_json(json_file: str) -> str | None:
    json_str = None
    if json_file:
        with open(json_file) as file:
            json_str = file.read()
    return json_str

schema_str = load_json("tests/si_prod_schema_v1.json")
data = load_json("tests/DataProductCDPricing.json")

#schema_closed = copy.deepcopy(schema)
#schema_closed["additionalProperties"] = False

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


def from_dict(obj, ctx):
    return obj


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


#def test_adding_new_property_with_open_model_is_backwards_compatible:
    
#def test_adding_new_property_with_closed_model_is_incompatible:
    
    
#def test_changing_close_to_open_model_is_forwards_compatible:
    
#def test_sending_irrelevant_data_in_open_model_consumed:


def test_produce_evo_closed():

    schema_v1_closed=json.loads(schema_str)

    schema_v1_closed["additionalProperties"] = False

    schema_v1_string = json.dumps(schema_v1_closed)

    producerV1 = create_test_producer(KAFKA_BROKER, SCHEMA_REGISTRY_URL, schema_v1_string)

    producerV1.produce('si-topic-evo', key='key1', value=json.loads(data), on_delivery=acked)
    producerV1.flush()

    consumer = create_test_consumer(KAFKA_BROKER, SCHEMA_REGISTRY_URL, schema_str, 'test-group', 'si-topic-evo')
    msg = consumer.poll(timeout=10.0)

    #print("consumed message:")
    #assert msg is not None
    #print(msg.value())

    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    #schema_registry_client.set_compatibility(subject_name="si-topic-evo-value", level="FORWARD")
    compat = try_except(lambda: schema_registry_client.get_compatibility(subject_name="si-topic-evo-value"), lambda: schema_registry_client.get_compatibility(), SchemaRegistryError)
    print(f"compat: {compat}")
    
    # schema V2 anpassen
    
    schema_v2_obj=copy.deepcopy(schema_v1_closed)
    
    # additionalProperties = True is BACKWARDS compatible
    schema_v2_obj["additionalProperties"] = True
    
    schema_v2_str = json.dumps(schema_v2_obj)
    
    schema_registry_client.test_compatibility(subject_name="si-topic-evo-value", schema=Schema(schema_v2_str, schema_type="JSON"), version="latest")
    
    #schema_v2_obj["new_attribute"] = {
    #    "type": "string"
    #  }
    
    added_attribute_compatible = schema_registry_client.test_compatibility(subject_name="si-topic-evo-value", schema=Schema(schema_v2_str, schema_type="JSON"), version="latest")
    
    print("added_attribute_compatible:")
    print(added_attribute_compatible)
    
    
    producerV2 = create_test_producer(KAFKA_BROKER, SCHEMA_REGISTRY_URL, schema_v2_str)
    
    data_v2_obj = json.loads(data)
    data_v2_obj["new_attribute"] = "bazinga!"

    producerV2.produce('si-topic-evo', key='key1', value=data_v2_obj, on_delivery=acked)
    producerV2.flush()
    
    msg2 = consumer.poll(timeout=10.0)

    print("consumed message:")
    assert msg2 is not None
    print(msg2.value())

    # compatibilität prüfen
    # mit schema v2 daten schreiben
