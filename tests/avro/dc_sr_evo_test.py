import copy
import inspect
import json
import logging

import sys
import os

# Add the src directory to the sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../tests')))

import pytest
from confluent_kafka.admin import AdminClient
from confluent_kafka.error import ValueSerializationError
from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
    SchemaRegistryError,
)

from py_json_sr_evo.person import Person
from py_json_sr_evo.personAddedJob import PersonAddedJob
from test_utils import (
    acked,
    clear_schema_registry_subjects,
    create_kafka_topics,
    create_test_consumer_avro,
    create_test_producer_avro,
    delete_consumer_groups,
    delete_kafka_topics,
    try_except,
)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
logger.addHandler(handler)

KAFKA_BROKER = "localhost:39092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPICS = ["test-topic-evo"]
SCHEMA_REGISTRY_CONF = {'url': SCHEMA_REGISTRY_URL}
COMMON_CLIENT_CONF = {'bootstrap.servers': KAFKA_BROKER}

schema_v1 = {
    "type": "record",
    "name": "Person",
    "fields": [
        {"name": "firstName", "type": "string"},
        {"name": "lastName", "type": "string"},
        {"name": "age", "type": "int", "default": 0}
    ]
}

schema_v2_remove_age = copy.deepcopy(schema_v1)
schema_v2_remove_age["fields"] = [field for field in schema_v2_remove_age["fields"] if field["name"] != "age"]

schema_v3_add_job_with_default = copy.deepcopy(schema_v1)
schema_v3_add_job_with_default["fields"].append({
    "name": "job",
    "type": "string",
    "default": "Unemployed"
})

schema_v4_add_job_no_default = copy.deepcopy(schema_v1)
schema_v4_add_job_no_default["fields"].append({
    "name": "job",
    "type": "string"
})

@pytest.fixture(scope="session", autouse=True)
def setup_kafka_and_schema_registry():
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    delete_kafka_topics(admin_client, TOPICS)
    create_kafka_topics(admin_client, TOPICS)

    yield


def person_to_dict(person: Person) -> dict:
    return {
        "firstName": person.firstName,
        "lastName": person.lastName,
        "age": person.age
    }

def person_job_added_to_dict(person: PersonAddedJob) -> dict:
    return {
        "firstName": person.firstName,
        "lastName": person.lastName,
        "job": person.job,
        "age": person.age
    }

def person_from_dict(obj, ctx):
    return Person(obj["firstName"], obj["lastName"], obj.get("age",0))

def test_remove_property_backward_incompatible():
    
    TOPIC = "test-topic-evo"
    SUBJECT = f"{TOPIC}-value"
    clear_schema_registry_subjects(SCHEMA_REGISTRY_CONF, [SUBJECT])

    schema_string_v1 = json.dumps(schema_v1)
    schema_string_age_removed = json.dumps(schema_v2_remove_age)

    person = Person("Phineas", "Crumb", 123)

    producer_v1 = create_test_producer_avro(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_string_v1)
    producer_v1.produce(TOPIC, key='key1', value=person_to_dict(person), on_delivery=acked)
    producer_v1.flush()

    consumer_group = f"test-group-{inspect.currentframe().f_code.co_name}"
    delete_consumer_groups(COMMON_CLIENT_CONF, [consumer_group])
    consumer = create_test_consumer_avro(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_string_v1, consumer_group, TOPIC, person_from_dict)
    msg = consumer.poll(timeout=10.0)
    assert msg is not None
    assert msg.key() == 'key1'
    assert msg.value() == person
    consumer.close()

    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONF)

    compat = try_except(lambda: schema_registry_client.get_compatibility(subject_name=SUBJECT), lambda: schema_registry_client.get_compatibility(), SchemaRegistryError)
    assert compat == 'BACKWARD'
    
    age_removed_compatible = schema_registry_client.test_compatibility(subject_name=SUBJECT, schema=Schema(schema_string_age_removed, schema_type="AVRO"))
    print(age_removed_compatible)
    
    # with pytest.raises(SchemaRegistryError, match="The new has a closed content model and is missing a property or item present at path '#/properties/age' in the old schema"):
    schema_registry_client.register_schema(subject_name=SUBJECT, schema=Schema(schema_str=schema_string_age_removed, schema_type="AVRO"),normalize_schemas=True)

    producer_age_removed = create_test_producer_avro(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_string_age_removed)

    # with pytest.raises(ValueSerializationError, match="Schema being registered is incompatible with an earlier schema for subject"):
    producer_age_removed.produce(TOPIC, key='key1', value=person_to_dict(person), on_delivery=acked)
    
    # TODO - read back
    