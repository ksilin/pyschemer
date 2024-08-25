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
    create_test_consumer_json_sr,
    create_test_producer_json_sr,
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
TOPICS = ["test-topic-evo", "test-topic-evo-open"]
SCHEMA_REGISTRY_CONF = {'url': SCHEMA_REGISTRY_URL}
COMMON_CLIENT_CONF = {'bootstrap.servers': KAFKA_BROKER}

schema_open = {
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

schema_closed = copy.deepcopy(schema_open)
schema_closed["additionalProperties"] = False

schema_open_age_removed = copy.deepcopy(schema_open)
del schema_open_age_removed["properties"]["age"]

schema_open_job_added = copy.deepcopy(schema_open)
schema_open_job_added["properties"]["job"] = {
      "type": "string",
      "description": "The person's job."
    }

schema_closed_age_removed = copy.deepcopy(schema_closed)
del schema_closed_age_removed["properties"]["age"]

schema_closed_job_added = copy.deepcopy(schema_closed)
schema_closed_job_added["properties"]["job"] = {
      "type": "string",
      "description": "The person's job."
    }

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

def test_closed_model_remove_property_backward_incompatible():
    
    TOPIC_CLOSED = "test-topic-evo-closed"
    SUBJECT_CLOSED = f"{TOPIC_CLOSED}-value"
    clear_schema_registry_subjects(SCHEMA_REGISTRY_CONF, [SUBJECT_CLOSED])

    schema_stringV1 = json.dumps(schema_closed)
    schema_string_age_removed = json.dumps(schema_closed_age_removed)

    person = Person("Phineas", "Crumb", 123)

    producerV1 = create_test_producer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_stringV1)
    producerV1.produce(TOPIC_CLOSED, key='key1', value=person_to_dict(person), on_delivery=acked)
    producerV1.flush()

    consumer_group = f"test-group-{inspect.currentframe().f_code.co_name}"
    delete_consumer_groups(COMMON_CLIENT_CONF, [consumer_group])
    consumer = create_test_consumer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_stringV1, consumer_group, TOPIC_CLOSED, person_from_dict)
    msg = consumer.poll(timeout=10.0)
    assert msg is not None
    assert msg.key() == 'key1'
    assert msg.value() == person

    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONF)

    compat = try_except(lambda: schema_registry_client.get_compatibility(subject_name=SUBJECT_CLOSED), lambda: schema_registry_client.get_compatibility(), SchemaRegistryError)
    assert compat == 'BACKWARD'
    
    age_removed_compatible = schema_registry_client.test_compatibility(subject_name=SUBJECT_CLOSED, schema=Schema(schema_string_age_removed, schema_type="JSON"))
    assert age_removed_compatible is False
    
    with pytest.raises(SchemaRegistryError, match="The new has a closed content model and is missing a property or item present at path '#/properties/age' in the old schema"):
      schema_registry_client.register_schema(subject_name=SUBJECT_CLOSED, schema=Schema(schema_str=schema_string_age_removed, schema_type="JSON"),normalize_schemas=True)


    producer_age_removed = create_test_producer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_string_age_removed)

    with pytest.raises(ValueSerializationError, match="Schema being registered is incompatible with an earlier schema for subject"):
        producer_age_removed.produce(TOPIC_CLOSED, key='key1', value=person_to_dict(person), on_delivery=acked)

def test_closed_model_add_property_backward_compatible():
    
    TOPIC_CLOSED = "test-topic-evo-closed"
    SUBJECT_CLOSED = f"{TOPIC_CLOSED}-value"
    clear_schema_registry_subjects(SCHEMA_REGISTRY_CONF, [SUBJECT_CLOSED])

    schema_stringV1 = json.dumps(schema_closed)
    schema_string_job_added = json.dumps(schema_closed_job_added)

    person = Person("Phineas", "Crumb", 123)

    producerV1 = create_test_producer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_stringV1)
    producerV1.produce(TOPIC_CLOSED, key='key1', value=person_to_dict(person), on_delivery=acked)
    producerV1.flush()

    consumer_group = f"test-group-{inspect.currentframe().f_code.co_name}"
    delete_consumer_groups(COMMON_CLIENT_CONF, [consumer_group])
    consumer = create_test_consumer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_stringV1, consumer_group, TOPIC_CLOSED, person_from_dict)
    msg = consumer.poll(timeout=10.0)
    assert msg is not None
    assert msg.key() == 'key1'
    assert msg.value() == person

    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONF)

    compat = try_except(lambda: schema_registry_client.get_compatibility(subject_name=SUBJECT_CLOSED), lambda: schema_registry_client.get_compatibility(), SchemaRegistryError)
    assert compat == 'BACKWARD'
    
    added_property_compatible = schema_registry_client.test_compatibility(subject_name=SUBJECT_CLOSED, schema=Schema(schema_string_job_added, schema_type="JSON"))
    assert added_property_compatible is True

    personAddedJob = PersonAddedJob("Phineas", "Crumb", "assistant", 123)
    producerAddedJob = create_test_producer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_string_job_added)
    producerAddedJob.produce(TOPIC_CLOSED, key='key1', value=person_job_added_to_dict(personAddedJob), on_delivery=acked)
    producerAddedJob.flush()
    msg = consumer.poll(timeout=10.0)
    assert msg is not None
    assert msg.key() == 'key1'
    assert msg.value() == person


def test_open_model_remove_property_backward_incompatible():

    TOPIC_OPEN = "test-topic-evo-open"
    SUBJECT_OPEN = f"{TOPIC_OPEN}-value"
    clear_schema_registry_subjects(SCHEMA_REGISTRY_CONF, [SUBJECT_OPEN])

    schema_stringV1 = json.dumps(schema_open)
    schema_string_age_removed = json.dumps(schema_open_age_removed)

    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONF)
    schema_registry_client.register_schema(subject_name=SUBJECT_OPEN, schema=Schema(schema_str=schema_stringV1, schema_type="JSON"), normalize_schemas=True)

    person = Person("Phineas", "Crumb", 123)

    producerV1 = create_test_producer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_stringV1)

    producerV1.produce(TOPIC_OPEN, key='key1', value=person_to_dict(person), on_delivery=acked)
    producerV1.flush()

    consumer_group = f"test-group-{inspect.currentframe().f_code.co_name}"
    delete_consumer_groups(COMMON_CLIENT_CONF, [consumer_group])
    consumer = create_test_consumer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_stringV1, consumer_group, TOPIC_OPEN, person_from_dict)
    msg = consumer.poll(timeout=10.0)
    assert msg is not None
    assert msg.key() == 'key1'
    assert msg.value() == person

    compat = try_except(lambda: schema_registry_client.get_compatibility(subject_name=SUBJECT_OPEN), lambda: schema_registry_client.get_compatibility(), SchemaRegistryError)
    assert compat == "BACKWARD"

    v2_compatible = schema_registry_client.test_compatibility(subject_name=SUBJECT_OPEN, schema=Schema(schema_string_age_removed, schema_type="JSON"))
    assert v2_compatible is False

    # removing property in OPEN model is NOT BACKWARD compatible
    with pytest.raises(SchemaRegistryError, match="A property or item is missing in the new schema but present at path '#/properties/age' in the old schema and is not covered by its partially open content model"):
      schema_registry_client.register_schema(subject_name=SUBJECT_OPEN, schema=Schema(schema_str=schema_string_age_removed, schema_type="JSON"),normalize_schemas=True)

    producerV2 = create_test_producer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_string_age_removed)

    # errorType:"PROPERTY_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", description:"A property or item is missing in the new schema but present at path '#/properties/age' in the old schema and is not covered by its partially open content model'
    with pytest.raises(ValueSerializationError, match="A property or item is missing in the new schema but present at path '#/properties/age' in the old schema and is not covered by its partially open content model"):
      producerV2.produce(TOPIC_OPEN, key='key1', value=person_to_dict(person), on_delivery=acked)
      producerV2.flush()

    consumer.close()

def test_open_model_add_property_backward_compatible():

    TOPIC_OPEN = "test-topic-evo-open"
    SUBJECT_OPEN = f"{TOPIC_OPEN}-value"
    clear_schema_registry_subjects(SCHEMA_REGISTRY_CONF, [SUBJECT_OPEN])

    schema_stringV1 = json.dumps(schema_open)
    schema_string_job_added = json.dumps(schema_open_job_added)

    schema_registry_client = SchemaRegistryClient(SCHEMA_REGISTRY_CONF)
    id1 = schema_registry_client.register_schema(subject_name=SUBJECT_OPEN, schema=Schema(schema_str=schema_stringV1, schema_type="JSON"), normalize_schemas=True)
    print(f"new schema id: {id1}")

    person = Person("Phineas", "Crumb", 123)

    producerV1 = create_test_producer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_stringV1)

    producerV1.produce(TOPIC_OPEN, key='key1', value=person_to_dict(person), on_delivery=acked)
    producerV1.flush()

    consumer_group = f"test-group-{inspect.currentframe().f_code.co_name}"
    delete_consumer_groups(COMMON_CLIENT_CONF, [consumer_group])
    consumer = create_test_consumer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_stringV1, consumer_group, TOPIC_OPEN, person_from_dict)
    msg = consumer.poll(timeout=10.0)
    assert msg is not None
    assert msg.key() == 'key1'
    assert msg.value() == person

    compat = try_except(lambda: schema_registry_client.get_compatibility(subject_name=SUBJECT_OPEN), lambda: schema_registry_client.get_compatibility(), SchemaRegistryError)
    assert compat == "BACKWARD"

    # adding property in OPEN model is BACKWARD compatible
    add_property_compatible = schema_registry_client.test_compatibility(subject_name=SUBJECT_OPEN, schema=Schema(schema_string_job_added, schema_type="JSON"))
    assert add_property_compatible is True
    schema_registry_client.register_schema(subject_name=SUBJECT_OPEN, schema=Schema(schema_str=schema_string_job_added, schema_type="JSON"),normalize_schemas=True)

    # adding property in OPEN model is BACKWARD compatible
    personAddedJob = PersonAddedJob("Phineas", "Crumb", "assistant", 123)
    producerAddedJob = create_test_producer_json_sr(COMMON_CLIENT_CONF, SCHEMA_REGISTRY_CONF, schema_string_job_added)
    producerAddedJob.produce(TOPIC_OPEN, key='key1', value=person_job_added_to_dict(personAddedJob), on_delivery=acked)
    producerAddedJob.flush()
    msg = consumer.poll(timeout=10.0)
    assert msg is not None
    assert msg.key() == 'key1'
    assert msg.value() == person

    consumer.close()
