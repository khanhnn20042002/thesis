import kafka
import utils
import subprocess
from collections import OrderedDict
import os

def reset_consumer_offsets(admin_client):
    admin_client.delete_topics(topics=['__consumer_offsets'])

def create_topic(admin_client, name, num_partitions=2, replication_factor=1):
    if name in admin_client.list_topics():
        admin_client.delete_topics(topics=[name])
    topic_list = [kafka.admin.NewTopic(name=name, num_partitions=num_partitions, replication_factor=replication_factor)]
    utils.try_until_success(admin_client.create_topics, 5, kafka.errors.TopicAlreadyExistsError, 10, new_topics=topic_list)

def delete_offsets(group, topic):
    kafka_command = f"kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete-offsets --group {group} --topic {topic}"
    gnome_terminal_command = f"gnome-terminal -- bash -c '{kafka_command}; exec bash'"
    subprocess.run(gnome_terminal_command, shell=True, check=True)

def start_indicator_calculator(indicator, process_ids, args=tuple()):
    command = f"mvn exec:java -Dexec.mainClass=com.indicator.{indicator}Calculator -Dexec.args=\"{' '.join([str(arg) for arg in args])}\" -f ./technical-indicators/pom.xml"
    process = subprocess.Popen(command, shell=True, preexec_fn=os.setpgrp)
    process_ids[indicator] = process.pid

if __name__ == '__main__':
    bootstrap_servers = ["localhost:9092"]
    admin_client = kafka.KafkaAdminClient(bootstrap_servers=bootstrap_servers)