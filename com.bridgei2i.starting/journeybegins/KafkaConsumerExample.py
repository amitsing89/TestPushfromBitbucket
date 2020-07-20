import json
from time import sleep

from kafka import KafkaConsumer

if __name__ == '__main__':
    # parsed_topic_name = 'raw_recipes'
    parsed_topic_name = 'parsed_recipes'
    # Notify if a recipe has more than 200 calories
    calories_threshold = 200

    consumer = KafkaConsumer(parsed_topic_name, bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='smallest', enable_auto_commit=True,
                             auto_commit_interval_ms=1000, group_id=None)
    print(consumer)
    for msg in consumer:
        print(msg)
        record = json.loads(msg.value)
        print(record)
        calories = int(record['calories'])
        title = record['title']

        if calories > calories_threshold:
            print('Alert: {} calories count is {}'.format(title, calories))
        sleep(3)

    if consumer is not None:
        consumer.close()
