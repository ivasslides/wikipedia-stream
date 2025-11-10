import json
from quixstreams import Application
import redis 

def produce_to_kafka(event):
    app = Application(
        broker_address="127.0.0.1:19092",
        loglevel="DEBUG",
        producer_extra_config={
            "broker.address.family": "v4",
        }
    )

    try: 
        value = event.value() 
        key = event.key()

        # produce to kafka
        with app.get_producer() as producer:
            producer.produce(
                topic="new-articles",
                key=key,
                value=value,
            )

        return True 
    except Exception as e:
        print(f"Error producing to Kafka: {e}")

def main():
    app = Application(
        broker_address="127.0.0.1:19092",
        loglevel="DEBUG",
        consumer_group="new-articles",
        auto_offset_reset="earliest",
        producer_extra_config={
            "broker.address.family": "v4",
        }
    )

# if of this particular type, the publish
# if not then ignore it 

if __name__ == "__main__":
    main()