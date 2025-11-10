from quixstreams import Application
import json
<<<<<<< HEAD


def main():
    app = Application(
        broker_address="127.0.0.1:19092",
=======
import redis

def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
>>>>>>> e2395af5f9706c6b5f4c097121031a04930865e9
        loglevel="DEBUG",
        consumer_group="wikipedia-consumer",
        auto_offset_reset="earliest",
        producer_extra_config={
<<<<<<< HEAD
=======
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
>>>>>>> e2395af5f9706c6b5f4c097121031a04930865e9
            "broker.address.family": "v4",
        }
    )

    with app.get_consumer() as consumer:
<<<<<<< HEAD
        consumer.subscribe(["wikipedia-changes"])
        type_edit = 0
        type_categorize = 0
        type_log = 0
        type_new = 0
        type_other = 0
=======
        consumer.subscribe(["wikipedia-edits"])
>>>>>>> e2395af5f9706c6b5f4c097121031a04930865e9

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()

<<<<<<< HEAD
                # pull type of change of the event
                change_type = value.get("type")
                # increment appropriate counter based on the type of change
                if change_type == "edit":
                    type_edit += 1
                elif change_type == "categorize":
                    type_categorize += 1
                elif change_type == "log":
                    type_log += 1
                elif change_type == "new":
                    type_new += 1
                else:
                    type_other += 1

                # print(f"{offset} {key} {value}")
                print(f"{offset} {key} {change_type}")
                consumer.store_offsets(msg)

    # return type_edit, type_categorize, type_log, type_new, type_other


=======
                # get out the "type" key from the value
                change_type = value.get("type")
                
                # Only process if change_type is not None
                if change_type is not None:
                    # save these values to redis and use incr() to increment the value
                    redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0)
                    redis_client.incr(change_type)
                    # print(f"Type count: {redis_client.get(change_type).decode('utf8')}")
                    redis_client.close()

                    # print on the screen
                    print(f"{offset} --> {change_type}")
                else:
                    print(f"{offset} {key} - No 'type' field in message")
                # print(f"{offset} {key} {value}")
                consumer.store_offsets(msg)

>>>>>>> e2395af5f9706c6b5f4c097121031a04930865e9
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
<<<<<<< HEAD
        # make values into strings with 0 decimal places
        # type_edit = str(type_edit)
        # type_categorize = str(type_categorize)
        # type_log = str(type_log)
        # type_new = str(type_new)
        # type_other = str(type_other)
        # print(f"Edit: {type_edit}, Categorize: {type_categorize}, Log: {type_log}, New: {type_new}, Other: {type_other}")
        pass
=======
        redis_client = redis.Redis(host="localhost", port=6379, db=0)
        # for each key in redis, print the key and value
        for key in redis_client.keys():
            print(f"Type: {key.decode('utf8')} | Count: {redis_client.get(key).decode('utf8')}")
        redis_client.close()
        pass
>>>>>>> e2395af5f9706c6b5f4c097121031a04930865e9
