from quixstreams import Application
import json


def main():
    app = Application(
        broker_address="127.0.0.1:19092",
        loglevel="DEBUG",
        consumer_group="wikipedia-consumer",
        auto_offset_reset="earliest",
        producer_extra_config={
            "broker.address.family": "v4",
        }
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["wikipedia-changes"])
        type_edit = 0
        type_categorize = 0
        type_log = 0
        type_new = 0
        type_other = 0

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


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # make values into strings with 0 decimal places
        # type_edit = str(type_edit)
        # type_categorize = str(type_categorize)
        # type_log = str(type_log)
        # type_new = str(type_new)
        # type_other = str(type_other)
        # print(f"Edit: {type_edit}, Categorize: {type_categorize}, Log: {type_log}, New: {type_new}, Other: {type_other}")
        pass
