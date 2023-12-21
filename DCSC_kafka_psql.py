from confluent_kafka import Consumer, KafkaError
import psycopg2
import json

def consume_and_insert(consumer_conf, topic_name, db_params):
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic_name])

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        while True:
            msg = consumer.poll(1.0)  # Timeout in seconds
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print("Reached end of partition, exiting consumer loop.")
                    break
                else:
                    print(f"Error: {msg.error()}")
            else:
                # Message successfully received
                json_data = msg.value().decode('utf-8')
                print(json_data)
                data = json.loads(json_data)

                # Insert data into PostgreSQL
                insert_query = """
                    INSERT INTO car_rental_data (
                        fueltype, rating, rentertripstaken, reviewcount,
                        city, country, state, latitude, longitude,
                        owner_id, daily_rate, make, model, type, year, airport_city
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """
                values = (
                    data['fuelType'],
                    data['rating'],
                    data['renterTripsTaken'],
                    data['reviewCount'],
                    data['location']['city'],
                    data['location']['country'],
                    data['location']['state'],
                    data['location']['latitude'],
                    data['location']['longitude'],
                    data['owner']['owner_id'],
                    data['vehicle']['daily_rate'],
                    data['vehicle']['make'],
                    data['vehicle']['model'],
                    data['vehicle']['type'],
                    data['vehicle']['year'],
                    data['airport_city']
                )
                cursor.execute(insert_query, values)
                conn.commit()

                print(f"Inserted data into PostgreSQL: {json_data}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close Kafka consumer and PostgreSQL connection
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == '__main__':
    # Kafka configuration
    bootstrap_servers = 'localhost:9092'
    group_id = 'my_consumer_group'
    consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

    # PostgreSQL configuration
    db_params = {
        'host': 'localhost',
        'port': '5432',
        'user': 'postgres',
        'password': 'postgres',
        'database': 'database'
    }

    kafka_topic = 'dcsc'

    consume_and_insert(consumer_conf, kafka_topic, db_params)