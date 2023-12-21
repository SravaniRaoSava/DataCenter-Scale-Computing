from confluent_kafka import Producer
import csv
import json

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_data(csv_file, topic_name):
    producer_conf = {'bootstrap.servers': 'localhost:9092', 'queue.buffering.max.messages': 1000000}
    producer = Producer(producer_conf)

    with open(csv_file, newline='', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            # Convert each CSV row to a structured schema JSON object
            structured_json_message = {
                "fuelType": row["fuelType"],
                "rating": float(row["rating"]),
                "renterTripsTaken": int(row["renterTripsTaken"]),
                "reviewCount": int(row["reviewCount"]),
                "location": {
                    "city": row["city"],
                    "country": row["Country"],
                    "state": row["state"],
                    "latitude": float(row["latitude"]),
                    "longitude": float(row["longitude"])
                },
                "owner": {
                    "owner_id": int(row["owner_id"]),
                },
                "vehicle": {
                    "daily_rate": float(row["daily_rate"]),
                    "make": row["make"],
                    "model": row["model"],
                    "type": row["type"],
                    "year": int(row["year"])
                },
                "airport_city": row["airport_city"]
            }

            # Serialize the structured schema JSON object to a string
            message_value = json.dumps(structured_json_message)
            
            # Produce the message to the Kafka topic
            producer.produce(topic=topic_name, value=message_value, callback=delivery_report)
            producer.poll(0)  # Trigger message delivery callback

    # Wait for outstanding messages to be delivered
    producer.flush()

if __name__ == '__main__':
    csv_file_path = 'C:/Users/akith/Desktop/DCSC/Final_Project/car_data.csv'
    kafka_topic = 'dcsc'

    produce_data(csv_file_path, kafka_topic)