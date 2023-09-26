# Import the necessary libraries
from kafka import KafkaProducer
import json

# Define constants for Kafka configuration
KAFKA_TOPIC = "first_topic"        # Kafka topic where weather data will be sent
KAFKA_BROKER = "localhost:9092"    # Kafka broker address

# Define API credentials and URL for weather data
API_KEY =  # RapidAPI Key
API_URL =  # Weather API URL

# Function to fetch weather data for a given city
def fetch_weather_data(city_name):
    import requests
    # Send a GET request to the Weather API with the city name and API key as headers
    response = requests.get(API_URL, params={"q": city_name}, headers={"X-RapidAPI-Key": API_KEY})
    # Parse the JSON response and return the weather data
    return response.json()

# Main function
def main():
    # List of cities for which you want weather data
    cities = ["Paris", "London", "New York", "Tokyo", "Sydney"]

    # Initialize a Kafka producer with the specified broker and JSON serializer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # Continuously fetch and send weather data for the specified cities to the Kafka topic
    while True:
        for city in cities:
            # Fetch weather data for the current city
            weather_data = fetch_weather_data(city)
            
            # Send the weather data as a JSON-encoded message to the Kafka topic
            producer.send(KAFKA_TOPIC, value=weather_data)
            
            # Ensure the message is sent immediately
            producer.flush()

# Entry point of the script
if __name__ == "__main__":
    main()

