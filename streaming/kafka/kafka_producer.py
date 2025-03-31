import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime
import sys

class YelpKafkaProducer:
    """
    Producer that simulates real-time Yelp data events and streams them to Kafka topics.
    """
    
    def __init__(self, bootstrap_servers='localhost:9092'):
        """Initialize the Kafka producer with server address."""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        # Load sample business and user IDs
        self.business_ids = self._load_business_ids()
        self.user_ids = self._load_user_ids()
        
        print(f"Loaded {len(self.business_ids)} businesses and {len(self.user_ids)} users")
    
    def _load_business_ids(self):
        """Load a list of business IDs from the business dataset."""
        business_ids = []
        try:
            with open('/app/data/yelp_academic_dataset_business.json', 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 1000:  # Limit to 1000 businesses for demo
                        break
                    data = json.loads(line)
                    business_ids.append(data['business_id'])
        except Exception as e:
            print(f"Error loading business IDs: {e}")
            business_ids = ["sample_business_id_1", "sample_business_id_2", "sample_business_id_3"]
        
        return business_ids
    
    def _load_user_ids(self):
        """Load a list of user IDs from the user dataset."""
        user_ids = []
        try:
            with open('/app/data/yelp_academic_dataset_user.json', 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= 1000:  # Limit to 1000 users for demo
                        break
                    data = json.loads(line)
                    user_ids.append(data['user_id'])
        except Exception as e:
            print(f"Error loading user IDs: {e}")
            user_ids = ["sample_user_id_1", "sample_user_id_2", "sample_user_id_3"]
        
        return user_ids
    
    def generate_review(self):
        """Generate a simulated review."""
        business_id = random.choice(self.business_ids)
        user_id = random.choice(self.user_ids)
        stars = random.randint(1, 5)
        
        review = {
            'review_id': f"STREAM_{int(time.time())}_{random.randint(1000, 9999)}",
            'business_id': business_id,
            'user_id': user_id,
            'stars': stars,
            'date': datetime.now().isoformat(),
            'text': self._generate_review_text(stars),
            'useful': 0,
            'funny': 0,
            'cool': 0,
            'source': 'streaming'
        }
        
        return review
    
    def generate_checkin(self):
        """Generate a simulated checkin."""
        business_id = random.choice(self.business_ids)
        
        checkin = {
            'business_id': business_id,
            'date': datetime.now().isoformat(),
            'count': random.randint(1, 5),
            'source': 'streaming'
        }
        
        return checkin
    
    def _generate_review_text(self, stars):
        """Generate realistic review text based on star rating."""
        positive_phrases = [
            "Amazing experience! Highly recommend.",
            "The service was excellent.",
            "Best food I've had in a long time.",
            "Will definitely come back again.",
            "Great value for money.",
            "Friendly staff and fast service.",
            "Exceeded my expectations.",
            "Such a wonderful place!",
            "The atmosphere was perfect."
        ]
        
        negative_phrases = [
            "Disappointing experience overall.",
            "The service was slow and inattentive.",
            "Food was cold and undercooked.",
            "Would not recommend.",
            "Overpriced for what you get.",
            "Staff was unfriendly.",
            "Did not meet expectations.",
            "The place was too noisy and crowded.",
            "Long wait times and mediocre food."
        ]
        
        neutral_phrases = [
            "It was okay, nothing special.",
            "Average experience overall.",
            "Some things were good, others not so much.",
            "Might give it another try.",
            "Food was decent but service could be better.",
            "Reasonable prices but nothing extraordinary.",
            "Has potential but needs improvement."
        ]
        
        if stars >= 4:
            phrases = positive_phrases
        elif stars <= 2:
            phrases = negative_phrases
        else:
            phrases = neutral_phrases
        
        # Combine 2-3 phrases for a more realistic review
        num_phrases = random.randint(2, 3)
        selected_phrases = random.sample(phrases, num_phrases)
        
        return " ".join(selected_phrases)
    
    def stream_events(self, interval=2, count=None):
        """
        Stream events to Kafka topics.
        
        Args:
            interval: Time between events in seconds
            count: Number of events to generate (None for infinite)
        """
        i = 0
        try:
            while count is None or i < count:
                # Generate a review (70% probability)
                if random.random() < 0.7:
                    review = self.generate_review()
                    self.producer.send('yelp-reviews', key=review['business_id'], value=review)
                    print(f"Sent review: {review['review_id']} for business {review['business_id']}")
                
                # Generate a checkin (30% probability)
                else:
                    checkin = self.generate_checkin()
                    self.producer.send('yelp-checkins', key=checkin['business_id'], value=checkin)
                    print(f"Sent checkin for business {checkin['business_id']}")
                
                self.producer.flush()
                i += 1
                
                # Sleep for the specified interval
                time.sleep(interval)
        
        except KeyboardInterrupt:
            print("Streaming stopped by user")
        finally:
            self.producer.close()
            print(f"Producer closed. Streamed {i} events.")

if __name__ == "__main__":
    # Set default values
    interval = 2  # seconds
    count = 100   # events
    
    # Parse command-line arguments
    if len(sys.argv) > 1:
        try:
            interval = float(sys.argv[1])
        except ValueError:
            print("Invalid interval value. Using default 2 seconds.")
    
    if len(sys.argv) > 2:
        try:
            count = int(sys.argv[2])
        except ValueError:
            print("Invalid count value. Using default 100 events.")
    
    # Initialize and run producer
    producer = YelpKafkaProducer()
    print(f"Starting to stream events every {interval} seconds. Press Ctrl+C to stop.")
    producer.stream_events(interval=interval, count=count)