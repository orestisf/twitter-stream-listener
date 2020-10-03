# twitter-stream-listener
A dockerized python app which uses Tweepy's Streamlistener class to track a list of hashtags, extract relevant tweet information and publishes them to an Azure Event Hub instance.

## Usage
The application accepts the following environmental variables:

- CONSUMER_KEY: Twitter dev app API key.
- CONSUMER_SECRET: Twitter dev app API key secret.
- ACCESS_KEY: Twitter def app acess token.
- ACCESS_SECRET: Twitter def app acess token secret.
- EVENTHUB_ENDPOINT: The event hub primary endpoint. Note that you need a Shared Access Key with at least WRITE permissions on the event hub.
- EVENTHUB_NAME: The event hub instance name.
- RUNTIME_MINS: Set to an integer to indicate how many minutes the application will run. After the specified time is elapsed, the application will stop.
- HASHTAGS: Insert a comma separated list of hashtags to track. DO NOT add the hash symbol ,the application will handle that by itself. Example: 'Hashtag1, Hashtag2, Hashtag3'