[![Tests](https://github.com/qdegraaf/kafka-twitter-producer/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/qdegraaf/kafka-twitter-producer/actions/workflows/tests.yml)

# kafka-twitter-producer
This repository contains a producer which connects to the Twitter V2 streaming API and publishes Kafka messages
using Confluent whenever a message matching a set of rules is posted. These rules can be set by the user
## Usage
To build the producer run `make build`.

To start the producer run:
```bash
./producer -rules rules.yml -reset False
```
Flags:
 - `-rules` **Required** and should point to a YAML containing the rules to filter the Twitter stream on
 - `-reset` **Optional** (Defaults to False) if set to `True` will attempt to delete all existing rules before creating new ones from file

### Environment
The producer requires the following environment variables to be set:

```
CONFLUENT_API_KEY=
CONFLUENT_API_SECRET=

KAFKA_BOOTSTRAP_SERVERS=
KAFKA_TOPIC=

TWITTER_API_KEY=
TWITTER_API_SECRET=
TWITTER_TOKEN_URL=
```
### Rules
For adding rules the producer expects a YAML file in the following format to be
passed with a `-rules` flag:
```yaml
rules:
  - name: someRule
    value: dogs
    tag: dogRule
  - name: someOtherRule
    value: cats
    tag: catRule
```
Where the `value` field is the string which the stream will be filtered for and `tag` is a label for the rule
useful for later processing. The `name` field is unused except for administrative purposes