
const kafka = require('kafka-node');

let _connected = false;
const _connect = (producer) => {
  if (_connected)
    return Promise.resolve();

  return new Promise((resolve, reject) => {
    producer.on('ready', () => {
      _connected = true;
      return resolve();
    });
    producer.on('error', error => {
      _connected = false;
      return reject(error);
    });
  });
};

/**
 * @class KafkaProducer
 * @classdesc Kafka producer abstraction who work nice with async await or promises
 */
class KafkaProducer {
  /**
    * Create a instance of KafkaProducer
    * @constructor
    * @param {clientOpts} clientOpts - KafkaClient options from kafka-node
    * @param {producerOpts} producerOpts - Producer options from kafka-node
    * @see https://github.com/SOHU-Co/kafka-node#kafkaclient
    * @see https://github.com/SOHU-Co/kafka-node#producer
    */
  constructor(clientOpts, producerOpts) {
    this.clientOpts = clientOpts || undefined;
    this.producerOpts = producerOpts || undefined;
    this.kafkaClient = new kafka.KafkaClient(clientOpts);
    this.producer = new kafka.HighLevelProducer(this.kafkaClient, producerOpts);
  }

  /**
   * @function send Send payload to kafka brokers
   * @param {Array<ProduceRequest>} payloads Array of ProduceRequest
   * @returns {Promise} resolve with inserton data or reject with error
   * @see https://github.com/SOHU-Co/kafka-node#producer
   * @async
   */
  async send(payloads) {
    await _connect(this.producer);

    return new Promise((resolve, reject) => {
      this.producer.send(payloads, (error, data) => {
        if (error)
          return reject(error);

        resolve(data);
      });
    });
  }

  /**
   * @function createTopics Create topics on the Kafka server
   * @param {Array<Topic>} topics Array of topics
   * @returns {Promise}
   * @see https://github.com/SOHU-Co/kafka-node#highlevelproducer
   * @async
   */
  async createTopics(topics) {
    await _connect(this.producer);

    return new Promise((resolve, reject) => {
      this.producer.createTopics(topics, true, (error, data) => {
        if (error)
          return reject(error);

        resolve(data);
      });
    });
  }
}

module.exports = KafkaProducer;
