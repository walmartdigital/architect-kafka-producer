const KafkaProducer = require('./kafka-producer')

module.exports = function (options, imports, register) {

    register(null, {
       KafkaProducer
    });
}