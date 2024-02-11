import { Kafka, Message, Partitioners, CompressionTypes, logLevel } from 'kafkajs'
import { v4 as uuid } from 'uuid'

const brokers = process.env.KAFKA_BROKERS ?? 'localhost:9092'

const kafka = new Kafka({
  clientId: 'users-service',
  brokers: brokers.split(','),
  requestTimeout: 55000,
  logLevel: logLevel.ERROR,
  connectionTimeout: 55000,
  retry: {
    factor: 0,
    multiplier: 4,
    maxRetryTime: 55000,
    retries: 10,
  },
})

class KafkaServer {
  static start() {
    console.log('Server started')
  }

  private producer = kafka.producer()
  constructor() {}

  async connect() {
    await this.producer.connect()
  }

  async producerOne<T>(topic: string, value: T) {
    const result = await this.producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [
        {
          value: JSON.stringify(value),
          headers: {
            'x-session-id': uuid(),
            'x-message-type': 'create',
            'x-message-version': '1.0.0',
            'x-message-timestamp': new Date().toISOString(),
            'x-message-status': 'pending',
            'system-id': 'my-system',
          },
        },
      ],
    })

    return result
  }

  async destroy() {
    await this.producer.disconnect()
  }

  static async producer<T>(topic: string, value: T) {
    const producer = kafka.producer({
      allowAutoTopicCreation: true,
      createPartitioner: Partitioners.DefaultPartitioner,
    })
    await producer.connect()

    const result = await producer.send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [
        {
          value: JSON.stringify(value),
          headers: {
            'x-session-id': uuid(),
            'x-message-type': 'create',
            'x-message-version': '1.0.0',
            'x-message-timestamp': new Date().toISOString(),
            'x-message-status': 'pending',
            'system-id': 'my-system',
          },
        },
      ],
    })

    await producer.disconnect()
    return result
  }

  static async multipleProducer<T>(topic: string, values: T[]) {
    const producer = kafka.producer({
      allowAutoTopicCreation: true,
      // transactionalId: uuid(),
      createPartitioner: Partitioners.DefaultPartitioner,
    })

    await producer.connect()

    const messages: Message[] = values.map((value) => ({
      value: Buffer.from(JSON.stringify(value)),
      headers: {
        'x-session-id': uuid(),
        'x-message-type': 'create',
        'x-message-version': '1.0.0',
        'x-message-timestamp': new Date().toISOString(),
        'x-message-status': 'pending',
        'system-id': 'my-system',
      },
    }))

    const result = await producer.send({ compression: CompressionTypes.GZIP, topic, messages })

    await producer.disconnect()
    return result
  }
}

export default KafkaServer
