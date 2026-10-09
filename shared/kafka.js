
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "flowmaster",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const producer = kafka.producer();

async function publishEvent(topic, event) {
  console.log("Publishing Kafka event:", event.eventType);

  try {
    const result = await producer.send({
      topic,
      messages: [
        {
          key: event.workflowId || "system",
          value: JSON.stringify(event),
        },
      ],
    });

    console.log("Kafka publish result:", result);
    return result;
  } catch (error) {
    console.error("Kafka publish failed:", error.message);
    throw error;
  }
}

module.exports = {
  kafka,
  producer,
  publishEvent,
};