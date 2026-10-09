
require("dotenv").config();

const express = require("express");
const { randomUUID } = require("crypto");
const { kafka, producer, publishEvent } = require("../shared/kafka");

const app = express();
app.use(express.json());

const consumer = kafka.consumer({
  groupId: "flowmaster-credit",
});

app.get("/health", (req, res) => {
  res.json({
    status: "UP",
    service: "credit-service",
  });
});

async function handleEvent(event) {
  const { workflowId, eventType, payload = {} } = event;

  if (!workflowId || eventType !== "START_CREDIT_CHECK") {
    return;
  }

  const customerName = payload.customerName;
  const loanAmount = Number(payload.loanAmount);

  if (
    !customerName ||
    !Number.isFinite(loanAmount) ||
    loanAmount <= 0
  ) {
    console.error("Invalid credit-check data:", workflowId);
    return;
  }

  // Demo rule: loan amounts up to 100000 are approved.
  const approved = loanAmount <= 100000;

  await publishEvent("workflow-events", {
    eventId: randomUUID(),
    workflowId,
    eventType: approved ? "CREDIT_APPROVED" : "CREDIT_REJECTED",
    payload: {
      customerName,
      loanAmount,
      status: approved ? "APPROVED" : "REJECTED",
    },
    timestamp: new Date().toISOString(),
  });

  console.log(
    `Credit check ${approved ? "approved" : "rejected"}: ${workflowId}`
  );
}

async function start() {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({
    topic: "workflow-events",
    fromBeginning: false,
  });

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      try {
        const event = JSON.parse(message.value.toString());
        await handleEvent(event);
      } catch (error) {
        console.error("Credit event failed:", error.message);
        throw error;
      }
    },
  });

  const PORT = process.env.CREDIT_PORT || 3002;

  app.listen(PORT, () => {
    console.log(`Credit Service running on port ${PORT}`);
    console.log("Listening for Kafka credit-check events");
  });
}

start().catch((error) => {
  console.error("Credit Service failed to start:", error);
  process.exit(1);
});