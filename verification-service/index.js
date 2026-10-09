
require("dotenv").config();

const express = require("express");
const { randomUUID } = require("crypto");
const { kafka, producer, publishEvent } = require("../shared/kafka");

const app = express();
app.use(express.json());

const consumer = kafka.consumer({
  groupId: "flowmaster-verification",
});

app.get("/health", (req, res) => {
  res.json({
    status: "UP",
    service: "verification-service",
  });
});

async function handleEvent(event) {
  const { workflowId, eventType, payload = {} } = event;

  if (!workflowId || !eventType) return;

  if (eventType === "START_VERIFICATION") {
    const { customerName, loanAmount } = payload;

    if (
      !customerName ||
      !Number.isFinite(Number(loanAmount)) ||
      Number(loanAmount) <= 0
    ) {
      console.error("Invalid verification data:", workflowId);
      return;
    }

    await publishEvent("workflow-events", {
      eventId: randomUUID(),
      workflowId,
      eventType: "CUSTOMER_VERIFIED",
      payload: {
        customerName,
        loanAmount: Number(loanAmount),
      },
      timestamp: new Date().toISOString(),
    });

    console.log(`Customer verified: ${workflowId}`);
  }

  if (eventType === "COMPENSATE_VERIFICATION") {
    await publishEvent("workflow-events", {
      eventId: randomUUID(),
      workflowId,
      eventType: "VERIFICATION_COMPENSATED",
      payload: {
        customerName: payload.customerName,
        loanAmount: payload.loanAmount,
        message: "Verification compensation completed",
      },
      timestamp: new Date().toISOString(),
    });

    console.log(`Verification compensated: ${workflowId}`);
  }
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
        console.error("Verification event failed:", error.message);
        throw error;
      }
    },
  });

  const PORT = process.env.VERIFICATION_PORT || 3001;

  app.listen(PORT, () => {
    console.log(`Verification Service running on port ${PORT}`);
    console.log("Listening for Kafka verification events");
  });
}

start().catch((error) => {
  console.error("Verification Service failed to start:", error);
  process.exit(1);
});