
require("dotenv").config();

const fs = require("fs");
const path = require("path");
const YAML = require("yaml");
const express = require("express");
const { Pool } = require("pg");
const { randomUUID } = require("crypto");
const { kafka, producer, publishEvent } = require("../shared/kafka");

const app = express();
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const consumer = kafka.consumer({
  groupId: "flowmaster-orchestrator",
});

const workflowPath = path.join(__dirname, "../workflows/loan-saga.yaml");

function loadWorkflowDefinition() {
  return YAML.parse(fs.readFileSync(workflowPath, "utf8"));
}

async function saveEvent(workflowId, eventType, payload = {}) {
  await pool.query(
    `INSERT INTO workflow_events (workflow_id, event_type, payload)
     VALUES ($1, $2, $3)`,
    [workflowId, eventType, JSON.stringify(payload)]
    
  );
}

async function queueWorkflowEvent(workflowId, eventType, payload = {}) {
  const event = {
    eventId: randomUUID(),
    workflowId,
    eventType,
    payload,
    timestamp: new Date().toISOString(),
  };

  await saveOutboxEvent(event);
}


async function saveOutboxEvent(event) { await pool.query( `INSERT INTO outbox_events (event_id, topic, payload) VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING`, [ event.eventId, "workflow-events", JSON.stringify(event), ] ); }
async function updateStatus(workflowId, status) {
  await pool.query(
    "UPDATE workflows SET status = $1 WHERE id = $2",
    [status, workflowId]
  );

  console.log(`Workflow ${workflowId} -> ${status}`);
}


app.get("/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({
      status: "UP",
      service: "orchestrator",
      database: "connected",
    });
  } catch (error) {
    res.status(500).json({
      status: "DOWN",
      error: error.message,
    });
  }
});


app.post("/workflows", async (req, res) => {
  const { customerName, loanAmount } = req.body;
  const amount = Number(loanAmount);

  if (
    typeof customerName !== "string" ||
    !customerName.trim() ||
    !Number.isFinite(amount) ||
    amount <= 0
  ) {
    return res.status(400).json({
      error: "Valid customerName and positive loanAmount are required",
    });
  }

  const workflowId = randomUUID();

  const input = {
    customerName: customerName.trim(),
    loanAmount: amount,
  };

  const event = {
    eventId: randomUUID(),
    workflowId,
    eventType: "WORKFLOW_CREATED",
    payload: input,
    timestamp: new Date().toISOString(),
  };

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const result = await client.query(
      `INSERT INTO workflows (id, status, input)
       VALUES ($1, 'PENDING', $2)
       RETURNING id, status, input, created_at`,
      [workflowId, JSON.stringify(input)]
    );

    await client.query(
      `INSERT INTO workflow_events
       (workflow_id, event_type, payload)
       VALUES ($1, $2, $3)`,
      [
        workflowId,
        "WORKFLOW_CREATED",
        JSON.stringify(input),
      ]
    );

    await client.query(
      `INSERT INTO outbox_events (event_id, topic, payload)
       VALUES ($1, $2, $3)
       ON CONFLICT (event_id) DO NOTHING`,
      [
        event.eventId,
        "workflow-events",
        JSON.stringify(event),
      ]
    );

    await client.query("COMMIT");

    res.status(201).json(result.rows[0]);
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    console.error("Workflow creation failed:", error);

    res.status(500).json({
      error: "Could not create workflow",
    });
  } finally {
    client.release();
  }
});

async function publishPendingOutboxEvents() {
  const result = await pool.query(
    `SELECT id, event_id, topic, payload
     FROM outbox_events
     WHERE published_at IS NULL
     ORDER BY created_at
     LIMIT 50`
  );

  for (const row of result.rows) {
    try {
      await publishEvent(row.topic, row.payload);

      await pool.query(
        `UPDATE outbox_events
         SET published_at = NOW(),
             attempts = attempts + 1
         WHERE id = $1`,
        [row.id]
      );
    } catch (error) {
      await pool.query(
        `UPDATE outbox_events
         SET attempts = attempts + 1
         WHERE id = $1`,
        [row.id]
      ).catch(() => {});

      console.error(
        `Outbox event ${row.event_id} publish failed:`,
        error.message
      );
    }
  }
}

app.get("/workflows/:id", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM workflows WHERE id = $1",
      [req.params.id]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: "Workflow not found" });
    }

    res.json(result.rows[0]);
  } catch (error) {
    console.error("Workflow lookup failed:", error.message);
    res.status(500).json({ error: "Could not retrieve workflow" });
  }
});

async function handleEvent(event) {
  const { workflowId, eventType, payload = {} } = event;

  if (!workflowId || !eventType) return;

  const workflowResult = await pool.query(
    "SELECT input, status FROM workflows WHERE id = $1",
    [workflowId]
  );

  if (workflowResult.rowCount === 0) return;

  const workflow = workflowResult.rows[0];
  const input = workflow.input;

  
  if (eventType === "WORKFLOW_CREATED") {
    if (workflow.status !== "PENDING") return;

    await updateStatus(workflowId, "VERIFYING");

    await queueWorkflowEvent(workflowId, "START_VERIFICATION", input);

    return;
  }
  if (eventType === "CUSTOMER_VERIFIED") {
    if (workflow.status !== "VERIFYING") return;

    await saveEvent(workflowId, eventType, payload);
    await updateStatus(workflowId, "CHECKING_CREDIT");

    await publishEvent("workflow-events", {
      eventId: randomUUID(),
      workflowId,
      eventType: "START_CREDIT_CHECK",
      payload: input,
      timestamp: new Date().toISOString(),
    });

    return;
  }

  if (eventType === "CREDIT_APPROVED") {
    if (
      workflow.status !== "CHECKING_CREDIT" &&
      workflow.status !== "VERIFYING"
    ) return;

    await saveEvent(workflowId, eventType, payload);
    await updateStatus(workflowId, "APPROVED");
    return;
  }

  if (eventType === "CREDIT_REJECTED") {
    if (workflow.status !== "CHECKING_CREDIT") return;

    await saveEvent(workflowId, eventType, payload);
    await updateStatus(workflowId, "COMPENSATING");

  await queueWorkflowEvent(workflowId, "START_CREDIT_CHECK", input);

    return;
  }

  if (eventType === "VERIFICATION_COMPENSATED") {
    if (workflow.status !== "COMPENSATING") return;

    await saveEvent(workflowId, eventType, payload);
    await updateStatus(workflowId, "REJECTED");
  }
}


async function recoverIncompleteWorkflows() {
  console.log("Checking for incomplete workflows...");

  const result = await pool.query(`
    SELECT id, status, input
    FROM workflows
    WHERE status IN (
      'PENDING',
      'VERIFYING',
      'CHECKING_CREDIT',
      'COMPENSATING'
    )
    ORDER BY created_at
  `);

  for (const workflow of result.rows) {
    let eventType;

    switch (workflow.status) {
      case "PENDING":
        eventType = "WORKFLOW_CREATED";
        break;

      case "VERIFYING":
        eventType = "START_VERIFICATION";
        break;

      case "CHECKING_CREDIT":
        eventType = "START_CREDIT_CHECK";
        break;

      case "COMPENSATING":
        eventType = "COMPENSATE_VERIFICATION";
        break;

      default:
        continue;
    }

    console.log(
      `Recovering workflow ${workflow.id} from ${workflow.status}`
    );

    await publishEvent("workflow-events", {
      eventId: randomUUID(),
      workflowId: workflow.id,
      eventType,
      payload: workflow.input,
      timestamp: new Date().toISOString(),
    });

    await saveEvent(workflow.id, "RECOVERY_TRIGGERED", {
      previousStatus: workflow.status,
      resumedWith: eventType,
    });
  }

  console.log(
    `Recovery scan complete. Found ${result.rowCount} incomplete workflow(s).`
  );
}

async function start() {
  const definition = loadWorkflowDefinition();

  console.log("Loaded workflow:", definition.name);
  console.log("Workflow steps:", definition.steps.map(step => step.name));

  await pool.query(`
    CREATE TABLE IF NOT EXISTS workflows (
      id UUID PRIMARY KEY,
      status VARCHAR(30) NOT NULL,
      input JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS workflow_events (
      id BIGSERIAL PRIMARY KEY,
      workflow_id UUID NOT NULL REFERENCES workflows(id),
      event_type VARCHAR(100) NOT NULL,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

    await pool.query(`
    CREATE TABLE IF NOT EXISTS outbox_events (
      id BIGSERIAL PRIMARY KEY,
      event_id UUID NOT NULL UNIQUE,
      topic VARCHAR(255) NOT NULL,
      payload JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      published_at TIMESTAMPTZ,
      attempts INTEGER NOT NULL DEFAULT 0
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox_events (created_at)
    WHERE published_at IS NULL
  `);

  await producer.connect();
  await consumer.connect();
  setInterval(() => {
  publishPendingOutboxEvents().catch(error => {
    console.error("Outbox publisher error:", error.message);
  });
}, 1000);

  await consumer.subscribe({
    topic: "workflow-events",
    fromBeginning: false,
  });
await recoverIncompleteWorkflows();

  await consumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;

      try {
        await handleEvent(JSON.parse(message.value.toString()));
      } catch (error) {
        console.error("Orchestrator event failed:", error.message);
        throw error;
      }
    },
  });

  app.listen(process.env.PORT || 3000, () => {
    console.log("Orchestrator running on port", process.env.PORT || 3000);
  });
}

start().catch(error => {
  console.error("Orchestrator startup failed:", error);
  process.exit(1);
});