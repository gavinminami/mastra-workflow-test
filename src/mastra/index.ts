import { Mastra } from "@mastra/core/mastra";
import { createLogger } from "@mastra/core/logger";
import { LibSQLStore } from "@mastra/libsql";
import { weatherWorkflow } from "./workflows";
import { weatherAgent } from "./agents";
import { registerApiRoute } from "@mastra/core/server";
import { PubSub } from "@google-cloud/pubsub";
import { JobMessage, JobProcessor } from "./workflows/job-processor";
import { JobResultProcessor } from "./workflows/job-result-processor";
import { v4 as uuidv4 } from "uuid";

const pubsub = process.env.PUBSUB_CREDENTIALS
  ? new PubSub({
      credentials: JSON.parse(process.env.PUBSUB_CREDENTIALS),
    })
  : new PubSub();

export const mastra = new Mastra({
  workflows: { weatherWorkflow },
  agents: { weatherAgent },
  storage: new LibSQLStore({
    // stores telemetry, evals, ... into memory storage, if it needs to persist, change to file:../mastra.db
    url: ":memory:",
  }),
  logger: createLogger({
    name: "Mastra",
    level: "info",
  }),
  server: {
    middleware: [
      // Add a global request logger
      {
        handler: async (c, next) => {
          console.log(`workflow call: ${c.req.method} ${c.req.url}`);
          if (c.req.method === "POST") {
            const runId = c.req.query("runId");
            const workflowName = c.req.path.split("/")[3] as "weatherWorkflow";
            console.log({ runId, workflowName });
            const workflow = mastra.getWorkflow(workflowName);
            const firstStep = workflow.stepGraph.initial[0].step;
            console.log({ firstStep });
            const message = {
              jobId: uuidv4(),
              jobType: firstStep?.id,
              arguments: [],
              runId,
              workflowName,
              triggerData: await c.req.json(),
              stepId: firstStep?.id,
            };
            console.log(
              "Publishing message:",
              JSON.stringify(message, null, 2)
            );
            await pubsub.topic("gavin-workflow-test").publishMessage({
              data: Buffer.from(JSON.stringify(message)),
            });
          }
          await next();
        },
        path: "/api/workflows/*/start",
      },
    ],
    apiRoutes: [
      registerApiRoute("/run-workflow-step/:stepId", {
        method: "GET",
        handler: async (c) => {
          console.log("here");
          const mastra = c.get("mastra");

          const workflow = mastra.getWorkflow("weatherWorkflow");
          console.log(workflow.steps);
          const stepId = c.req.param("stepId");
          console.log({ stepId });

          const step = workflow.steps[stepId];

          if (!step) {
            return c.json({ error: "Step not found" }, 404);
          }

          console.log({ step });

          // const agents = await mastra.getAgent("my-agent");

          // publish a message to pubsub
          pubsub.topic("gavin-workflow-test").publishMessage({
            data: Buffer.from("Hello, world!"),
          });
          return c.json({ message: "Hello, world!" });
        },
      }),
    ],
  },
});

const jobProcessor = new JobProcessor(
  pubsub,
  "gavin-workflow-test",
  "gavin-workflow-test-results",
  "gavin-workflow-test-subscriber",
  24 * 60 * 60
);
jobProcessor.registerHandler("fetch-weather", async (job: JobMessage) => {
  console.log("fetch-weather", job);
  return {
    jobId: job.jobId,
  };
});

jobProcessor.registerHandler("plan-activities", async (job: JobMessage) => {
  console.log("plan-activities", job);
  return {
    jobId: job.jobId,
  };
});

jobProcessor.start();

const jobResultProcessor = new JobResultProcessor(
  pubsub,
  "gavin-workflow-test-results",
  "gavin-workflow-test-results-subscriber",
  24 * 60 * 60
);
jobResultProcessor.start();

// startPubSubSubscription(
//   pubsub,
//   "gavin-workflow-test-subscriber",
//   async (message) => {
//     console.log(`Received message: ${message.data}`);
//     const { runId, workflowName, triggerData, stepId } = JSON.parse(
//       message.data.toString()
//     );
//     console.log({ runId, workflowName, triggerData, stepId });
//     const workflow = mastra.getWorkflow(workflowName);
//     console.log({ workflow });
//     console.log(workflow.steps);
//     const step = workflow.steps[stepId];
//     console.log({ step });
//     console.log({ triggerData });

//     const result = await step.execute({
//       steps: {},
//       triggerData: {
//         city: "90275",
//       },
//       attempts: {
//         "fetch-weather": 0,
//         "plan-activities": 0,
//       },
//       inputData: {},
//     });
//     // console.log({ result });
//     // const step = workflow.steps[stepId];
//     // console.log({ step });
//     // const result = await step.execute({
//     //   context: {
//     //     runId,
//     //     workflowName,
//     //   },
//     // });
//     // console.log({ result });
//   }
// );

mastra.getServer();
