import { Mastra } from "@mastra/core/mastra";
import { createLogger } from "@mastra/core/logger";
import { LibSQLStore } from "@mastra/libsql";
import { weatherWorkflow } from "./workflows";
import { weatherAgent } from "./agents";
import { registerApiRoute } from "@mastra/core/server";
import { PubSub } from "@google-cloud/pubsub";
import { JobServer } from "./jobs/job-server";
import { JobClient } from "./jobs/job-client";
import { JobResult, JobSubmission, JobArgument } from "./jobs/types";

const pubsub = process.env.PUBSUB_CREDENTIALS
  ? new PubSub({
      credentials: JSON.parse(process.env.PUBSUB_CREDENTIALS),
    })
  : new PubSub();

const pubsubTopic = process.env.PUBSUB_TOPIC || "gavin-workflow-test";
const pubsubSubscription =
  process.env.PUBSUB_SUBSCRIPTION || "gavin-workflow-test-subscriber";
const pubsubResultsTopic =
  process.env.PUBSUB_RESULTS_TOPIC || "gavin-workflow-test-results";
const pubsubResultsSubscription =
  process.env.PUBSUB_RESULTS_SUBSCRIPTION ||
  "gavin-workflow-test-results-subscriber";

type MyJobSpec = JobSubmission & {
  runId: string;
  workflowName: string;
  triggerData: any;
  stepId: string;
};

type MyJobResult = JobResult & {
  xyz: string;
};

const jobClient = new JobClient(
  pubsub,
  pubsubTopic,
  pubsubResultsTopic,
  pubsubResultsSubscription
).start();

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
              jobType: firstStep?.id,
              args: [{ name: "arg1", value: "test" }],
              runId: runId || "test",
              workflowName,
              triggerData: await c.req.json(),
              stepId: firstStep?.id,
            };

            const result = await jobClient.submitAndWaitForResult(message);
            console.log(JSON.stringify(result, null, 2));
            // const jobId = await jobProcessorClient.submitJob(message);
            // console.log(`Submitted job ${jobId}`);
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
          pubsub.topic(pubsubTopic).publishMessage({
            data: Buffer.from("Hello, world!"),
          });
          return c.json({ message: "Hello, world!" });
        },
      }),
      registerApiRoute("/test-job-processor", {
        method: "GET",
        handler: async (c) => {
          const message = {
            jobType: "test-job",
            args: [{ name: "arg1", value: "test" }],
            runId: "test",
            workflowName: "test-workflow",
            triggerData: { hello: "world" },
            stepId: "test-step",
          };

          const result = await jobClient.submitAndWaitForResult<
            MyJobSpec,
            MyJobResult
          >(message);
          console.log(JSON.stringify(result, null, 2));

          if (result.error) {
            return c.json({ error: result.error }, 500);
          }

          return c.json({ hello: result.retval });
        },
      }),
    ],
  },
});

const jobProcessor = new JobServer(
  pubsub,
  "gavin-workflow-test",
  "gavin-workflow-test-results",
  "gavin-workflow-test-subscriber",
  24 * 60 * 60
).registerHandler("test-job", async (arg1?: JobArgument) => {
  console.log("processing test-job", arg1);

  // add a delay
  await new Promise((resolve) => setTimeout(resolve, 1000));

  return "hello" + (arg1?.value || "asdf");
});

jobProcessor.start();

mastra.getServer();
