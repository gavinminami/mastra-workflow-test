import { PubSub } from "@google-cloud/pubsub";
import { JobResultProcessor, JobResult } from "./job-result-processor";
import { v4 as uuidv4 } from "uuid";

export interface JobSubmission {
  jobType: string;
  arguments: any[];
}

export class JobProcessorClient {
  private pubsub: PubSub;
  private topicName: string;
  private jobResultProcessor: JobResultProcessor;

  constructor(
    pubsub: PubSub,
    topicName: string,
    resultsTopicName: string,
    resultsSubscriptionName?: string
  ) {
    this.pubsub = pubsub;
    this.topicName = topicName;
    this.jobResultProcessor = new JobResultProcessor(
      pubsub,
      resultsTopicName,
      resultsSubscriptionName
    );
  }

  /**
   * Submit a job to the queue and return the job ID
   * @param submission The job submission details
   * @returns The ID of the submitted job
   */
  public async submitJob(submission: JobSubmission): Promise<string> {
    const jobId = uuidv4();
    const message = {
      jobId,
      ...submission,
    };

    const messageBuffer = Buffer.from(JSON.stringify(message));
    await this.pubsub.topic(this.topicName).publishMessage({
      data: messageBuffer,
    });

    return jobId;
  }

  /**
   * Submit a job and wait for its result
   * @param submission The job submission details
   * @param timeoutMs Optional timeout in milliseconds (default: 5 minutes)
   * @returns The job result
   * @throws Error if the job times out or fails
   */
  public async submitAndWaitForResult(
    submission: JobSubmission,
    timeoutMs: number = 5 * 60 * 1000
  ): Promise<JobResult> {
    const jobId = await this.submitJob(submission);

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error(`Job ${jobId} timed out after ${timeoutMs}ms`));
      }, timeoutMs);

      const handler = async (result: JobResult) => {
        if (result.jobId === jobId) {
          cleanup();
          if (result.success) {
            resolve(result);
          } else {
            reject(new Error(result.error || "Job failed"));
          }
        }
      };

      const cleanup = () => {
        clearTimeout(timeout);
        this.jobResultProcessor.unregisterHandler(submission.jobType, handler);
      };

      this.jobResultProcessor.registerHandler(submission.jobType, handler);
    });
  }

  /**
   * Start the client
   */
  public start(): void {
    this.jobResultProcessor.start();
  }

  /**
   * Stop the client
   */
  public async stop(): Promise<void> {
    await this.jobResultProcessor.stop();
  }
}
