import { PubSub, Subscription } from "@google-cloud/pubsub";
import { JobResult, JobSubmission } from "./types";

type JobHandler = (...args: any[]) => Promise<any>;

export interface JobMessage {
  jobId: string;
  jobType: string;
  arguments: any[];
}

// 30 days in seconds
const DEFAULT_EXPIRATION_SECONDS = 30 * 24 * 60 * 60;

export class JobServer {
  private pubsub: PubSub;
  private subscription: Subscription;
  private resultsTopic: string;
  private handlers: Map<string, JobHandler>;
  private isShuttingDown: boolean = false;

  constructor(
    pubsub: PubSub,
    topicName: string,
    resultsTopicName: string,
    subscriptionName?: string,
    expirationTimeSeconds: number = DEFAULT_EXPIRATION_SECONDS
  ) {
    this.pubsub = pubsub;
    this.handlers = new Map();
    this.resultsTopic = resultsTopicName;

    // Create a unique subscription name if not provided
    const uniqueSubscriptionName =
      subscriptionName ||
      `${topicName}-${Date.now()}-${Math.random().toString(36).substring(2, 15)}`;

    // Create or get the subscription
    this.subscription = this.pubsub.subscription(uniqueSubscriptionName);

    // Ensure the subscription exists with expiration policy
    this.pubsub
      .topic(topicName)
      .createSubscription(uniqueSubscriptionName, {
        expirationPolicy: {
          ttl: {
            seconds: expirationTimeSeconds,
          },
        },
      })
      .catch((error) => {
        // If subscription already exists, that's fine
        if (error.code !== 6) {
          // 6 is the error code for ALREADY_EXISTS
          console.error("Error creating subscription:", error);
        }
      });

    // Set up error handler
    this.subscription.on("error", (error) => {
      console.error("Subscription error:", error);
    });

    // Set up process exit handlers
    this.setupExitHandlers();
  }

  private setupExitHandlers(): void {
    // Handle uncaught exceptions
    process.on("uncaughtException", async (error) => {
      console.error("Uncaught exception:", error);
      await this.cleanup();
      process.exit(1);
    });

    // Handle unhandled promise rejections
    process.on("unhandledRejection", async (reason) => {
      console.error("Unhandled rejection:", reason);
      await this.cleanup();
      process.exit(1);
    });
  }

  private async cleanup(): Promise<void> {
    if (this.isShuttingDown) {
      return;
    }
    this.isShuttingDown = true;
    console.log("Cleaning up subscription...");
    try {
      // Stop receiving messages
      await this.subscription.close();
      // Remove all listeners to prevent memory leaks
      this.subscription.removeAllListeners();
      console.log("Subscription closed successfully");
    } catch (error) {
      console.error("Error closing subscription:", error);
    }
  }

  /**
   * Register a handler for a specific job type
   * @param jobType The type of job this handler will process
   * @param handler The function that will process the job
   */
  public registerHandler(jobType: string, handler: JobHandler): void {
    this.handlers.set(jobType, handler);
  }

  /**
   * Publish a job result to the results queue
   * @param result The job result to publish
   */
  private async publishResult(result: JobResult): Promise<void> {
    console.log(`Publishing result to ${this.resultsTopic}`);
    console.log(JSON.stringify(result, null, 2));
    try {
      const messageBuffer = Buffer.from(JSON.stringify(result));
      await this.pubsub.topic(this.resultsTopic).publishMessage({
        data: messageBuffer,
      });
    } catch (error) {
      console.error("Error publishing result:", error);
    }
  }

  /**
   * Handle incoming messages from PubSub
   * @param message The message received from PubSub
   */
  private async handleMessage(message: any): Promise<void> {
    try {
      console.log(
        "Received message:",
        JSON.stringify(Buffer.from(message.data).toString(), null, 2)
      );
      const data = JSON.parse(
        Buffer.from(message.data).toString()
      ) as JobMessage;
      const handler = this.handlers.get(data.jobType);

      if (!handler) {
        console.warn(`No handler registered for job type: ${data.jobType}`);
        console.log(message);
        // Ack the message since retrying won't help if there's no handler
        await this.publishResult({
          jobId: data.jobId,
          jobType: data.jobType,
          success: false,
          error: `No handler registered for job type: ${data.jobType}`,
        });
        message.ack();
        return;
      }

      try {
        console.log("Executing handler:", JSON.stringify(data, null, 2));
        const result = await handler(data);
        await this.publishResult({
          jobId: data.jobId,
          jobType: data.jobType,
          success: true,
          result,
        });
        message.ack();
      } catch (error: unknown) {
        console.error("Error executing handler:", error);
        await this.publishResult({
          jobId: data.jobId,
          jobType: data.jobType,
          success: false,
          error: error instanceof Error ? error.message : String(error),
        });
        // Only nack if the error might be temporary
        if (
          (error instanceof Error &&
            (error.message.includes("temporary") ||
              error.message.includes("retry"))) ||
          error instanceof SyntaxError
        ) {
          message.nack();
        } else {
          // For permanent errors, ack the message to prevent infinite retries
          message.ack();
        }
      }
    } catch (error: unknown) {
      console.error("Error processing message:", error);
      message.ack();
    }
  }

  /**
   * Start listening for messages
   */
  public start(): void {
    // Remove any existing message handlers to prevent duplicates
    this.subscription.removeListener("message", this.handleMessage.bind(this));
    // Add the message handler
    this.subscription.on("message", this.handleMessage.bind(this));
  }

  /**
   * Stop listening for messages and cleanup
   */
  public async stop(): Promise<void> {
    // Remove the message handler
    this.subscription.removeListener("message", this.handleMessage.bind(this));
    await this.cleanup();
  }
}
