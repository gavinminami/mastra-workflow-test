import { PubSub, Subscription } from "@google-cloud/pubsub";
import { EventEmitter } from "events";

export interface JobResult {
  jobId: string;
  jobType: string;
  success: boolean;
  result?: any;
  error?: string;
}

type ResultHandler = (result: JobResult) => Promise<void>;

// 30 days in seconds
const DEFAULT_EXPIRATION_SECONDS = 30 * 24 * 60 * 60;

export class JobResultProcessor extends EventEmitter {
  private pubsub: PubSub;
  private subscription: Subscription;
  private isShuttingDown: boolean = false;

  constructor(
    pubsub: PubSub,
    resultsTopicName: string,
    subscriptionName?: string,
    expirationTimeSeconds: number = DEFAULT_EXPIRATION_SECONDS
  ) {
    super();
    this.pubsub = pubsub;

    // Create a unique subscription name if not provided
    const uniqueSubscriptionName =
      subscriptionName ||
      `${resultsTopicName}-results-${Date.now()}-${Math.random().toString(36).substring(2, 15)}`;

    // Create or get the subscription
    this.subscription = this.pubsub.subscription(uniqueSubscriptionName);

    // Ensure the subscription exists with expiration policy
    console.log(
      `Creating subscription ${uniqueSubscriptionName} on topic ${resultsTopicName}`
    );
    this.pubsub
      .topic(resultsTopicName)
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
    console.log("Cleaning up results subscription...");
    try {
      // Stop receiving messages
      await this.subscription.close();
      // Remove all listeners to prevent memory leaks
      this.subscription.removeAllListeners();
      console.log("Results subscription closed successfully");
    } catch (error) {
      console.error("Error closing results subscription:", error);
    }
  }

  /**
   * Handle incoming result messages from PubSub
   * @param message The message received from PubSub
   */
  private async handleMessage(message: any): Promise<void> {
    try {
      const messageData = Buffer.from(message.data).toString();
      console.log("Received result message:", messageData);

      const result = JSON.parse(messageData) as JobResult;

      // Emit the result event
      this.emit("result", result);
      // Also emit a job-type specific event
      this.emit(`result:${result.jobType}`, result);

      message.ack();
    } catch (error: unknown) {
      console.error("Error processing result message:", error);
      // Only nack the message if it's a parsing error, not a processing error
      if (error instanceof SyntaxError) {
        message.nack();
      } else {
        message.ack();
      }
    }
  }

  /**
   * Start listening for result messages
   */
  public start(): void {
    console.log(`Started JobResultProcessor ${this.subscription.name}`);
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
