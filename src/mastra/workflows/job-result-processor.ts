import { PubSub, Subscription } from "@google-cloud/pubsub";

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

export class JobResultProcessor {
  private pubsub: PubSub;
  private subscription: Subscription;
  private handlers: Map<string, ResultHandler>;
  private isShuttingDown: boolean = false;

  constructor(
    pubsub: PubSub,
    resultsTopicName: string,
    subscriptionName?: string,
    expirationTimeSeconds: number = DEFAULT_EXPIRATION_SECONDS
  ) {
    this.pubsub = pubsub;
    this.handlers = new Map();

    // Create a unique subscription name if not provided
    const uniqueSubscriptionName =
      subscriptionName ||
      `${resultsTopicName}-results-${Date.now()}-${Math.random().toString(36).substring(2, 15)}`;

    // Create or get the subscription
    this.subscription = this.pubsub.subscription(uniqueSubscriptionName);

    // Ensure the subscription exists with expiration policy
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
   * Register a handler for a specific job type's results
   * @param jobType The type of job whose results this handler will process
   * @param handler The function that will process the results
   */
  public registerHandler(jobType: string, handler: ResultHandler): void {
    this.handlers.set(jobType, handler);
  }

  /**
   * Unregister a handler for a specific job type's results
   * @param jobType The type of job whose results handler will be removed
   * @param handler The handler function to remove
   */
  public unregisterHandler(jobType: string, handler: ResultHandler): void {
    const currentHandler = this.handlers.get(jobType);
    if (currentHandler === handler) {
      this.handlers.delete(jobType);
    }
  }

  /**
   * Handle incoming result messages from PubSub
   * @param message The message received from PubSub
   */
  private async handleMessage(message: any): Promise<void> {
    try {
      console.log(
        "Received result message:",
        JSON.stringify(Buffer.from(message.data).toString(), null, 2)
      );
      const result = JSON.parse(
        Buffer.from(message.data).toString()
      ) as JobResult;
      const handler = this.handlers.get(result.jobType);

      if (!handler) {
        console.warn(
          `No handler registered for job type results: ${result.jobType}`
        );
        // Ack the message since retrying won't help if there's no handler
        message.ack();
        return;
      }

      try {
        await handler(result);
        message.ack();
      } catch (error: unknown) {
        console.error("Error processing result:", error);
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
      console.error("Error processing result message:", error);
      message.ack();
    }
  }

  /**
   * Start listening for result messages
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
