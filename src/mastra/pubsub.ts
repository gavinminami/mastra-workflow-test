import { Message, PubSub } from "@google-cloud/pubsub";

/**
 * Creates and returns a Google Cloud Pub/Sub client instance.
 * The client will use the default credentials from the environment.
 *
 * @returns {PubSub} A configured Pub/Sub client instance
 */
export function createPubSubClient(): PubSub {
  return new PubSub();
}

/**
 * Creates and returns a Google Cloud Pub/Sub client instance with custom project ID.
 *
 * @param {string} projectId - The Google Cloud project ID
 * @returns {PubSub} A configured Pub/Sub client instance
 */
export function createPubSubClientWithProject(projectId: string): PubSub {
  return new PubSub({
    projectId,
  });
}

// pull messages from pubsub
export async function startPubSubSubscription(
  pubsub: PubSub,
  subscriptionName: string,
  callback: (message: Message) => void | Promise<void>
) {
  const subscription = pubsub.subscription(subscriptionName);
  subscription.on("message", async (message) => {
    message.ack();
    callback(message);
  });
}
