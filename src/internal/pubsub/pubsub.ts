import type { ConfirmChannel, ChannelModel, Channel, Replies } from "amqplib";

type SimpleQueueType = "DURABLE" | "TRANSIENT";

export async function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T
): Promise<void> {
  const jsonString = JSON.stringify(value);

  const jsonBytes = Buffer.from(jsonString);

  await ch.publish(exchange, routingKey, jsonBytes, {
    contentType: "application/json",
  });
}

export async function declareAndBind(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType
): Promise<[Channel, Replies.AssertQueue]> {
  const channel = await conn.createConfirmChannel();
  console.log("Confirm channel created.");

  const queue = await channel.assertQueue(queueName, {
    durable: queueType === "DURABLE",
    autoDelete: queueType === "TRANSIENT",
    exclusive: queueType === "TRANSIENT",
  });

  await channel.bindQueue(queue.queue, exchange, key);

  return [channel, queue];
}
