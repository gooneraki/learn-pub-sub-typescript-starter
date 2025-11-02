import type { ConfirmChannel, ChannelModel, Channel, Replies } from "amqplib";

export enum SimpleQueueType {
  "DURABLE",
  "TRANSIENT",
}

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
    durable: queueType === SimpleQueueType.DURABLE,
    autoDelete: queueType === SimpleQueueType.TRANSIENT,
    exclusive: queueType === SimpleQueueType.TRANSIENT,
  });

  await channel.bindQueue(queue.queue, exchange, key);

  return [channel, queue];
}

export async function subscribeJSON<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType, // an enum to represent "durable" or "transient"
  handler: (data: T) => void
): Promise<void> {
  const [channel, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType
  );

  await channel.consume(queueName, (msg) => {
    if (!msg) return;
    const parsedData = JSON.parse(msg.content.toString());

    handler(parsedData);

    channel.ack(msg);
  });
}
