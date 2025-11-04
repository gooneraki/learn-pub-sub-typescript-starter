import type { ConfirmChannel, ChannelModel, Channel, Replies } from "amqplib";

export enum SimpleQueueType {
  "DURABLE",
  "TRANSIENT",
}
export enum AckType {
  "Ack",
  "NackRequeue",
  "NackDiscard",
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
    arguments: {
      "x-dead-letter-exchange": "peril_dlx",
    },
  });

  await channel.bindQueue(queue.queue, exchange, key);

  return [channel, queue];
}

export async function subscribeJSON<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => AckType
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

    const ackResult = handler(parsedData);

    switch (ackResult) {
      case AckType.Ack:
        channel.ack(msg);
        console.log("Message Acknowledged");
        return;

      case AckType.NackDiscard:
        channel.nack(msg, false, false);
        console.log("Message discarded");
        return;

      case AckType.NackRequeue:
        channel.nack(msg, false, true);
        console.log("Message requeued");
        return;

      default:
        throw new Error(`Unknown acktype ${ackResult}`);
    }
  });
}
