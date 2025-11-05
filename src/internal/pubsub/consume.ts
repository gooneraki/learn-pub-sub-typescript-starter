import type { ChannelModel } from "amqplib";
import { AckType, declareAndBind, SimpleQueueType } from "./pubsub.js";
import { decode } from "@msgpack/msgpack";

export async function subscribeJSON<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data: Buffer) => JSON.parse(data.toString())
  );
}

export async function subscribeMsgPack<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data: Buffer) => decode(data) as T
  );
}

export async function subscribe<T>(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  unmarshaller: (data: Buffer) => T
): Promise<void> {
  const [channel, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    routingKey,
    simpleQueueType
  );

  await channel.consume(queueName, async (msg) => {
    if (!msg) return;
    const parsedData = unmarshaller(msg.content);

    const ackResult = await handler(parsedData);

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
