import type { ConfirmChannel } from "amqplib";

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
