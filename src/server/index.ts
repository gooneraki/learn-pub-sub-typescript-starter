import amqp from "amqplib";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { publishJSON } from "../internal/pubsub/pubsub.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Peril game server connected to RabbitMQ!");

  const channel = await conn.createConfirmChannel();
  console.log("Confirm channel created.");

  const koko: PlayingState = { isPaused: true };

  await publishJSON(channel, ExchangePerilDirect, PauseKey, koko);

  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await conn.close();
        console.log("RabbitMQ connection closed.");
      } catch (err) {
        console.error("Error closing RabbitMQ connection:", err);
      } finally {
        process.exit(0);
      }
    })
  );
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
