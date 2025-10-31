import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind } from "../internal/pubsub/pubsub.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  console.log("Starting Peril client...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";

  const conn = await amqp.connect(rabbitConnString);
  console.log("Peril game client connected to RabbitMQ!");

  const username = await clientWelcome();

  // Declare and bind a transient queue for receiving pause messages
  const [channel, queue] = await declareAndBind(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    "TRANSIENT"
  );
  console.log(
    `Queue ${queue.queue} declared and bound to ${ExchangePerilDirect} exchange`
  );
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
