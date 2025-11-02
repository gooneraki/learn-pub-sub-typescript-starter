import amqp from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { SimpleQueueType, subscribeJSON } from "../internal/pubsub/pubsub.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import {
  GameState,
  type PlayingState,
} from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";

async function main() {
  console.log("Starting Peril client...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";

  const conn = await amqp.connect(rabbitConnString);
  console.log("Peril game client connected to RabbitMQ!");

  const username = await clientWelcome();

  const gs = new GameState(username);

  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.TRANSIENT,
    handlerPause(gs)
  );

  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }
    const command = words[0];
    if (command === "move") {
      try {
        commandMove(gs, words);
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "status") {
      commandStatus(gs);
    } else if (command === "spawn") {
      try {
        commandSpawn(gs, words);
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "help") {
      printClientHelp();
    } else if (command === "quit") {
      printQuit();
      process.exit(0);
    } else if (command === "spam") {
      console.log("Spamming not allowed yet!");
    } else {
      console.log("Unknown command");
      continue;
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

function handlerPause(gs: GameState): (ps: PlayingState) => void {
  return function (ps: PlayingState) {
    if (ps.isPaused) {
      console.log();
      console.log("==== Pause Detected ====");
      gs.pauseGame();
      console.log("------------------------");
    } else {
      console.log();
      console.log("==== Resume Detected ====");
      gs.resumeGame();
      console.log("------------------------");
    }
    console.log("> ");
  };
}
