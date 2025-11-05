import amqp, { type ConfirmChannel } from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import {
  AckType,
  publishJSON,
  publishMsgPack,
  SimpleQueueType,
  subscribeJSON,
} from "../internal/pubsub/pubsub.js";
import {
  ArmyMovesPrefix,
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
  PauseKey,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import {
  GameState,
  type PlayingState,
} from "../internal/gamelogic/gamestate.js";
import {
  commandMove,
  handleMove,
  MoveOutcome,
} from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import type {
  ArmyMove,
  RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

async function main() {
  console.log("Starting Peril client...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";

  const conn = await amqp.connect(rabbitConnString);
  console.log("Peril game client connected to RabbitMQ!");

  const username = await clientWelcome();

  const publishCh = await conn.createConfirmChannel();

  const gs = new GameState(username);

  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.TRANSIENT,
    handlerPause(gs)
  );

  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.TRANSIENT,
    handlerMove(gs, publishCh, username)
  );

  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    "war",
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.DURABLE,
    handlerWar(gs, publishCh, username)
  );

  while (true) {
    const words = await getInput();
    if (words.length === 0) {
      continue;
    }
    const command = words[0];
    if (command === "move") {
      try {
        const move = commandMove(gs, words);
        await publishJSON(
          publishCh,
          ExchangePerilTopic,
          `${ArmyMovesPrefix}.${username}`,
          move
        );
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

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState): AckType => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

function handlerMove(
  gs: GameState,
  publishCh: ConfirmChannel,
  username: string
): (move: ArmyMove) => Promise<AckType> {
  return async function (move: ArmyMove): Promise<AckType> {
    const outcome = handleMove(gs, move);

    if (outcome === MoveOutcome.MakeWar) {
      const recognitionOfWar: RecognitionOfWar = {
        attacker: move.player,
        defender: gs.getPlayerSnap(),
      };
      try {
        await publishJSON(
          publishCh,
          ExchangePerilTopic,
          `${WarRecognitionsPrefix}.${username}`,
          recognitionOfWar
        );
        process.stdout.write("> ");
        return AckType.Ack;
      } catch {
        process.stdout.write("> ");
        return AckType.NackRequeue;
      }
    }

    process.stdout.write("> ");
    return outcome === MoveOutcome.Safe ? AckType.Ack : AckType.NackDiscard;
  };
}

function handlerWar(
  gs: GameState,
  publishCh: ConfirmChannel,
  username: string
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async function (rw: RecognitionOfWar): Promise<AckType> {
    const warResolution = handleWar(gs, rw);

    switch (warResolution.result) {
      case WarOutcome.NotInvolved:
      case WarOutcome.NoUnits:
        process.stdout.write("> ");
        return AckType.NackRequeue;

      case WarOutcome.OpponentWon:
      case WarOutcome.YouWon:
        await publishGameLog(
          publishCh,
          username,
          `${warResolution.winner} won a war against ${warResolution.loser}`
        );
        process.stdout.write("> ");
        return AckType.Ack;

      case WarOutcome.Draw:
        await publishGameLog(
          publishCh,
          username,
          `A war between ${warResolution.attacker} and ${warResolution.defender} resulted in a draw`
        );
        process.stdout.write("> ");
        return AckType.Ack;

      default:
        console.error("Unknown war outcome");
        process.stdout.write("> ");
        return AckType.NackDiscard;
    }
  };
}

async function publishGameLog(
  channel: ConfirmChannel,
  username: string,
  message: string
) {
  const gameLog: GameLog = {
    username,
    message,
    currentTime: new Date(),
  };

  await publishMsgPack(
    channel,
    ExchangePerilTopic,
    `${GameLogSlug}.${username}`,
    gameLog
  );
}
