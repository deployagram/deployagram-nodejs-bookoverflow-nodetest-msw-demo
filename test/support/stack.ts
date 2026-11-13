import { randomUUID } from 'node:crypto';
import type { StartedKafkaContainer } from '@testcontainers/kafka';
import { startDeployagramEnvironment, stopDeployagramEnvironment } from '@deployagram/environment';
import { DiagramLoggerClient } from '@deployagram/logger-client';
import type { StartedTestContainer } from 'testcontainers';
import {
  brokersOf,
  createTopic,
  networkBootstrapServersOf,
  startKafka,
  SUGGESTIONS_TOPIC,
  waitUntilBrokerReady,
} from './kafka.js';
import {
  networkSchemaRegistryUrlOf,
  schemaRegistryUrlOf,
  startSchemaRegistry,
} from './schema-registry.js';

/** The broker, the Schema Registry and the Collector that one run uses, and how to stop what the run started. */
export interface TestStack {
  readonly brokers: string[];
  readonly schemaRegistryUrl: string;
  stop(): Promise<void>;
}

/**
 * Starts the Collector, then Kafka and the Schema Registry on the Deployagram network, and points the Collector at them.
 * The Collector is started here, not by a test file, so every test file reads the same one. Services that were already
 * running are reused, and are left running.
 */
export async function startTestStack(): Promise<TestStack> {
  // Runtime mode lets the run point the Collector at its broker and registry.
  const deployagram = await startDeployagramEnvironment({
    proxy: false,
    loggerEnvironment: { KAFKA_BROKER_LOCATION_MODE: 'RUNTIME', SCHEMA_REGISTRY_LOCATION_MODE: 'RUNTIME' },
  });
  let kafka: StartedKafkaContainer | undefined;
  let schemaRegistry: StartedTestContainer | undefined;
  const stop = async () => {
    await schemaRegistry?.stop();
    await kafka?.stop();
    await stopDeployagramEnvironment();
  };

  try {
    // Unique names per run, so a run never reaches another run's containers.
    const suffix = randomUUID().slice(0, 8);
    const kafkaAlias = `bookoverflow-kafka-${suffix}`;
    const registryAlias = `bookoverflow-schema-registry-${suffix}`;

    kafka = await startKafka(kafkaAlias);
    const brokers = brokersOf(kafka);
    await waitUntilBrokerReady(brokers);
    await createTopic(brokers, SUGGESTIONS_TOPIC);

    schemaRegistry = await startSchemaRegistry(registryAlias, kafkaAlias);

    // The Collector reads each produced record from the broker, and decodes it with the registry.
    const logger = new DiagramLoggerClient({ baseUrl: deployagram.logger.url });
    await logger.configureKafkaBroker(networkBootstrapServersOf(kafkaAlias));
    await logger.configureSchemaRegistry(networkSchemaRegistryUrlOf(registryAlias));

    return { brokers, schemaRegistryUrl: schemaRegistryUrlOf(schemaRegistry), stop };
  } catch (error) {
    await stop();
    throw error;
  }
}

/** The broker addresses the run started. Throws if the tests were not run through test/with-stack.ts. */
export function brokersFromEnvironment(): string[] {
  return requiredEnv('TEST_KAFKA_BROKERS').split(',');
}

/** The Schema Registry address the run started. Throws if the tests were not run through test/with-stack.ts. */
export function schemaRegistryUrlFromEnvironment(): string {
  return requiredEnv('TEST_SCHEMA_REGISTRY_URL');
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is not set: run the tests with npm test, which starts the containers first.`);
  }
  return value;
}
