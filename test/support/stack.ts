import { randomUUID } from 'node:crypto';
import type { StartedKafkaContainer } from '@testcontainers/kafka';
import { Network, type StartedNetwork, type StartedTestContainer } from 'testcontainers';
import { brokersOf, createTopic, startKafka, SUGGESTIONS_TOPIC, waitUntilBrokerReady } from './kafka.js';
import { schemaRegistryUrlOf, startSchemaRegistry } from './schema-registry.js';

/** The broker and the Schema Registry that one run starts, and how to stop them. */
export interface TestStack {
  readonly brokers: string[];
  readonly schemaRegistryUrl: string;
  stop(): Promise<void>;
}

/** Starts Kafka and the Schema Registry, on a network of their own, and creates the suggestions topic. */
export async function startTestStack(): Promise<TestStack> {
  let network: StartedNetwork | undefined;
  let kafka: StartedKafkaContainer | undefined;
  let schemaRegistry: StartedTestContainer | undefined;
  const stop = async () => {
    await schemaRegistry?.stop();
    await kafka?.stop();
    await network?.stop();
  };

  try {
    // Unique names per run, so a run never reaches another run's containers.
    const suffix = randomUUID().slice(0, 8);
    const kafkaAlias = `bookoverflow-kafka-${suffix}`;
    const registryAlias = `bookoverflow-schema-registry-${suffix}`;

    network = await new Network().start();
    kafka = await startKafka(network, kafkaAlias);
    const brokers = brokersOf(kafka);
    await waitUntilBrokerReady(brokers);
    await createTopic(brokers, SUGGESTIONS_TOPIC);

    schemaRegistry = await startSchemaRegistry(network, registryAlias, kafkaAlias);
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
