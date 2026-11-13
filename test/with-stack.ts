import { spawn } from 'node:child_process';
import { startTestStack } from './support/stack.js';

/**
 * Starts Kafka and the Schema Registry once, runs the command with their addresses in the environment, and stops the
 * containers when the command ends. Each test file then reads the addresses it needs.
 *
 * Usage: node --import tsx test/with-stack.ts <command> [arguments]
 */
const [command, ...args] = process.argv.slice(2);
if (!command) {
  throw new Error('Usage: node --import tsx test/with-stack.ts <command> [arguments]');
}

const stack = await startTestStack();
try {
  const exitCode = await new Promise<number>((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: 'inherit',
      env: {
        ...process.env,
        TEST_KAFKA_BROKERS: stack.brokers.join(','),
        TEST_SCHEMA_REGISTRY_URL: stack.schemaRegistryUrl,
      },
    });
    child.on('error', reject);
    child.on('exit', (code) => resolve(code ?? 1));
  });
  process.exitCode = exitCode;
} finally {
  await stack.stop();
}
