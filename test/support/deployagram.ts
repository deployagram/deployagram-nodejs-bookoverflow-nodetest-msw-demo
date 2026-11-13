import { after, afterEach, before, beforeEach } from 'node:test';
import { createDeployagramSuite, type DeployagramSuiteOptions } from '@deployagram/core';

/**
 * The node:test adapter for @deployagram/core: wires the suite into the runner's hooks. The core finds the Collector
 * that the run started, and marks each test's end. node:test's test context carries the test's own name, so markers
 * match the other adapters.
 *
 * The adapter does not stop the environment. The Collector belongs to the run, which test/with-stack.ts stops.
 */
export function useDeployagram(options: DeployagramSuiteOptions) {
  const suite = createDeployagramSuite(options);

  before(() => suite.beforeAll(), { timeout: 180_000 });
  beforeEach(() => suite.beforeEach());
  // The hook's context carries the test's own name, without the suite's.
  afterEach((context) => suite.afterEach(context.name));

  return suite;
}
