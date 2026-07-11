import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: [
      "packages/**/*.test.ts",
      "apps/**/*.test.ts",
      "engines/**/*.test.ts"
    ],
    exclude: [
      "node_modules",
      "dist",
      "build",
      "coverage"
    ]
  }
});