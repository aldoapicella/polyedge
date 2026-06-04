import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#17201b",
        panel: "#f7f8f4",
        line: "#d9ddd2",
        good: "#18705b",
        warn: "#a45d13",
        danger: "#b3363a"
      },
      boxShadow: {
        hairline: "0 0 0 1px rgba(23,32,27,0.08)"
      }
    }
  },
  plugins: []
};

export default config;
