# syntax=docker/dockerfile:1
FROM node:20-slim

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev

COPY tsconfig.json ./
COPY src ./src
RUN npm run build && npm prune --omit=dev

ENV NODE_ENV=production
ENV PORT=8080
EXPOSE 8080
CMD ["node", "dist/server.js"]
