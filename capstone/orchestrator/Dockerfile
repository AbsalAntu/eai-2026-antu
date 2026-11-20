FROM node:20-bookworm-slim
WORKDIR /app
COPY package.json ./
RUN npm install
COPY shared ./shared
COPY orchestrator ./orchestrator
CMD ["node_modules/.bin/tsx", "orchestrator/server.ts"]
