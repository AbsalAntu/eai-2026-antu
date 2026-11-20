FROM node:20-bookworm-slim
WORKDIR /app
COPY package.json ./
RUN npm install
COPY shared ./shared
COPY notifier ./notifier
CMD ["node_modules/.bin/tsx", "notifier/server.ts"]
