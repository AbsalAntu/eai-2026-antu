FROM node:20-bookworm-slim
WORKDIR /app
COPY package.json ./
RUN npm install
COPY shared ./shared
COPY observer ./observer
CMD ["node_modules/.bin/tsx", "observer/server.ts"]
