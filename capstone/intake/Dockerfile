FROM node:20-bookworm-slim
WORKDIR /app
COPY package.json ./
RUN npm install
COPY shared ./shared
COPY intake ./intake
CMD ["node_modules/.bin/tsx", "intake/server.ts"]
