FROM node:22.17.1-alpine
WORKDIR /app

RUN apk add --no-cache openssl
COPY package*.json ./
RUN npm ci

COPY prisma ./prisma
COPY prisma.config.ts ./
RUN DATABASE_URL="postgresql://build:build@localhost:5432/build" npx prisma generate

COPY src ./src
ENV NODE_ENV=production
EXPOSE 5000
CMD ["node", "src/server.js"]
