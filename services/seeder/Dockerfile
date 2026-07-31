FROM node:20-alpine

WORKDIR /app

COPY package.json yarn.lock* ./
RUN yarn install --production --frozen-lockfile

COPY src ./src

CMD ["node", "src/index.js"]
