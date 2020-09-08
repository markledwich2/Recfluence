FROM node:alpine

RUN apk --update add git 
RUN npm i -g @dataform/cli typescript ts-node

COPY package*.json ./
COPY tsconfig.json ./
RUN npm i
COPY src ./src

RUN tsc --skipLibCheck

CMD ["ts-node-script", "src/run.ts"]