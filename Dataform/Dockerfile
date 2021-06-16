FROM alpine:3.13

RUN apk add --no-cache --update nodejs nodejs-npm git 
RUN npm i -g @dataform/cli typescript ts-node

COPY package*.json ./
COPY tsconfig.json ./
RUN npm i --production
COPY src ./src

RUN tsc --skipLibCheck
RUN rm -rf /root/.npm/_cacache/

CMD ["ts-node-script", "src/run.ts"]