FROM node:current-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         ca-certificates curl git-core

# Clean up
RUN apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

RUN npm i -g @dataform/cli typescript ts-node

COPY package*.json ./
COPY tsconfig.json ./
RUN npm i
COPY src ./src

RUN tsc --skipLibCheck

#CMD ["bin/bash"]
CMD ["ts-node-script", "src/run.ts"]