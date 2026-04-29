# Build with repository root as context:
# docker build -f packages/data-warehouse/Dockerfile .

FROM node:24-bookworm-slim AS build-stage
ENV NODE_ENV=production
WORKDIR /usr/src/medplum

COPY package.json package-lock.json turbo.json tsconfig.json ./
COPY packages/data-warehouse/package.json ./packages/data-warehouse/package.json

RUN npm ci --workspace=@medplum/data-warehouse

COPY packages/data-warehouse ./packages/data-warehouse

RUN npm run build --workspace=@medplum/data-warehouse
RUN npm prune --omit=dev

FROM node:24-bookworm-slim AS runtime-stage
ENV NODE_ENV=production
WORKDIR /usr/src/medplum

COPY --from=build-stage /usr/src/medplum/node_modules ./node_modules
COPY --from=build-stage /usr/src/medplum/packages/data-warehouse/package.json ./packages/data-warehouse/package.json
COPY --from=build-stage /usr/src/medplum/packages/data-warehouse/dist ./packages/data-warehouse/dist

ENTRYPOINT ["node", "packages/data-warehouse/dist/main.js", "sync"]
