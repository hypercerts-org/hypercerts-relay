# Shared bootstrap can also be built from Jetstream's isolated context.
FROM golang:1.26.6-bookworm AS bootstrap
WORKDIR /src
COPY jetstream/cmd/container-entrypoint/main.go ./main.go
RUN CGO_ENABLED=0 go build -trimpath -o /container-entrypoint main.go

# Build from the repository root.
FROM node:24.18.0-bookworm-slim AS build
WORKDIR /app
COPY administration/package.json administration/package-lock.json ./
RUN npm ci --ignore-scripts
COPY administration/ ./
RUN npm run build && npm prune --omit=dev --ignore-scripts

FROM node:24.18.0-bookworm-slim
RUN apt-get update && apt-get install --yes --no-install-recommends dumb-init && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/package.json /app/package-lock.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/server ./server
COPY --from=build /app/dist ./dist
COPY --from=bootstrap /container-entrypoint /usr/local/bin/container-entrypoint
ENV NODE_ENV=production ADMIN_BIND=:: PORT=3000
USER node
EXPOSE 3000
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/usr/local/bin/container-entrypoint", "administration"]
CMD ["/app/node_modules/.bin/tsx", "server/main.ts"]
