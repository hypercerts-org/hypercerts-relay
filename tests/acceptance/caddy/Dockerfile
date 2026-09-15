# caddy:2.10.2-alpine
FROM caddy@sha256:4c6e91c6ed0e2fa03efd5b44747b625fec79bc9cd06ac5235a779726618e530d

RUN addgroup -S -g 1000 caddy && adduser -S -D -H -u 1000 -G caddy caddy
COPY Caddyfile /etc/caddy/Caddyfile
USER caddy
