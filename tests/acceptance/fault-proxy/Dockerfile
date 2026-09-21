# node:24-alpine
FROM node@sha256:50c8e8ca1d27439048670df5883f32d57cf81cff6233222c893fd0d9884cbd81

WORKDIR /app
COPY server.mjs ./
USER node
EXPOSE 3000
CMD ["node", "server.mjs"]
