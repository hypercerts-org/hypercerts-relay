# node:24-alpine
FROM node@sha256:50c8e8ca1d27439048670df5883f32d57cf81cff6233222c893fd0d9884cbd81

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts
COPY patch-did-plc-document.mjs ./
RUN node patch-did-plc-document.mjs
COPY server.mjs ./

USER node
EXPOSE 2582
CMD ["node", "server.mjs"]
