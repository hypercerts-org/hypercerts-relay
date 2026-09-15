# node:24-alpine
FROM node@sha256:50c8e8ca1d27439048670df5883f32d57cf81cff6233222c893fd0d9884cbd81

WORKDIR /app
COPY driver.mjs ./
USER node
CMD ["node", "driver.mjs", "idle"]
