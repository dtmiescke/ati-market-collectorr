FROM node:22-alpine
WORKDIR /app
COPY ati-collector.mjs .
CMD ["node","ati-collector.mjs"]
