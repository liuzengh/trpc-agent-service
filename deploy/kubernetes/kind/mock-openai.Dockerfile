FROM node:22-alpine
WORKDIR /app
COPY tests/mock/mock-openai.mjs /app/mock-openai.mjs
ENV HOST=0.0.0.0
ENV PORT=9999
USER node
EXPOSE 9999
CMD ["node", "/app/mock-openai.mjs"]
