FROM node:24-alpine AS build

ARG VITE_JAEGER_URL
ENV VITE_JAEGER_URL=$VITE_JAEGER_URL
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /src/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
