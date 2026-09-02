# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM node:22-alpine AS build
ARG VITE_API_BASE=/api
ENV VITE_API_BASE=${VITE_API_BASE}

WORKDIR /app
COPY package.json ./
RUN npm install --registry=https://registry.npmmirror.com --no-audit --no-fund

COPY . .
RUN npm run build

# ---- Runtime stage ----
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
