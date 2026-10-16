FROM node:22-alpine AS verify
WORKDIR /workspace
COPY src/main/resources/web/ src/main/resources/web/
COPY src/test/web/ src/test/web/
RUN node --test src/test/web/app.test.mjs && node --check src/main/resources/web/app.js

FROM nginxinc/nginx-unprivileged:1.27-alpine

COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=verify /workspace/src/main/resources/web/ /usr/share/nginx/html/

EXPOSE 8080
