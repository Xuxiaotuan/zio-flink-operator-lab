FROM nginxinc/nginx-unprivileged:1.27-alpine

COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY src/main/resources/web/ /usr/share/nginx/html/

EXPOSE 8080
