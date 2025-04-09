FROM sennet/api-base-image:1.3.0

LABEL description="SenNet Entity API Service"

# The commons branch to be used in requirements.txt during image build
# Default is master branch specified in docker-compose.yml if not set before the build
ARG COMMONS_BRANCH

ENV HOST_GID=1002
ENV HOST_UID=1002

# Change to directory that contains the Dockerfile
WORKDIR /usr/src/app

# Copy from host to image
COPY . .

EXPOSE 5000 8080

USER root

# Update outdated packages and install nginx
RUN apk update && \
    apk upgrade && \
    apk add --no-cache --virtual .tmp-build-deps curl ca-certificates && \
    printf "%s%s%s%s\n" \
        "@nginx " \
        "http://nginx.org/packages/mainline/alpine/v" \
        `egrep -o '^[0-9]+\.[0-9]+' /etc/alpine-release` \
        "/main" \
        | tee -a /etc/apk/repositories && \
    curl -o /tmp/nginx_signing.rsa.pub https://nginx.org/keys/nginx_signing.rsa.pub && \
    mv /tmp/nginx_signing.rsa.pub /etc/apk/keys/ && \
    apk add nginx@nginx && \
    apk del .tmp-build-deps

# Update Python packages. uwsgi is installed by the base image.
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r src/requirements.txt

# Modify the UID and GID of codcc to match the host
RUN apk add --no-cache --virtual .tmp-build-deps shadow && \
    groupmod --gid $HOST_GID codcc && \
    usermod --uid $HOST_UID codcc && \
    apk del .tmp-build-deps

# Ensure codcc has access to the following directories
RUN touch /var/run/nginx.pid && \
    chown -R codcc:codcc /var/run/nginx.pid && \
    chown -R codcc:codcc /var/cache/nginx && \
    chown -R codcc:codcc /var/log/nginx && \
    rm /etc/nginx/conf.d/default.conf && \
    mv nginx/nginx.conf /etc/nginx/nginx.conf && \
    chown -R codcc:codcc /etc/nginx && \
    rm -rf nginx && \
    chmod +x start.sh && \
    chown -R codcc:codcc /usr/src/app

USER codcc

CMD ["./start.sh"]
