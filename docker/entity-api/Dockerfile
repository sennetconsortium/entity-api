FROM sennet/api-base-image:1.3.0-alpha3

LABEL description="SenNet Entity API Service"

# The commons branch to be used in requirements.txt during image build
# Default is master branch specified in docker-compose.yml if not set before the build
ARG COMMONS_BRANCH
ARG HOST_GID
ARG HOST_UID

# Change to directory that contains the Dockerfile
WORKDIR /usr/src/app

EXPOSE 5000 8080

USER root

# Update outdated packages and install nginx
RUN apk update && \
    apk upgrade && \
    printf "%s%s%s%s\n" \
        "@nginx " \
        "http://nginx.org/packages/mainline/alpine/v" \
        `egrep -o '^[0-9]+\.[0-9]+' /etc/alpine-release` \
        "/main" \
        | tee -a /etc/apk/repositories && \
    curl -o /tmp/nginx_signing.rsa.pub https://nginx.org/keys/nginx_signing.rsa.pub && \
    mv /tmp/nginx_signing.rsa.pub /etc/apk/keys/ && \
    apk add nginx@nginx

# Copy from host to image
COPY . .
COPY ./entrypoint.sh /usr/local/bin/entrypoint.sh

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
    rm ./entrypoint.sh && \
    chmod +x /usr/local/bin/entrypoint.sh && \
    chown -R codcc:codcc /usr/src/app

USER codcc

CMD ["/usr/local/bin/entrypoint.sh"]
