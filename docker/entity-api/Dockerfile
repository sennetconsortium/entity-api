FROM sennet/api-base-image:1.3.1

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

# Update outdated packages and install nginx (https://nginx.org/en/linux_packages.html#Debian)
RUN apt-get update -y && \
    apt-get dist-upgrade -y && \
    apt-get install -y curl gnupg2 ca-certificates lsb-release debian-archive-keyring && \
    # import an official nginx signing key
    curl https://nginx.org/keys/nginx_signing.key | gpg --dearmor \
        | tee /usr/share/keyrings/nginx-archive-keyring.gpg >/dev/null && \
    # verify the signature contains the full fingerprint "573BFD6B3D8FBC641079A6ABABF5BD827BD9BF62", if not then fail
    gpg --dry-run --quiet --no-keyring --import --import-options import-show /usr/share/keyrings/nginx-archive-keyring.gpg | grep -q "573BFD6B3D8FBC641079A6ABABF5BD827BD9BF62" || \
        (echo "Signature verification failed!" && exit 1) && \
    # add the mainline nginx repository
    echo "deb [signed-by=/usr/share/keyrings/nginx-archive-keyring.gpg] \
    http://nginx.org/packages/mainline/debian `lsb_release -cs` nginx" \
        | tee /etc/apt/sources.list.d/nginx.list && \
    # set up repository pinning to prefer nginx mainline
    printf "Package: *\nPin: origin nginx.org\nPin: release o=nginx\nPin-Priority: 900\n" \
        | tee /etc/apt/preferences.d/99nginx >/dev/null && \
    apt-get update -y && \
    apt-get install -y nginx && \
    apt-get purge -y lsb-release && \
    apt-get auto-remove -y

# Copy from host to image
COPY . .
COPY ./entrypoint.sh /usr/local/bin/entrypoint.sh

# Update Python packages. uwsgi is installed by the base image.
RUN pip install --upgrade pip setuptools wheel && \
    pip install -r src/requirements.txt

# Modify the UID and GID of codcc to match the host
RUN groupmod --gid $HOST_GID codcc && \
    usermod --uid $HOST_UID codcc

# Ensure codcc has access to the following directories
RUN touch /var/run/nginx.pid && \
    chown -R codcc:codcc /etc/nginx/ && \
    chown -R codcc:codcc /etc/nginx/conf.d/ && \
    chown -R codcc:codcc /usr/lib/nginx/ && \
    touch /var/log/nginx/error.log && \
    chown -R codcc:codcc /var/log/nginx && \
    chown -R codcc:codcc /var/cache/nginx && \
    chown -R codcc:codcc /var/run/nginx.pid && \
    chown -R codcc:codcc /run/nginx.pid && \
    mv nginx/nginx.conf /etc/nginx/nginx.conf && \
    rm -rf nginx && \
    rm -rf rq && \
    rm entrypoint.sh && \
    chmod +x /usr/local/bin/entrypoint.sh && \
    chown -R codcc:codcc /usr/src/app

USER codcc

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
