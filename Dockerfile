FROM opensciencegrid/software-base:23-el9-release
ARG LOCALE=C.UTF-8
ENV LANG=$LOCALE
ENV LC_ALL=$LOCALE

ARG clean_cache=
RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    if [ -n "$clean_cache" ]; then \
        dnf clean all; \
    fi
RUN --mount=type=cache,target=/var/cache/yum,sharing=locked \
    dnf install -y --setopt=install_weak_deps=False --enablerepo=osg-internal-development \
        createrepo_c \
        httpd \
        python3 \
        rsync

# supervisord and cron configs
COPY docker/supervisor-*.conf /etc/supervisord.d/
COPY docker/*.cron /etc/cron.d/
COPY 99-tail-cron-logs.sh /etc/osg/image-init.d/

# OSG scripts for repo maintenance
COPY bin/* /usr/bin/

COPY distrepos /usr/local/lib/python3.9/site-packages/distrepos

# Data required for update_mashfiles.sh and rsyncd config
COPY etc/ /etc/

# Add symlinks for OSG script output, pointing to /data directory
# Create repo script log directory
# Create symlink to mirrorlist
# Disable Apache welcome page
# Set Apache docroot to /usr/local/repo
RUN for i in mirror repo repo.previous repo.working ; do mkdir -p /data/$i ; ln -s /data/$i /usr/local/$i ; done && \
    mkdir /var/log/repo && \
    ln -s /data/mirror /usr/local/repo/mirror && \
    truncate --size 0 /etc/httpd/conf.d/welcome.conf && \
    perl -pi -e 's#/var/www/html#/usr/local/repo#g' /etc/httpd/conf/httpd.conf

EXPOSE 80/tcp
EXPOSE 873/tcp
