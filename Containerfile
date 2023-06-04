FROM registry.fedoraproject.org/fedora:34
LABEL description="Simplelog container image based on fedora 34"
MAINTAINER Martin Bukatovic <mbukatov@redhat.com>
RUN dnf install -y python3 strace ltrace less lsof rsync tar \
    && dnf clean all \
    && rm -rf /var/cache/dnf
ADD log*.py /opt/
