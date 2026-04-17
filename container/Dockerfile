FROM ubuntu:24.04

USER root
RUN mkdir -p /run/user/1000 && chown 1000:1000 /run/user/1000
RUN apt-get update -q && \
    apt-get install -y --no-install-recommends \
        git curl ca-certificates sudo \
        python3 \
        libgl1 libegl1 libglib2.0-0 libxkbcommon0 \
        libfontconfig1 libfreetype6 libdbus-1-3 \
        libwayland-client0 libwayland-cursor0 libwayland-egl1 \
        qt6-wayland && \
    rm -rf /var/lib/apt/lists/* && \
    echo "ubuntu ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

USER ubuntu
RUN mkdir -p /home/ubuntu/.local/bin /home/ubuntu/.local/share/uv

ENV QT_QPA_PLATFORM=wayland

WORKDIR /home/ubuntu/MTR
CMD ["bash", "launch.sh"]
