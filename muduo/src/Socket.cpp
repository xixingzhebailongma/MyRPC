#include "../include/Socket.h"
#include "Logger.h"
#include <asm-generic/socket.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

int createnonblocking() {
  int listenfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);
  if (listenfd < 0) {
    LOG_ERROR("listen socket create error: %s.", strerror(errno));
  }
  return listenfd;
}

Socket::Socket(int fd) : fd_(fd) {}

Socket::~Socket() { ::close(fd_); }

int Socket::fd() const { return fd_; }

std::string Socket::ip() const { return ip_; }

uint16_t Socket::port() const { return port_; }

void Socket::settcpnodelay(bool on) {
  int optval = on ? 1 : 0;
  ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval));
}

void Socket::setreuseport(bool on) {
    int optval = on ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval));  // 原来是 SO_REUSEADDR，是错的
  }

void Socket::setkeepalive(bool on) {
  int optval = on ? 1 : 0;
  ::setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval));
}

void Socket::bind(const InetAddress &servaddr) {
  if (::bind(fd_, servaddr.addr(), sizeof(sockaddr)) < 0) {
    LOG_ERROR("bind() failed: %s.", strerror(errno));
    close(fd_);
    exit(-1);
  }
  setipport(servaddr.ip(), servaddr.port());
}

void Socket::setipport(const std::string &ip, uint16_t port) {
  ip_ = ip;
  port_ = port;
}

void Socket::listen(int nn) {
  if (::listen(fd_, nn) != 0) {
    LOG_ERROR("listen() failed: %s.", strerror(errno));
    close(fd_);
    exit(-1);
  }
}

int Socket::accept(InetAddress &clientaddr) {
  sockaddr_in peeraddr;
  socklen_t len = sizeof(peeraddr);
  int clientfd = accept4(fd_, (sockaddr *)&peeraddr, &len, SOCK_NONBLOCK);
  clientaddr.setaddr(peeraddr);
  return clientfd;
}

void Socket::setreuseaddr(bool on) {
  int optval = on ? 1 : 0;
  ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
}

