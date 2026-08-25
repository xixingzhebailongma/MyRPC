CREATE DATABASE IF NOT EXISTS myrpc_im DEFAULT CHARACTER SET utf8mb4;

USE myrpc_im;

CREATE TABLE IF NOT EXISTS users (
    username VARCHAR(64) NOT NULL,
    password_hash CHAR(64) NOT NULL, -- SHA2(...,256) 输出 64 位小写 hex
    salt VARCHAR(32) NOT NULL, -- 每用户随机盐（16 字节 → 32 hex）
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (username)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE IF NOT EXISTS friends (
    user_id VARCHAR(64) NOT NULL,
    friend_id VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, friend_id),
    FOREIGN KEY (user_id) REFERENCES users (username) ON DELETE CASCADE,
    FOREIGN KEY (friend_id) REFERENCES users (username) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;