package com.github.marschall.spring.batch.inmemory;

import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.ShardingKey;

final class NullConnectionBuilder implements ConnectionBuilder {

  private String username;

  NullConnectionBuilder() {
    super();
  }

  @Override
  public ConnectionBuilder user(String username) {
    this.username = username;
    return this;
  }

  @Override
  public ConnectionBuilder password(String password) {
    return this;
  }

  @Override
  public ConnectionBuilder shardingKey(ShardingKey shardingKey) {
    return this;
  }

  @Override
  public ConnectionBuilder superShardingKey(ShardingKey superShardingKey) {
    return this;
  }

  @Override
  public Connection build() throws SQLException {
    return new NullConnection(this.username);
  }

}
