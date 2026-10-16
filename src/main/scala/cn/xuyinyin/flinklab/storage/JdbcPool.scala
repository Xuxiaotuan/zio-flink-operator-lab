package cn.xuyinyin.flinklab.storage

import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection

/** 每个存储适配器拥有有界连接池；Scope 退出时关闭，连接归还不关闭物理连接。 */
final class JdbcPool(jdbcUrl: String, username: String, password: String) extends AutoCloseable:
  private val source = new HikariDataSource()
  source.setJdbcUrl(jdbcUrl)
  source.setUsername(username)
  source.setPassword(password)
  source.setMaximumPoolSize(4)
  source.setMinimumIdle(0)
  source.setConnectionTimeout(5000)
  source.setValidationTimeout(1000)
  source.setInitializationFailTimeout(-1)
  // 同时约束建连、网络读取、服务端语句和行锁等待，避免 JDBC 占住无限时长的 blocking fiber。
  source.addDataSourceProperty("connectTimeout", "5")
  source.addDataSourceProperty("socketTimeout", "15")
  source.addDataSourceProperty("options", "-c statement_timeout=10000 -c lock_timeout=5000")

  def withConnection[A](f: Connection => A): A =
    val connection = source.getConnection()
    try f(connection)
    finally connection.close()

  override def close(): Unit = source.close()
