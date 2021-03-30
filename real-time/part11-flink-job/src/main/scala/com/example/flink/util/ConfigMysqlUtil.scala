package com.example.flink.util

/**
 * table_proecess 所在mysql对象的静态工具类，具备ORM，CRUD,池化能力
 */
object ConfigMysqlUtil extends JDBCBaseUtil("config.properties","config"){

}