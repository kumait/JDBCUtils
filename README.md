# JDBCUtils

Access data over JDBC with minimal code. The whole data access code is written in single class with static methods.
The project depends on [Google Gson](http://mvnrepository.com/artifact/com.google.code.gson/gson/2.5)

Best way to use it is to simply include the JDBCUtils class code in your project.

## Usage
Suppose we have the following table (mysql)

```sql
create table `student` (
  `id` int not null auto_increment,
  `creation_date` datetime not null,
  `uid` varchar(40) null,
  `country` char(2) null,
  `gender` int(11) not null,
  `status` int(11) not null,
  primary key (`id`)
)
```

The following code shows how you deal with the table using JDBCUtils

```java
public long add(String uid, String country, int gender, int status, Date creationDate) throws SQLException {
    String sql = "insert into `student` (`uid`, `country`, `gender`, `status`, `creation_date`) values (?, ?, ?, ?, ?)";
    return JDBCUtils.executeInsert(connection, sql, true, uid, country, gender, status, creationDate);
}

public void updateStatus(long id, int status) throws SQLException {
    String sql = "update `student` set `status` = ? where `id` = ?";
    JDBCUtils.executeUpdate(connection, sql, status, id);
}

public JsonArray get(String uid, int status) throws SQLException {
    String sql = "select * from `student` where `uid` = ? and `status` = ?";
    return JDBCUtils.getJsonArray(connection, sql, uid, status);
}

public long getCount(String uid, int status) throws Exception {
    String sql = "select count(`id`) from `student` where `uid` = ? and `status` = ?";
    return JDBCUtils.getSimpleObject(connection, sql, long.class, 1, uid, status);
}

public void delete(long id) throws SQLException {
    String sql = "delete from `student` where `id` = ?";
    JDBCUtils.executeUpdate(connection, sql, id);
}
```

It is possible to get typed objects as results instead of JSON by calling methods methods such `getList`, `getSimpleList`, `getObject`.

You can also get results from stored procedures by calling methods with the SP suffix in their name such as `getJsonArraySP`, `getListSP`.



