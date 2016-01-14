package data;

import com.google.gson.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by kumait on 12/1/14.
 */
public class JDBCUtils {
    private static Gson gson;

    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(java.util.Date.class, new JsonSerializer<java.util.Date>() {
            public JsonElement serialize(java.util.Date date, Type type, JsonSerializationContext jsonSerializationContext) {
                return new JsonPrimitive( date.getTime());
            }
        });
        gson = gsonBuilder.create();
    }

    private static void bindParams(PreparedStatement stmt, Object... parameters) throws SQLException {
        for (int i = 0; i < parameters.length; i++) {
            stmt.setObject(i + 1, parameters[i]);
        }
    }

    private static String getCallStatementSQL(String spName, int paramCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int i = 0; i < paramCount; i++) {
            sb.append("?");
            if (i < paramCount - 1) {
                sb.append(", ");
            }
        }
        sb.append(")");
        String sql = "call " + spName + sb.toString();
        return sql;
    }

    private static List<Field> getClassFields(Class cls, boolean includeParents) {
        List<Field> fields = new ArrayList<Field>();
        fields.addAll(Arrays.asList(cls.getDeclaredFields()));

        if (includeParents) {
            Class tmp = cls.getSuperclass();
            while (tmp != Object.class) {
                fields.addAll(Arrays.asList(tmp.getDeclaredFields()));
                tmp = tmp.getSuperclass();
            }
        }
        return fields;
    }

    private static HashMap<Field, Integer> getFieldToResultSetMap(ResultSet resultSet, Class cls) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<Field> fields = getClassFields(cls, true);
        HashMap<String, Integer> colMap = new HashMap<String, Integer>();
        HashMap<Field, Integer> fieldMap = new HashMap<Field, Integer>();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            colMap.put(metaData.getColumnName(i), i);
        }

        for (Field field : fields) {
            field.setAccessible(true);
            Annotation[] annotations = field.getDeclaredAnnotations();
            Database databaseAnno = (Database) field.getAnnotation(Database.class);
            String colName = databaseAnno != null && !databaseAnno.column().equals("?") ? databaseAnno.column() : field.getName();
            if (colMap.containsKey(colName)) {
                int colIndex = colMap.get(colName);
                fieldMap.put(field, colIndex);
            }
        }
        return fieldMap;
    }

    private static <T> List<T> getList(ResultSet resultSet, Class<T> cls) throws SQLException, IllegalAccessException, InstantiationException {
        List<T> list = new ArrayList<T>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<Field> fields = getClassFields(cls, true);
        HashMap<String, Integer> colMap = new HashMap<String, Integer>();
        HashMap<Field, Integer> fieldMap = getFieldToResultSetMap(resultSet, cls);

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            colMap.put(metaData.getColumnName(i), i);
        }

        for (Field field : fields) {
            field.setAccessible(true);
            Annotation[] annotations = field.getDeclaredAnnotations();
            Database databaseAnno = (Database) field.getAnnotation(Database.class);
            String colName = databaseAnno != null && !databaseAnno.column().equals("?") ? databaseAnno.column() : field.getName();
            if (colMap.containsKey(colName)) {
                int colIndex = colMap.get(colName);
                fieldMap.put(field, colIndex);
            }
        }

        while (resultSet.next()) {
            T t = cls.newInstance();
            for (Field field : fields) {
                if (fieldMap.containsKey(field)) {
                    int colIndex = fieldMap.get(field);
                    Object val = resultSet.getObject(colIndex);
                    field.set(t, val);
                }
            }
            list.add(t);
        }
        return list;
    }

    private static <T> List<T> getSingleFieldList(ResultSet resultSet, int fieldIndex, Class<T> cls) throws SQLException, IllegalAccessException, InstantiationException {
        List<T> list = new ArrayList<T>();
        while (resultSet.next()) {
            Object val = resultSet.getObject(fieldIndex);
            T t = (T) val;
            list.add(t);
        }
        return list;
    }

    private static JsonArray getJsonArray(ResultSet resultSet) throws SQLException {
        JsonArray jsonArray = new JsonArray();
        ResultSetMetaData metaData = resultSet.getMetaData();
        HashMap<Integer, String> columns = new HashMap<Integer, String>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            columns.put(i, metaData.getColumnName(i));
        }

        while (resultSet.next()) {
            JsonObject jsonObject = new JsonObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String colName = columns.get(i);
                Object colValue = resultSet.getObject(i);
                jsonObject.add(colName, gson.toJsonTree(colValue));
            }
            jsonArray.add(jsonObject);
        }
        return jsonArray;
    }

    //=============== public methods =================

    public static int executeUpdate(Connection conn, String sql, Object... parameters) throws SQLException {
        PreparedStatement stmt = null;
        int result = -1;
        try {
            stmt = conn.prepareStatement(sql);
            bindParams(stmt, parameters);
            result = stmt.executeUpdate();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return result;
    }

    public static long executeInsert(Connection conn, String sql, boolean hasAutoGeneratedKey, Object... parameters) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet generatedKeysResultSet = null;
        long result = -1;
        try {
            if (hasAutoGeneratedKey) {
                stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            } else {
                stmt = conn.prepareStatement(sql);
            }
            bindParams(stmt, parameters);
            stmt.executeUpdate();
            if (hasAutoGeneratedKey) {
                generatedKeysResultSet = stmt.getGeneratedKeys();
                if (generatedKeysResultSet.next()) {
                    result = generatedKeysResultSet.getLong(1);
                }
            }
        } finally {
            if (generatedKeysResultSet != null) {
                generatedKeysResultSet.close();
            }

            if (stmt != null) {
                stmt.close();
            }
        }
        return result;
    }


    // ======= JSON ========
    public static JsonArray getJsonArray(Connection conn, String sql, Object... parameters) throws SQLException {
        PreparedStatement stmt = null;
        ResultSet resultSet;
        JsonArray jsonArray;
        try {
            stmt = conn.prepareStatement(sql);
            bindParams(stmt, parameters);
            resultSet = stmt.executeQuery();
            jsonArray = getJsonArray(resultSet);
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return jsonArray;
    }

    public static JsonObject getJsonObject(Connection conn, String sql, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        JsonArray array = getJsonArray(conn, sql, parameters);
        return array.size() > 0 ? array.get(0).getAsJsonObject() : null;
    }

    public static JsonArray getJsonArraySP(Connection conn, String spName, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        CallableStatement stmt = null;
        ResultSet resultSet = null;
        JsonArray jsonArray = new JsonArray();
        try {
            String sql = getCallStatementSQL(spName, parameters.length);
            stmt = conn.prepareCall(sql);
            bindParams(stmt, parameters);
            boolean isResultSet = stmt.execute();
            if (isResultSet) {
                resultSet = stmt.getResultSet();
                jsonArray = getJsonArray(resultSet);
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (stmt != null) {
                stmt.close();
            }
        }
        return jsonArray;
    }

    public static JsonObject getObjectSP(Connection conn, String spName, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        JsonArray jsonArray = getJsonArraySP(conn, spName, parameters);
        return jsonArray.size() > 0 ? jsonArray.get(0).getAsJsonObject() : null;
    }

    // ======== classes =====
    public static <T> List<T> getList(Connection conn, String sql, Class<T> cls, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        PreparedStatement stmt = null;
        ResultSet resultSet = null;
        List<T> list = null;
        try {
            stmt = conn.prepareStatement(sql);
            bindParams(stmt, parameters);
            resultSet = stmt.executeQuery();
            list = getList(resultSet, cls);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (stmt != null) {
                stmt.close();
            }
        }
        return list;
    }



    public static <T> List<T> getSimpleList(Connection conn, String sql, Class<T> cls, int columnIndex, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        PreparedStatement stmt = null;
        ResultSet resultSet = null;
        List<T> list = null;
        try {
            stmt = conn.prepareStatement(sql);
            bindParams(stmt, parameters);
            resultSet = stmt.executeQuery();
            list = getSingleFieldList(resultSet, columnIndex, cls);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (stmt != null) {
                stmt.close();
            }
        }
        return list;
    }

    public static <T> T getObject(Connection conn, String sql, Class<T> cls, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        List<T> list = getList(conn, sql, cls, parameters);
        return list.size() > 0 ? list.get(0) : null;
    }

    public static <T> T getSimpleObject(Connection conn, String sql, Class<T> cls, int columnIndex, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        List<T> list = getSimpleList(conn, sql, cls, columnIndex, parameters);
        return list.size() > 0 ? list.get(0) : null;
    }

    public static int executeSP(Connection conn, String spName, Object... parameters) throws SQLException {
        CallableStatement stmt = null;
        int result = -1;
        try {
            String sql = getCallStatementSQL(spName, parameters.length);
            stmt = conn.prepareCall(sql);
            bindParams(stmt, parameters);
            result = stmt.executeUpdate();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return result;
    }


    public static <T> List<T> getListSP(Connection conn, String spName, Class<T> cls, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        CallableStatement stmt = null;
        ResultSet resultSet = null;
        List<T> list = null;
        try {
            String sql = getCallStatementSQL(spName, parameters.length);
            stmt = conn.prepareCall(sql);
            bindParams(stmt, parameters);
            boolean isResultSet = stmt.execute();
            if (isResultSet) {
                resultSet = stmt.getResultSet();
                list = getList(resultSet, cls);
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (stmt != null) {
                stmt.close();
            }
        }
        return list;
    }

    public static <T> List<T> getSimpleListSP(Connection conn, String spName, Class<T> cls, int columnIndex, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        CallableStatement stmt = null;
        ResultSet resultSet = null;
        List<T> list = null;
        try {
            String sql = getCallStatementSQL(spName, parameters.length);
            stmt = conn.prepareCall(sql);
            bindParams(stmt, parameters);
            boolean isResultSet = stmt.execute();
            if (isResultSet) {
                resultSet = stmt.getResultSet();
                list = getSingleFieldList(resultSet, columnIndex, cls);
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (stmt != null) {
                stmt.close();
            }
        }
        return list;
    }

    public static <T> T getObjectSP(Connection conn, String spName, Class<T> cls, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        List<T> list = getListSP(conn, spName, cls, parameters);
        return list.size() > 0 ? list.get(0) : null;
    }

    public static <T> T getSimpleObjectSP(Connection conn, String spName, Class<T> cls, int columnIndex, Object... parameters) throws SQLException, InstantiationException, IllegalAccessException {
        List<T> list = getSimpleListSP(conn, spName, cls, columnIndex, parameters);
        return list.size() > 0 ? list.get(0) : null;
    }

}
