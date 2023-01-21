package ch.finecloud.peopledb.repository;

import ch.finecloud.peopledb.annotation.Id;
import ch.finecloud.peopledb.annotation.MultiSQL;
import ch.finecloud.peopledb.annotation.SQL;
import ch.finecloud.peopledb.exception.DataException;
import ch.finecloud.peopledb.exception.UnableToSaveException;
import ch.finecloud.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class CRUDRepository<T> {

    protected Connection connection;
    private PreparedStatement savePS;
    private PreparedStatement findByIdPS;
    private PreparedStatement findAllPS;
    private PreparedStatement countPS;
    private PreparedStatement deletePS;
    private PreparedStatement updatePS;
    private PreparedStatement deleteManyPS;

    public CRUDRepository(Connection connection) {
        try {
            this.connection = connection;
            savePS = connection.prepareStatement(getSqlByAnnotation(CrudOperation.SAVE, this::getSaveSql), Statement.RETURN_GENERATED_KEYS);
            findByIdPS = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSql));
            findAllPS = connection.prepareStatement(getSqlByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSql));
            countPS = connection.prepareStatement(getSqlByAnnotation(CrudOperation.COUNT, this::getCountSql));
            deletePS = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE_ONE, this::getDeleteSql));
            updatePS = connection.prepareStatement(getSqlByAnnotation(CrudOperation.UPDATE, this::getUpdateSql));
        } catch (SQLException e) {
            throw new DataException("Unable to create prepared statement for CrudRepository", e);
        }
    }


    public T save(T entity) throws UnableToSaveException {
        try {
            mapForSave(entity, savePS);
            int recordsAffected = savePS.executeUpdate();
            ResultSet rs = savePS.getGeneratedKeys();
            while (rs.next()) {
                long id = rs.getLong(1);
                setIdByAnnotation(id, entity);
//                System.out.println(entity);
            }
//            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            throw new DataException("Unable to create prepared statement for CrudRepository", e);
        }
        return entity;
    }

    public Optional<T> findById(Long id) {
        T entity = null;
        try {
            findByIdPS.setLong(1, id);
            ResultSet rs = findByIdPS.executeQuery();
            while (rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new DataException("Unable to create prepared statement for CrudRepository", e);
        }
        return Optional.ofNullable(entity);
    }

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();
        try {
            ResultSet rs = findAllPS.executeQuery();
            while (rs.next()) {
                entities.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new DataException("Unable to create prepared statement for CrudRepository", e);
        }
        return entities;
    }

    public Long count() {
        long count = 0;
        try {
            ResultSet rs = countPS.executeQuery();
            if (rs.next()) {
                count = rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new DataException("Unable to create prepared statement for CrudRepository", e);
        }
        return count;
    }

    // delete more one entity only
    public void delete(T entity) {
        try {
            deletePS.setLong(1, getIdByAnnotation(entity));
            int affectedRecordCount = deletePS.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            throw new DataException("Unable to create prepared statement for CrudRepository", e);
        }
    }

    // delete more than one entity at a time
    public void delete(T... entities) {
        try {
            String ids = Arrays.stream(entities).map(this::getIdByAnnotation).map(String::valueOf).collect(Collectors.joining(","));
            deleteManyPS = connection.prepareStatement(getSqlByAnnotation(CrudOperation.DELETE_MANY, this::getDeleteInSql).replace(":ids", ids));
            int affectedRecordCount = deleteManyPS.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setIdByAnnotation(Long id, T entity) {
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .forEach(f -> {
                    f.setAccessible(true);
                    try {
                        f.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to set ID field value.");
                    }
                });
    }


    private Long getIdByAnnotation(T entity) {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .map(f -> {
                    f.setAccessible(true);
                    Long id = null;
                    try {
                        id = (long) f.get(entity);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No ID annotated field found")); // this should be replaced with a custom exception
    }

    public void update(T entity) {
        try {
            mapForUpdate(entity, updatePS);
            updatePS.setLong(5, getIdByAnnotation(entity));
            updatePS.executeUpdate();
        } catch (SQLException e) {
            throw new DataException("Unable to create prepared statement for CrudRepository", e);
        }
    }

    // go find the sql that we're looking for via the annotation, but if it's not there, then just fallback to the supplied sql method, the 2nd parameter
    private String getSqlByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        Stream<SQL> multiSqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(MultiSQL.class))
                .map(m -> m.getAnnotation(MultiSQL.class))
                .flatMap((msql -> Arrays.stream(msql.value())));

        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(m -> m.isAnnotationPresent(SQL.class))
                .map(m -> m.getAnnotation(SQL.class));

        return Stream.concat(multiSqlStream, sqlStream)
                .filter(a -> a.operationType().equals(operationType))
                .map(SQL::value)
                .findFirst().orElseGet(sqlGetter);
    }

    /**
     * @return should return a SQL string like:
     * "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * Be sure to include the '(:ids)' named parameter & call it 'ids'
     */
    protected String getDeleteInSql() {
        throw new RuntimeException("SQL not defined");
    }

    protected String getDeleteSql() {
        throw new RuntimeException("SQL not defined");
    }

    protected String getCountSql() {
        throw new RuntimeException("SQL not defined");
    }

    protected String getFindAllSql() {
        throw new RuntimeException("SQL not defined");
    }

    protected String getUpdateSql() {
        throw new RuntimeException("SQL not defined");
    }

    /**
     * @return Returns a String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's ID.
     */
    protected String getFindByIdSql() {
        throw new RuntimeException("SQL not defined");
    }

    protected String getSaveSql() {
        throw new RuntimeException("SQL not defined");
    }

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;


    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;


}
