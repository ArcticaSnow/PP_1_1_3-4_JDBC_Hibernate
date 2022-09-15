package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    private Connection connection;
    private Statement statement;
    private PreparedStatement preparedStatement;
    private String sqlQuery;

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        sqlQuery = "CREATE TABLE IF NOT EXISTS Users (id INT NOT NULL AUTO_INCREMENT, name varchar(30), lastname varchar(30), " +
                "age int, PRIMARY KEY (id))";

        try {

            connection = Util.getConnection();
            statement = connection.createStatement();
            statement.getConnection();
            statement.executeUpdate(sqlQuery);
            connection.commit();
        } catch (SQLException e) {
            connectionRollback();
            e.printStackTrace();
        } finally {
            try {
                statement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void dropUsersTable() {
        sqlQuery = "DROP TABLE IF EXISTS Users";

        try {
            connection = Util.getConnection();
            statement = connection.createStatement();
            statement.getConnection();
            statement.executeUpdate(sqlQuery);
            connection.commit();
        } catch (SQLException e) {
            connectionRollback();
            e.printStackTrace();
        } finally {
            try {
                statement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        sqlQuery = "INSERT INTO Users(name, lastName, age) VALUES(?, ?, ?)";

        try {
            connection = Util.getConnection();
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setInt(3, age);
            preparedStatement.executeUpdate();

            connection.commit();
            System.out.println("User с именем - " + name + " дообавлен в базу данных");
        } catch (SQLException e) {
            connectionRollback();
            e.printStackTrace();
        } finally {
            try {
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void removeUserById(long id) {
        String sqlQuery = "DELETE FROM Users WHERE id = ?";

        try {
            connection = Util.getConnection();
            preparedStatement = connection.prepareStatement(sqlQuery);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connectionRollback();
            e.printStackTrace();
        } finally {
            try {
                preparedStatement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        sqlQuery = "SELECT * FROM Users";

        try {
            connection = Util.getConnection();
            statement = connection.createStatement();
            statement.getConnection();
            ResultSet resultSet = statement.executeQuery(sqlQuery);

            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                userList.add(user);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                statement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return userList;
    }

    public void cleanUsersTable() {
        sqlQuery = "TRUNCATE TABLE Users";

        try {
            connection = Util.getConnection();
            statement = connection.createStatement();
            statement.getConnection();
            statement.executeUpdate(sqlQuery);
            connection.commit();
        } catch (SQLException e) {
//            connectionRollback();
            e.printStackTrace();
        } finally {
            try {
                statement.close();
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void connectionRollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
