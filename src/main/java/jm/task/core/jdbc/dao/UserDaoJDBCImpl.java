package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    private Connection connection = Util.getConnection();
    private String sqlQuery;

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        sqlQuery = "CREATE TABLE IF NOT EXISTS Users (id INT NOT NULL AUTO_INCREMENT, name varchar(30), lastname varchar(30), " +
                "age int, PRIMARY KEY (id))";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            connectionRollback();
            e.printStackTrace();
        }
    }

    public void dropUsersTable() {
        sqlQuery = "DROP TABLE IF EXISTS Users";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
            preparedStatement.execute();
            connection.commit();
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            connectionRollback();
            e.printStackTrace();
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        sqlQuery = "INSERT INTO Users(name, lastName, age) VALUES(?, ?, ?)";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setInt(3, age);
            preparedStatement.executeUpdate();

            connection.commit();
            System.out.println("User с именем - " + name + " дообавлен в базу данных");
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            connectionRollback();
            e.printStackTrace();
        }
    }

    public void removeUserById(long id) {
        String sqlQuery = "DELETE FROM Users WHERE id = id";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            connectionRollback();
            e.printStackTrace();
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        sqlQuery = "SELECT * FROM Users";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("lastName"));
                user.setAge(resultSet.getByte("age"));
                userList.add(user);
            }
            connection.commit();
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            connectionRollback();
            e.printStackTrace();
        }

        return userList;
    }

    public void cleanUsersTable() {
        sqlQuery = "TRUNCATE TABLE Users";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
            preparedStatement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            connectionRollback();
            e.printStackTrace();
        }
    }

    public void connectionRollback() {
        try {
            connection.rollback();
        } catch (SQLException e) {
//            throw new RuntimeException(e);
            e.printStackTrace();
        }
    }
}
